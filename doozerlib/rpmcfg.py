import glob
import io
import os
import re
import shutil
import threading
import traceback
from typing import Optional

import rpm

from doozerlib import brew, constants, util
from doozerlib.exceptions import DoozerFatalError
from doozerlib.source_modifications import SourceModifierFactory

from . import exectools
from .brew import watch_tasks
from .metadata import Metadata
from .model import Missing
from .pushd import Dir

RELEASERS_CONF = """
[{target}]
releaser = tito.release.DistGitReleaser
branches = {branch}
build_targets = {branch}:{brew_target}
srpm_disttag = .el7aos
builder.test = 1
remote_git_name = {name}
"""

# note; appended to, does not replace existing props
TITO_PROPS = """

[{target}]
remote_git_name = {name}
"""


class RPMMetadata(Metadata):

    def __init__(self, runtime, data_obj, commitish: Optional[str] = None, clone_source=True,
                 source_modifier_factory=SourceModifierFactory()):
        super(RPMMetadata, self).__init__('rpm', runtime, data_obj, commitish)

        self.source = self.config.content.source
        if self.source is Missing:
            raise ValueError('RPM config must contain source entry.')
        self.source_modifier_factory = source_modifier_factory
        self.rpm_name = self.config.name
        self.version = None
        self.release = None
        self.tag = None
        self.build_status = False
        self.pre_init_sha = None
        # This will be set to True if the source contains embargoed (private) CVE fixes. Defaulting to None means this should be auto-determined.
        self.private_fix = None

        # If populated, extra variables that will added as os_git_vars
        self.extra_os_git_vars = {}

        # List of Brew targets.
        # The first target is the primary target, against which tito will direct build.
        # Others are secondary targets. We will use Brew API to build against secondary targets with the same distgit commit as the primary target.
        self.targets = self.config.get('targets', [])
        if not self.targets:
            # If not specified, load from group config
            profile_name = runtime.profile or runtime.group_config.default_rpm_build_profile
            if profile_name:
                self.targets = runtime.group_config.build_profiles.rpm[profile_name].targets.primitive()
        if not self.targets:
            # If group config doesn't define the targets either, the target name will be derived from the distgit branch name
            self.targets = [self.branch() + "-candidate"]

        self.source_path = None
        self.source_head = None
        # path to the specfile
        self.specfile = None
        if clone_source:
            self.clone_source()

    def clone_source(self):
        self.source_path = self.runtime.resolve_source(self)
        self.source_head = self.runtime.resolve_source_head(self)
        with Dir(self.source_path):
            # gather source repo short sha for audit trail
            out, _ = exectools.cmd_assert(["git", "rev-parse", "HEAD"])
            self.pre_init_sha = source_full_sha = out.strip()

            # Determine if the source contains private fixes by checking if the private org branch commit exists in the public org
            if self.private_fix is None and self.public_upstream_branch:
                self.private_fix = not util.is_commit_in_public_upstream(source_full_sha, self.public_upstream_branch, self.source_path)

            self.extra_os_git_vars.update(self.extract_kube_env_vars())

        if self.source.specfile:
            self.specfile = os.path.join(self.source_path, self.source.specfile)
            if not os.path.isfile(self.specfile):
                raise ValueError('{} config specified a spec file that does not exist: {}'.format(
                    self.config_filename, self.specfile
                ))
        else:
            with Dir(self.source_path):
                specs = []
                for spec in glob.glob('*.spec'):
                    specs.append(spec)
                if len(specs) > 1:
                    raise ValueError('More than one spec file found. Specify correct file in config yaml')
                elif len(specs) == 0:
                    raise ValueError('Unable to find any spec files in {}'.format(self.source_path))
                else:
                    self.specfile = os.path.join(self.source_path, specs[0])
        return self.source_path

    def set_nvr(self, version, release):
        self.version = version
        self.release = release
        self.tag = '{}-{}-{}'.format(self.config.name, self.version, self.release)

    def push_tag(self):
        if not self.tag:
            raise ValueError('Must run set_nvr() before calling!')

        with Dir(self.source_path):
            exectools.cmd_assert('git push origin --tags', retries=3)

    def commit_changes(self, scratch):
        if not self.tag:
            raise ValueError('Must run set_nvr() before calling!')

        # For tito to do its thing, you must have a commit locally.
        # Whether we push that commit comes later in post_build.
        # If we don't push it, we want to revert the commit so that subsequent
        # rpm / image builds don't think the sha is valid in origin.

        with Dir(self.source_path):
            if self.config.content.build.use_source_tito_config:
                # just use the tito tagger to change spec and tag
                exectools.cmd_assert([
                    "tito",
                    "tag",
                    "--no-auto-changelog",
                    "--use-version", self.version,
                    "--use-release", "{}%{{?dist}}".format(self.release)
                ])

            else:
                # We must have a local commit for tito to do its thing.
                exectools.cmd_assert("git add .")
                commit_msg = "Automatic commit of package [{name}] release [{version}-{release}].".format(
                    name=self.config.name, version=self.version, release=self.release
                )
                exectools.cmd_assert(['git', 'commit', '-m', commit_msg])

    def post_build(self, scratch):
        build_spec = self.config.content.build
        with Dir(self.source_path):

            valid_build = self.build_status and not scratch

            if not valid_build:
                # Reset everything we have done to allow others to use potentially global source.
                exectools.cmd_assert(f'git reset --hard {self.pre_init_sha}')
                return

            if build_spec.push_release_tag:
                # If we are configured to push a tag back to the repo, tag the sha we know is there (pre-init)
                # and push it back.
                exectools.cmd_assert(["git", "tag", "-am", "Release with doozer", self.tag, self.pre_init_sha])
                exectools.cmd_assert(['git', 'push', 'origin', self.tag], retries=3)

            if build_spec.push_release_commit:
                # If tito from source ran OR our on-demand tito made a commit that the
                # origin wants to see, go ahead and push it.
                exectools.cmd_assert(['git', 'push', 'origin'], retries=3)
            else:
                # Otherwise, reset everything we did so that subsequent builds out
                # of this source will have a trusted sha.
                exectools.cmd_assert(f'git reset --hard {self.pre_init_sha}')

    def tito_setup(self):
        if self.config.content.build.use_source_tito_config:
            return  # rely on tito already set up in source

        tito_dir = os.path.join(self.source_path, '.tito')
        tito_target = self.config.content.build.tito_target
        tito_target = tito_target if tito_target else 'aos'
        tito_dist = self.config.content.build.tito_dist
        tito_dist = tito_dist if tito_dist else '.el7aos'

        with Dir(self.source_path):

            # We either use .tito from source or we don't. If you want to use the upstream
            # source, use self.config.content.build.use_source_tito_config=True. Otherwise, we
            # are going to clear it out and create our own.
            if os.path.isdir(tito_dir):
                shutil.rmtree(tito_dir)

            exectools.cmd_assert('tito init')
            # clear out anything tito may have added by reverting to captured HEAD. It may not have
            # added anything depending .gitignore, so don't try to improve this with HEAD^.
            exectools.cmd_assert('git reset {}'.format(self.pre_init_sha))

            with io.open(os.path.join(tito_dir, 'releasers.conf'), 'w', encoding='utf-8') as r:
                r.write(RELEASERS_CONF.format(
                    branch=self.branch(),
                    brew_target=self.targets[0],
                    name=self.name,
                    target=tito_target,
                    dist=tito_dist,
                ))
                r.flush()

            # fix for tito 0.6.10 which looks like remote_git_name in wrong place
            with io.open(os.path.join(tito_dir, 'tito.props'), 'a', encoding='utf-8') as props:
                props.write(TITO_PROPS.format(name=self.name, target=tito_target))
                props.flush()

            # If there are multiple .spec files in the root of the project and
            # one was specifically identified in the metadata, rename those
            # which we are not targeting. Tito/brew do not handle multiple .specs.
            if self.source.specfile is not Missing:
                for f in os.listdir(self.source_path):
                    found_path = os.path.join(self.source_path, f)
                    if os.path.isfile(found_path) and found_path.endswith('.spec') and found_path != self.specfile:
                        self.logger.info('Renaming extraneous spec file before build: {} (only want {})'.format(f, self.source.specfile))
                        os.rename(found_path, '{}.ignore'.format(found_path))

    def _run_modifications(self, specfile: Optional[os.PathLike] = None, cwd: Optional[os.PathLike] = None):
        """
        Interprets and applies content.source.modify steps in the image metadata.

        :param specfile: Path to an alternative specfile. None means use the specfile in the source dir
        :param cwd: If set, change current working directory. None means use the source dir
        """
        with io.open(specfile or self.specfile, 'r', encoding='utf-8') as df:
            specfile_data = df.read()

        self.logger.debug(
            "About to start modifying spec file [{}]:\n{}\n".
            format(self.name, specfile_data))

        # add build data modifications dir to path; we *could* add more
        # specific paths for the group and the individual config but
        # expect most scripts to apply across multiple groups.
        metadata_scripts_path = self.runtime.data_dir + "/modifications"
        path = os.pathsep.join([os.environ['PATH'], metadata_scripts_path])

        for modification in self.config.content.source.modifications:
            if self.source_modifier_factory.supports(modification.action):
                # run additional modifications supported by source_modifier_factory
                modifier = self.source_modifier_factory.create(**modification)
                # pass context as a dict so that the act function can modify its content
                context = {
                    "component_name": self.name,
                    "kind": "spec",
                    "content": specfile_data,
                    "set_env": {"PATH": path},
                }
                modifier.act(context=context, ceiling_dir=cwd or Dir.getcwd())
                new_specfile_data = context.get("result")
            else:
                raise IOError("%s: Don't know how to perform modification action: %s" % (self.distgit_key, modification.action))

        if new_specfile_data is not None and new_specfile_data != specfile_data:
            with io.open(specfile or self.specfile, 'w', encoding='utf-8') as df:
                df.write(new_specfile_data)

    def update_spec(self):
        if self.config.content.build.use_source_tito_config:
            # tito tag will handle updating specfile
            return

        maintainer = self.get_maintainer_info()
        maintainer_string = ''
        if maintainer:
            flattened_info = ', '.join(f"{k}: {v}" for (k, v) in maintainer.items())
            # The space before the [Maintainer] information is actual rpm spec formatting
            # clue to preserve the line instead of assuming it is part of a paragraph.
            maintainer_string = f'[Maintainer] {flattened_info}'

        # otherwise, make changes similar to tito tagging
        rpm_spec_tags = {
            'Name:': 'Name:           {}\n'.format(self.config.name),
            'Version:': 'Version:        {}\n'.format(self.version),
            'Release:': 'Release:        {}%{{?dist}}\n'.format(self.release),
        }

        # self.version example: 3.9.0
        # Extract the major, minor, patch
        major, minor, patch = self.version.split('.')
        full = "v{}".format(self.version)

        # If this is a pre-release RPM, the include the release field in
        # the full version.
        # pre-release full version: v3.9.0-0.20.1
        # release full version: v3.9.0
        if self.release.startswith("0."):
            full += "-{}".format(self.release)

        with Dir(self.source_path):
            commit_sha = exectools.cmd_assert('git rev-parse HEAD')[0].strip()

            # run generic modifications first
            if self.config.content.source.modifications is not Missing:
                self._run_modifications()

            # second, update with NVR, env vars, and descriptions
            described = False
            with io.open(self.specfile, 'r+', encoding='utf-8') as sf:
                lines = sf.readlines()
                for i in range(len(lines)):

                    # If an RPM has sub packages, there can be multiple %description directives
                    if lines[i].strip().lower().startswith('%description'):
                        lines[i] = f'{lines[i].strip()}\n{maintainer_string}\n'
                        described = True

                    if "%global os_git_vars " in lines[i]:
                        lines[i] = f"%global os_git_vars OS_GIT_VERSION={major}.{minor}.{patch}-{self.release}-{commit_sha[0:7]} OS_GIT_MAJOR={major} OS_GIT_MINOR={minor} OS_GIT_PATCH={patch} OS_GIT_COMMIT={commit_sha} OS_GIT_TREE_STATE=clean"
                        for k, v in self.extra_os_git_vars.items():
                            lines[i] += f' {k}={v}'
                        lines[i] += '\n'

                    elif "%global commit" in lines[i]:
                        lines[i] = re.sub(r'commit\s+\w+', "commit {}".format(commit_sha), lines[i])

                    elif rpm_spec_tags:  # If there are keys left to replace
                        for k in list(rpm_spec_tags.keys()):
                            v = rpm_spec_tags[k]
                            # Note that tags are not case sensitive
                            if lines[i].lower().startswith(k.lower()):
                                lines[i] = v
                                del rpm_spec_tags[k]
                                break

                # If there are still rpm tags to inject, do so
                for v in rpm_spec_tags.values():
                    lines.insert(0, v)

                # If we didn't find a description, create one
                if not described:
                    lines.insert(0, f'%description\n{maintainer_string}\n')

                # truncate the original file
                sf.seek(0)
                sf.truncate()
                # write back new lines
                sf.writelines(lines)

    def _build_rpm(self, scratch, record, terminate_event, dry_run=False):
        """
        The part of `build_container` which actually starts the build,
        separated for clarity.
        """
        with Dir(self.source_path):
            self.logger.info("Building rpm: %s" % self.rpm_name)

            cmd_list = ['tito', 'release', '--debug', '--yes', '--test']
            if scratch:
                cmd_list.append('--scratch')
            if dry_run:
                cmd_list.append('--dry-run')
            tito_target = self.config.content.build.tito_target
            cmd_list.append(tito_target if tito_target else 'aos')

            # Run the build with --nowait so that we can immediately get information about the brew task
            rc, out, err = exectools.cmd_gather(cmd_list)

            if rc != 0:
                # Probably no point in continuing.. can't contact brew?
                self.logger.info("Unable to create brew task: out={}  ; err={}".format(out, err))
                return False

            if dry_run:
                self.logger.info("[Dry Run] Successfully built rpm: {}".format(self.rpm_name))
                self.logger.warning("Checking build output and downloading logs are skipped due to --dry-run.")
                return True
            # Otherwise, we should have a brew task we can monitor listed in the stdout.
            out_lines = out.splitlines()

            # Look for a line like: "Created task: 13949050" . Extract the identifier.
            primary_task_id = int(next((created_line.split(":")[1]).strip() for created_line in out_lines if created_line.startswith("Created task:")))

            record["task_id"] = primary_task_id

            # Look for a line like: "Task info: https://brewweb.engineering.redhat.com/brew/taskinfo?taskID=13948942"
            task_url = next((info_line.split(":", 1)[1]).strip() for info_line in out_lines if
                            info_line.startswith("Task info:"))

            self.logger.info("Build running: %s - %s - %s", self.rpm_name, self.targets[0], task_url)

            record["task_url"] = task_url
            task_ids = [primary_task_id]

            brew_session = self.runtime.build_retrying_koji_client()

            # Build against secondary targets
            secondary_targets = self.targets[1:]
            if secondary_targets:
                brew_session.krb_login()
                source, _, _ = brew_session.getTaskRequest(primary_task_id)
                for target in secondary_targets:
                    self.logger.info(f"Submitting task for secondary target {target}")
                    tid = brew_session.build(source, target)
                    task_ids.append(tid)
                    task_url = f"{constants.BREWWEB_URL}/taskinfo?taskID={tid}"
                    self.logger.info("Build running: %s - %s - %s", self.rpm_name, target, task_url)
            record["targets"] = self.targets
            record["task_ids"] = task_ids

            # Wait for all tasks to complete
            errors = watch_tasks(brew_session, self.logger.info, task_ids, terminate_event)

            # Gather brew-logs
            for target, task_id in zip(self.targets, task_ids):
                logs_dir = os.path.join(self.runtime.brew_logs_dir, self.name, f"{target}-{task_id}")
                logs_rc, _, logs_err = exectools.cmd_gather(
                    ["brew", "download-logs", "--recurse", "-d", logs_dir, task_id])
                if logs_rc != 0:
                    self.logger.info("Error downloading build logs from brew for task %s: %s" % (task_id, logs_err))

            failed_tasks = {task_id for task_id, error in errors.items() if error is not None}
            if failed_tasks:
                # An error occurred. We don't have a viable build.
                self.logger.info("Error building rpm %s:", self.rpm_name)
                for task_id in failed_tasks:
                    self.logger.info("\tTask %s failed: %s", task_id, errors[task_id])
                return False

            self.logger.info("Successfully built rpm: {} ; {}".format(self.rpm_name, [f"{constants.BREWWEB_URL}/taskinfo?taskID={tid}" for tid in task_ids]))
        return True

    def build_rpm(self, version, release, terminate_event, scratch=False, retries=3, local=False, dry_run=False):
        """
        Builds a package using tito release.

        If the source repository has the necessary tito configuration in .tito, the build can be
        configured to use that in the standard `tito tag` flow.

        The default flow imitates `tito tag`, but instead of creating a release commit and tagging that,
        the tag is added to the existing commit and a release commit is created afterward. In
        this way, the tag can be pushed back to the source, but pushing the commit is optional.
        [lmeyer 2019-04-01] I looked into customizing the tito tagger to support this flow.
        It was not going to be pretty, and even so would probably require doozer to do
        some modification of the spec first. It seems best to limit the craziness to doozer.

        By default, the tag is pushed, then it and the commit are removed locally after the build.
        But optionally the commit can be pushed before the build, so that the actual commit released is in the source.
        """
        if local:
            raise DoozerFatalError("Local RPM build is not currently supported.")

        # TODO: adjust Doozer's execution for private build
        if self.private_fix:
            self.logger.warning("Source contains embargoed fixes.")

        with self.runtime.get_named_semaphore(self.source_path, is_dir=True):

            with Dir(self.source_path):
                # Remember what we were at before tito activity. We may need to revert
                # to this if we don't push changes back to origin.
                self.pre_init_sha = exectools.cmd_assert('git rev-parse HEAD')[0].strip()

            pval = '.p0'
            if self.runtime.group_config.public_upstreams:
                if not release.endswith(".p?"):
                    raise ValueError(f"'release' must end with '.p?' for an rpm with a public upstream but its actual value is {release}")
                pval = ".p1" if self.private_fix else ".p0"

            if release.endswith(".p?"):
                release = release[:-3]  # strip .p?
                release += pval

            self.set_nvr(version, release)
            self.tito_setup()
            self.update_spec()
            self.commit_changes(scratch)
            action = "build_rpm"
            record = {
                "specfile": self.specfile,
                "source_head": self.source_head,
                "distgit_key": self.distgit_key,
                "rpm": self.rpm_name,
                "version": self.version,
                "release": self.release,
                "message": "Unknown failure",
                "status": -1,
                # Status defaults to failure until explicitly set by succcess. This handles raised exceptions.
            }

            if len(self.targets) > 1:  # for a multi target build, we need to ensure all buildroots have valid versions of golang compilers
                self.logger.info("Checking whether this is a golang package...")
                out, _ = exectools.cmd_assert(["rpmspec", "-q", "--buildrequires", "--", self.specfile])
                if any(dep.startswith("golang") for dep in out.splitlines()):
                    # assert buildroots contain the correct versions of golang
                    self.logger.info("This is a golang package. Checking whether buildroots contain consistent versions of golang compilers...")
                    self.assert_golang_versions()

            try:
                def wait(n):
                    self.logger.info("Async error in rpm build thread [attempt #{}]: {}".format(n + 1, self.qualified_name))
                    # Brew does not handle an immediate retry correctly, wait
                    # before trying another build, terminating if interrupted.
                    if terminate_event.wait(timeout=5 * 60):
                        raise KeyboardInterrupt()
                try:
                    exectools.retry(
                        retries=3, wait_f=wait,
                        task_f=lambda: self._build_rpm(
                            scratch, record, terminate_event, dry_run))
                except exectools.RetryException as err:
                    self.logger.error(str(err))
                    return (self.distgit_key, False)

                record["message"] = "Success"
                record["status"] = 0
                self.build_status = True

            except (Exception, KeyboardInterrupt):
                tb = traceback.format_exc()
                record["message"] = "Exception occurred:\n{}".format(tb)
                self.logger.info("Exception occurred during build:\n{}".format(tb))
                # This is designed to fall through to finally. Since this method is designed to be
                # threaded, we should not throw an exception; instead return False.
            finally:
                self.runtime.add_record(action, **record)

            self.post_build(scratch)
            return self.distgit_key, self.build_status

    target_golangs = {}  # a dict of cached target -> golang_version mappings
    target_golangs_lock = threading.Lock()

    def assert_golang_versions(self):
        """ Assert all buildroots have consistent versions of golang compilers
        """
        # no: do not check; x.y: only major and minor version; exact: the z-version must be the same
        check_mode = self.runtime.group_config.check_golang_versions
        if check_mode is Missing:
            check_mode = "x.y"

        if check_mode == "no" or check_mode is False:
            # if 'no' is not wrapped in quotes in the yaml, it is interpreted as False; so check both
            return

        # populate target_golangs with information from Brew
        with RPMMetadata.target_golangs_lock:
            uncached_targets = set(self.targets) - RPMMetadata.target_golangs.keys()
        if uncached_targets:
            uncached_targets = list(uncached_targets)
            self.logger.debug(f"Querying golang compiler versions for targets {uncached_targets}...")
            brew_session = self.runtime.build_retrying_koji_client()
            # get buildroots for uncached targets
            with brew_session.multicall(strict=True) as m:
                tasks = [m.getBuildTarget(target) for target in uncached_targets]
            buildroots = [task.result["build_tag_name"] for task in tasks]
            # get latest build of golang compiler for each buildroot
            golang_components = ["golang", "golang-scl-shim"]
            for target, buildroot in zip(uncached_targets, buildroots):
                latest_builds = brew.get_latest_builds([(buildroot, component) for component in golang_components], "rpm", None, brew_session)
                latest_builds = [builds[0] for builds in latest_builds if builds]  # flatten latest_builds
                # It is possible that a buildroot has multiple golang compiler packages (golang and golang-scl-shim) tagged in.
                # We need to find the maximum version in each buildroot.
                max_golang_nevr = None
                for build in latest_builds:
                    nevr = (build["name"], build["epoch"], build["version"], build["release"])
                    if max_golang_nevr is None or rpm.labelCompare(nevr[1:], max_golang_nevr[1:]) > 0:
                        max_golang_nevr = nevr
                if max_golang_nevr is None:
                    raise DoozerFatalError(f"Buildroot {buildroot} doesn't contain any golang compiler packages.")
                if max_golang_nevr[0] == "golang-scl-shim":
                    # golang-scl-shim is not an actual compiler but an adaptor to make go-toolset look like golang for an RPM build.
                    # We need to check the actual go-toolset build it requires.
                    # See https://source.redhat.com/groups/public/atomicopenshift/atomicopenshift_wiki/what_art_needs_to_know_about_golang#jive_content_id_golangsclshim
                    major, minor = max_golang_nevr[2].split(".")[:2]
                    go_toolset_builds = brew_session.getLatestBuilds(buildroot, package=f"go-toolset-{major}.{minor}", type="rpm")
                    if not go_toolset_builds:
                        raise DoozerFatalError(f"Buildroot {buildroot} doesn't have go-toolset-{major}.{minor} tagged in.")
                    max_golang_nevr = (go_toolset_builds[0]["name"], go_toolset_builds[0]["epoch"], go_toolset_builds[0]["version"], go_toolset_builds[0]["release"])
                with RPMMetadata.target_golangs_lock:
                    RPMMetadata.target_golangs[target] = max_golang_nevr

        # assert all buildroots have the same version of golang compilers
        it = iter(self.targets)
        first_target = next(it)
        with RPMMetadata.target_golangs_lock:
            first_nevr = RPMMetadata.target_golangs[first_target]
            for target in it:
                nevr = RPMMetadata.target_golangs[target]
                if (check_mode == "exact" and nevr[2] != first_nevr[2]) or (check_mode == "x.y" and nevr[2].split(".")[:2] != first_nevr[2].split(".")[:2]):
                    raise DoozerFatalError(f"Buildroot for target {target} has inconsistent golang compiler version {nevr[2]} while target {first_target} has {first_nevr[2]}.")

    def get_package_name(self, default=-1):
        """
        Returns the brew package name for the distgit repository. Method requires
        a local clone.
        :param default: If specified, this value will be returned if package name could not be found. If
                        not specified, an exception will be raised instead.
        :return: The package name if detected in the distgit spec file. Otherwise, the specified default.
                If default is not passed in, an exception will be raised if the package name can't be found.
        """
        specs = glob.glob(f'{self.distgit_repo().distgit_dir}/*.spec')
        if len(specs) != 1:
            if default != -1:
                return default
            raise IOError('Unable to find .spec file in RPM distgit: ' + self.qualified_name)

        spec_path = specs[0]
        with open(spec_path, mode='r', encoding='utf-8') as f:
            for line in f.readlines():
                if line.lower().startswith('name:'):
                    return line[5:].strip()  # Exclude "Name:" and then remove whitespace

        if default != -1:
            return default

        raise IOError(f'Unable to find Name: field in rpm spec: {spec_path}')

    def get_component_name(self, default=-1):
        return self.get_package_name(default=default)

    def candidate_brew_tag(self):
        return self.targets[0]

    def candidate_brew_tags(self):
        return self.targets.copy()
