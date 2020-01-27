# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, unicode_literals

import glob
import io
import os
import traceback
import re
import io

from . import exectools
from .pushd import Dir
from .brew import watch_task

from .metadata import Metadata
from .model import Missing
from doozerlib.exceptions import DoozerFatalError
from doozerlib.source_modifications import SourceModifierFactory

RELEASERS_CONF = """
[{target}]
releaser = tito.release.DistGitReleaser
branches = {branch}
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

    def __init__(self, runtime, data_obj, clone_source=True,
                 source_modifier_factory=SourceModifierFactory()):
        super(RPMMetadata, self).__init__('rpm', runtime, data_obj)

        self.source = self.config.content.source
        if self.source is Missing:
            raise ValueError('RPM config must contain source entry.')
        self.source_modifier_factory = source_modifier_factory
        self.rpm_name = self.config.name
        self.version = None
        self.release = None
        self.tag = None
        self.build_status = False

        if clone_source:
            self.source_path = self.runtime.resolve_source('rpm_{}'.format(self.rpm_name), self)
            self.source_head = self.runtime.resolve_source_head('rpm_{}'.format(self.rpm_name), self)
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
                if self.config.content.build.push_release_commit and not scratch:
                    exectools.cmd_assert("git push origin")
                return

            exectools.cmd_assert(["git", "tag", "-am", "Release with doozer", self.tag])
            exectools.cmd_assert("git add .")
            commit_msg = "Automatic commit of package [{name}] release [{version}-{release}].".format(
                name=self.config.name, version=self.version, release=self.release
            )
            exectools.cmd_assert(['git', 'commit', '-m', commit_msg])

    def post_build(self, scratch):
        build_spec = self.config.content.build
        with Dir(self.source_path):

            valid_build = self.build_status and not scratch

            if valid_build and build_spec.push_release_commit:
                # success; push the tag
                try:
                    self.push_tag()
                except Exception:
                    raise RuntimeError('Build succeeded but failure pushing RPM tag for {}'.format(self.qualified_name))

            if not build_spec.push_release_commit or not valid_build:
                if build_spec.use_source_tito_config:
                    # don't leave the tag/commit lying around it is not upstream in the source
                    exectools.cmd_assert("tito tag --undo")
                else:
                    # temporary tag/commit; never leave lying around
                    exectools.cmd_assert("git reset --hard HEAD~")
                    exectools.cmd_assert("git tag -d {}".format(self.tag))

    def tito_setup(self):
        if self.config.content.build.use_source_tito_config:
            return  # rely on tito already set up in source

        tito_dir = os.path.join(self.source_path, '.tito')
        tito_target = self.config.content.build.tito_target
        tito_target = tito_target if tito_target else 'aos'
        tito_dist = self.config.content.build.tito_dist
        tito_dist = tito_dist if tito_dist else '.el7aos'

        with Dir(self.source_path):
            if not os.path.isdir(tito_dir):
                pre_init_sha = exectools.cmd_assert('git rev-parse HEAD')[0].strip()
                exectools.cmd_assert('tito init')
                # tito init will try to create a commit. We want this commit to be ignored
                # from the perspective of any builds about to occur. So, we want to reset
                # back to the original HEAD. The complexity here is that tito init will NOT
                # make a commit if .gitignore is configured to somehow ignores the the .tito
                # directory it creates. Thus, we store the SHA before running tito init
                # and use that as a means to return to the upstream HEAD.
                exectools.cmd_assert('git reset {}'.format(pre_init_sha))

            with io.open(os.path.join(tito_dir, 'releasers.conf'), 'w', encoding='utf-8') as r:
                branch = self.config.get('distgit', {}).get('branch', self.runtime.branch)
                r.write(RELEASERS_CONF.format(
                    branch=branch,
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

    def _run_modifications(self):
        """
        Interprets and applies content.source.modify steps in the image metadata.
        """
        with io.open(self.specfile, 'r', encoding='utf-8') as df:
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
                modifier.act(context=context, ceiling_dir=os.getcwd())
                specfile_data = context["content"]
            else:
                raise IOError("%s: Don't know how to perform modification action: %s" % (self.distgit_key, modification.action))

        with io.open(self.specfile, 'w', encoding='utf-8') as df:
            df.write(specfile_data)

    def update_spec(self):
        if self.config.content.build.use_source_tito_config:
            # tito tag will handle updating specfile
            return

        # otherwise, make changes similar to tito tagging
        replace = {
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

        replace_keys = list(replace.keys())

        with Dir(self.source_path):
            commit_sha = exectools.cmd_assert('git rev-parse HEAD')[0].strip()

            # run generic modifications first
            if self.config.content.source.modifications is not Missing:
                self._run_modifications()

            # second, update with NVR
            with io.open(self.specfile, 'r+', encoding='utf-8') as sf:
                lines = sf.readlines()
                for i in range(len(lines)):
                    if "%global os_git_vars " in lines[i]:
                        lines[i] = "%global os_git_vars OS_GIT_VERSION={version} OS_GIT_MAJOR={major} OS_GIT_MINOR={minor} OS_GIT_PATCH={patch} OS_GIT_COMMIT={commit} OS_GIT_TREE_STATE=clean\n".format(
                            version=full, major=major, minor=minor, patch=patch, commit=commit_sha
                        )

                    elif "%global commit" in lines[i]:
                        lines[i] = re.sub(r'commit\s+\w+', "commit {}".format(commit_sha), lines[i])

                    elif replace_keys:  # If there are keys left to replace
                        for k in replace_keys:
                            v = replace[k]
                            if lines[i].startswith(k):
                                lines[i] = v
                                replace_keys.remove(k)
                                break

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
            task_id = next((created_line.split(":")[1]).strip() for created_line in out_lines if
                           created_line.startswith("Created task:"))

            record["task_id"] = task_id

            # Look for a line like: "Task info: https://brewweb.engineering.redhat.com/brew/taskinfo?taskID=13948942"
            task_url = next((info_line.split(":", 1)[1]).strip() for info_line in out_lines if
                            info_line.startswith("Task info:"))

            self.logger.info("Build running: {} - {}".format(self.rpm_name, task_url))

            record["task_url"] = task_url

            # Now that we have the basics about the task, wait for it to complete
            error = watch_task(self.runtime.group_config.urls.brewhub, self.logger.info, task_id, terminate_event)

            # Gather brew-logs
            logs_dir = "%s/%s" % (self.runtime.brew_logs_dir, self.name)
            logs_rc, _, logs_err = exectools.cmd_gather(
                ["brew", "download-logs", "-d", logs_dir, task_id])

            if logs_rc != 0:
                self.logger.info("Error downloading build logs from brew for task %s: %s" % (task_id, logs_err))

            if error is not None:
                # An error occurred. We don't have a viable build.
                self.logger.info("Error building rpm: {}, {}".format(task_url, error))
                return False

            self.logger.info("Successfully built rpm: {} ; {}".format(self.rpm_name, task_url))
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

        return (self.distgit_key, self.build_status)
