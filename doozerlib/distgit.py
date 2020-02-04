from __future__ import absolute_import, print_function, unicode_literals
import hashlib
import os
import shutil
import time
import traceback
import errno
from multiprocessing import Lock
import yaml
import logging
import bashlex
import glob
import re
import io
import copy
from datetime import datetime, timedelta

from dockerfile_parse import DockerfileParser

from . import logutil
from . import assertion
from . import exectools
from .pushd import Dir
from .brew import watch_task
from .model import Model, Missing
from doozerlib.exceptions import DoozerFatalError
from doozerlib.util import yellow_print
from doozerlib import state
from doozerlib.source_modifications import SourceModifierFactory
from doozerlib import constants

# doozer used to be part of OIT
OIT_COMMENT_PREFIX = '#oit##'
OIT_BEGIN = '##OIT_BEGIN'
OIT_END = '##OIT_END'

CONTAINER_YAML_HEADER = """
# This file is managed by doozer: https://github.com/openshift/doozer
# operated by the OpenShift Automated Release Tooling team (#aos-art on CoreOS Slack).

# Any manual changes will be overwritten by doozer on the next build.
#
# See https://mojo.redhat.com/docs/DOC-1159997 for more information on
# maintaining this file and the format and examples

---
"""

# Always ignore these files/folders when rebasing into distgit
# May be added to based on group/image config
BASE_IGNORE = [".git", ".oit", "additional-tags"]

logger = logutil.getLogger(__name__)


def recursive_overwrite(src, dest, ignore=set()):
    """
    Use rsync to copy one file tree to a new location
    """
    exclude = ' --exclude .git/ '
    for i in ignore:
        exclude += ' --exclude="{}" '.format(i)
    cmd = 'rsync -av {} {}/ {}/'.format(exclude, src, dest)
    exectools.cmd_assert(cmd, retries=3)


def pull_image(url):
    logger.info("Pulling image: %s" % url)

    def wait(_):
        logger.info("Error pulling image %s -- retrying in 60 seconds" % url)
        time.sleep(60)

    exectools.retry(
        3, wait_f=wait,
        task_f=lambda: exectools.cmd_gather(["podman", "pull", url])[0] == 0)


def convert_source_url_to_https(source):
    url = re.sub(
        pattern=r'git@([^:]+):([^\.]+)',
        repl='https://\\1/\\2',
        string=source,
    )
    return re.sub(string=url, pattern=r'\.git$', repl='')


def build_image_ref_name(name):
    return 'openshift/ose-' + re.sub(pattern='^ose-', repl='', string=name)


class DistGitRepo(object):
    def __init__(self, metadata, autoclone=True):
        self.metadata = metadata
        self.config = metadata.config
        self.runtime = metadata.runtime
        self.name = self.metadata.name
        self.distgit_dir = None
        self.build_status = False
        self.push_status = False

        self.branch = self.runtime.branch

        self.source_sha = None
        self.source_full_sha = None
        self.source_latest_tag = None
        self.source_date_epoch = None
        self.source_url = None

        # Allow the config yaml to override branch
        # This is primarily useful for a sync only group.
        if self.config.distgit.branch is not Missing:
            self.branch = self.config.distgit.branch

        self.logger = self.metadata.logger

        # Initialize our distgit directory, if necessary
        if autoclone:
            self.clone(self.runtime.distgits_dir, self.branch)

    def clone(self, distgits_root_dir, distgit_branch):
        with Dir(distgits_root_dir):

            namespace_dir = os.path.join(distgits_root_dir, self.metadata.namespace)

            # It is possible we have metadata for the same distgit twice in a group.
            # There are valid scenarios (when they represent different branches) and
            # scenarios where this is a user error. In either case, make sure we
            # don't conflict by stomping on the same git directory.
            self.distgit_dir = os.path.join(namespace_dir, self.metadata.distgit_key)

            fake_distgit = (self.runtime.local and 'content' in self.metadata.config)

            if os.path.isdir(self.distgit_dir):
                self.logger.info("Distgit directory already exists; skipping clone: %s" % self.distgit_dir)
            else:

                # Make a directory for the distgit namespace if it does not already exist
                try:
                    os.mkdir(namespace_dir)
                except OSError as e:
                    if e.errno != errno.EEXIST:
                        raise

                if fake_distgit and self.runtime.command in ['images:rebase', 'images:update-dockerfile']:
                    cmd_list = ['mkdir', '-p', self.distgit_dir]
                    self.logger.info("Creating local build dir: {}".format(self.distgit_dir))
                    exectools.cmd_assert(cmd_list)
                else:
                    if self.runtime.command == 'images:build':
                        yellow_print('Warning: images:rebase was skipped and therefore your '
                                     'local build will be sourced from the current dist-git '
                                     'contents and not the typical GitHub source. '
                                     )
                    cmd_list = ["timeout", str(self.runtime.global_opts['rhpkg_clone_timeout']), "rhpkg"]

                    if self.runtime.rhpkg_config_lst:
                        cmd_list.extend(self.runtime.rhpkg_config_lst)

                    if self.runtime.user is not None:
                        cmd_list.append("--user=%s" % self.runtime.user)

                    cmd_list.extend(["clone", self.metadata.qualified_name, self.distgit_dir])
                    cmd_list.extend(["--branch", distgit_branch])

                    self.logger.info("Cloning distgit repository [branch:%s] into: %s" % (distgit_branch, self.distgit_dir))

                    # Clone the distgit repository. Occasional flakes in clone, so use retry.
                    set_env = os.environ.copy()
                    set_env.update(constants.GIT_NO_PROMPTS)
                    exectools.cmd_assert(cmd_list, retries=3, set_env=set_env)

    def merge_branch(self, target, allow_overwrite=False):
        self.logger.info('Switching to branch: {}'.format(target))
        cmd = ["rhpkg"]
        if self.runtime.rhpkg_config_lst:
            cmd.extend(self.runtime.rhpkg_config_lst)
        cmd.extend(["switch-branch", target])
        exectools.cmd_assert(cmd, retries=3)
        if not allow_overwrite:
            if os.path.isfile('Dockerfile') or os.path.isdir('.oit'):
                raise IOError('Unable to continue merge. Dockerfile found in target branch. Use --allow-overwrite to force.')
        self.logger.info('Merging source branch history over current branch')
        msg = 'Merge branch {} into {}'.format(self.branch, target)
        exectools.cmd_assert(
            cmd=['git', 'merge', '--allow-unrelated-histories', '-m', msg, self.branch],
            retries=3,
            on_retry=['git', 'reset', '--hard', target],  # in case merge failed due to storage
        )

    def has_source(self):
        """
        Check whether this dist-git repo has source content
        """
        return "git" in self.config.content.source or \
            "alias" in self.config.content.source

    def source_path(self):
        """
        :return: Returns the directory containing the source which should be used to populate distgit.
        """

        source_root = self.runtime.resolve_source(self.name, self.metadata)
        sub_path = self.config.content.source.path

        path = source_root
        if sub_path is not Missing:
            path = os.path.join(source_root, sub_path)

        assertion.isdir(path, "Unable to find path for source [%s] for config: %s" % (path, self.metadata.config_filename))
        return path

    def commit(self, commit_message, log_diff=False):
        if self.runtime.local:
            return ''  # no commits if local

        with Dir(self.distgit_dir):
            self.logger.info("Adding commit to local repo: {}".format(commit_message))
            if log_diff:
                rc, out, err = exectools.cmd_gather(["git", "diff", "Dockerfile"])
                assertion.success(rc, 'Failed fetching distgit diff')
                self.runtime.add_distgits_diff(self.metadata.distgit_key, out)
            if self.source_sha:
                # add short sha of source for audit trail
                commit_message += " - {}".format(self.source_sha)
            commit_message += "\n- MaxFileSize: {}".format(50000000)  # set dist-git size limit to 50MB
            # commit changes; if these flake there is probably not much we can do about it
            exectools.cmd_assert(["git", "add", "-A", "."])
            exectools.cmd_assert(["git", "commit", "--allow-empty", "-m", commit_message])
            rc, sha, err = exectools.cmd_gather(["git", "rev-parse", "HEAD"])
            assertion.success(rc, "Failure fetching commit SHA for {}".format(self.distgit_dir))
        return sha.strip()

    def tag(self, version, release):
        if self.runtime.local:
            return ''  # no tags if local

        if version is None:
            return

        tag = '{}'.format(version)

        if release is not None:
            tag = '{}-{}'.format(tag, release)

        with Dir(self.distgit_dir):
            self.logger.info("Adding tag to local repo: {}".format(tag))
            exectools.cmd_gather(["git", "tag", "-f", tag, "-m", tag])

    def matches_source_commit(self, builds):
        """
        Check whether the commit that we recorded in the distgit content (according to cgit)
        matches the commit of the source (according to git ls-remote) and has been built
        (according to brew). Nothing is cloned and no existing clones are consulted.
        :param builds: dict of builds, each entry an NVR like "pkgname": tuple("4.0.0", "123.dist")
        Returns: bool
        """
        source_details = self.config.content.source.git
        alias = self.config.content.source.alias
        if source_details is Missing and alias is not Missing:
            source_details = self.runtime.group_config.sources[alias]
        if source_details is Missing:
            commit_hash = None  # source is in distgit; just check if it has built
        else:
            _, commit_hash = self.runtime.detect_remote_source_branch(dict(source_details))
        return self._matches_commit(commit_hash, builds)

    def release_is_recent(self, release):
        # believe it or not, there is no good (generic) way to determine a remote commit timestamp
        # without cloning it or parsing html from cgit. therefore, if so configured, we rely on the
        # only timestamp we can easily get, in the release value; otherwise just consider it stale.

        release_re = self.runtime.group_config.scan_freshness.release_regex
        threshold_hours = self.runtime.group_config.scan_freshness.threshold_hours
        if release_re is Missing or threshold_hours is Missing:
            self.logger.debug("scan_freshness is not configured; treating distgit as stale")
            return False

        match = re.search(release_re, release)
        if not match:
            self.logger.debug("release doesn't match release_regex; treating distgit as stale")
            return False
        timestamp = [int(f) for f in match.groups()]
        # naively assume local timezone matches timestamp timezone for comparison
        if datetime.now() - datetime(*timestamp) < timedelta(hours=threshold_hours):
            self.logger.debug(
                "distgit timetamp within {} hours; will wait to count it as stale"
                .format(threshold_hours)
            )
            return True
        self.logger.debug(
            "distgit timetamp is more than {} hours stale"
            .format(threshold_hours)
        )
        return False


class ImageDistGitRepo(DistGitRepo):

    source_labels = dict(
        old=dict(
            sha='io.openshift.source-repo-commit',
            source='io.openshift.source-repo-url',
            source_commit='io.openshift.source-commit-url',
        ),
        now=dict(
            sha='io.openshift.build.commit.id',
            source='io.openshift.build.source-location',
            source_commit='io.openshift.build.commit.url',
        ),
    )

    def __init__(self, metadata, autoclone=True,
                 source_modifier_factory=SourceModifierFactory()):
        super(ImageDistGitRepo, self).__init__(metadata, autoclone)
        self.build_lock = Lock()
        self.build_lock.acquire()
        self.logger = metadata.logger
        self.source_modifier_factory = source_modifier_factory

    def clone(self, distgits_root_dir, distgit_branch):
        super(ImageDistGitRepo, self).clone(distgits_root_dir, distgit_branch)
        self._read_master_data()

    @property
    def image_build_method(self):
        build_method = self.runtime.group_config.default_image_build_method
        # If the build is multistage, override with 'imagebuilder' as required for multistage.
        if 'builder' in self.config.get('from', {}):
            build_method = 'imagebuilder'
        # If our config specifies something, override with that.
        if self.config.image_build_method is not Missing:
            build_method = self.config.image_build_method

        return build_method

    def _write_osbs_image_config(self):
        # Writes OSBS image config (container.yaml).
        # For more info about the format, see https://osbs.readthedocs.io/en/latest/users.html#image-configuration.

        self.logger.info('Generating container.yaml')
        container_config = self._generate_osbs_image_config()

        if 'compose' in container_config:
            self.logger.info("Rebasing with ODCS support")
        else:
            self.logger.info("Rebasing without ODCS support")

        # generate yaml data with header
        content_yml = yaml.safe_dump(container_config, default_flow_style=False)
        with io.open('container.yaml', 'w', encoding="utf-8") as rc:
            rc.write(CONTAINER_YAML_HEADER + content_yml)

    def _generate_osbs_image_config(self):
        """
        Generates OSBS image config (container.yaml).
        Returns a dict for the config.

        Example in image yml file:
        odcs:
            packages:
                mode: auto (default) | manual
                # auto - If container.yaml with packages is given from source, use them.
                #        Otherwise all packages with from the Koji build tag will be included.
                # manual - only use list below
                list:
                  - package1
                  - package2
        arches: # Optional list of image specific arches. If given, it must be a subset of group arches.
          - x86_64
          - s390x
        container_yaml: ... # verbatim container.yaml content (see https://mojo.redhat.com/docs/DOC-1159997)
        """

        # list of platform (architecture) names to build this image for
        arches = []  # type: list
        global_arches = set(self.metadata.runtime.arches)
        image_specific_arches = set(self.metadata.config.arches)
        if not image_specific_arches:
            # assume all global arches if no image specific arches are defined
            arches = list(global_arches)
        else:
            # only include arches from metadata that are valid globally
            missing_arches = image_specific_arches - global_arches
            if missing_arches:
                raise ValueError("Image specific arches ({}) are not enabled in group.yml.".format(", ".join(missing_arches)))
            arches = list(image_specific_arches)

        # override image config with this dict
        config_overrides = {}
        if self.config.container_yaml is not Missing:
            config_overrides = copy.deepcopy(self.config.container_yaml.primitive())
        if self.image_build_method is not Missing:
            config_overrides['image_build_method'] = self.image_build_method
        if arches:
            config_overrides.setdefault('platforms', {})['only'] = arches

        if not self.runtime.group_config.doozer_feature_gates.odcs_enabled and not self.runtime.odcs_mode:
            # ODCS mode is not enabled
            return config_overrides

        odcs = self.config.odcs
        if odcs is Missing:
            # image yml doesn't have `odcs` field defined
            if not self.runtime.group_config.doozer_feature_gates.odcs_aggressive:
                # Doozer's odcs_aggressive feature gate is off, disable ODCS mode for this image
                return config_overrides
            self.logger.warning("Enforce ODCS auto mode because odcs_aggressive feature gate is on")

        package_mode = odcs.packages.get('mode', 'auto')
        valid_package_modes = ['auto', 'manual']
        if package_mode not in valid_package_modes:
            raise ValueError('odcs.packages.mode must be one of {}'.format(', '.join(valid_package_modes)))

        # generate container.yaml content for ODCS
        config = {}
        if self.has_source():  # if upstream source provides container.yaml, load it.
            source_container_yaml = os.path.join(self.source_path(), 'container.yaml')
            if os.path.isfile(source_container_yaml):
                with open(source_container_yaml, 'r') as scy:
                    config = yaml.full_load(scy)

        # ensure defaults
        config.setdefault('compose', {}).setdefault('pulp_repos', True)

        # create package list for ODCS, see https://osbs.readthedocs.io/en/latest/users.html#compose
        if package_mode == 'auto':
            if isinstance(config["compose"].get("packages"), list):
                # container.yaml with packages was given from source
                self.logger.info("Use ODCS package list from source")
            else:
                config["compose"]["packages"] = []  # empty list composes all packages from the current Koji target
        elif package_mode == 'manual':
            if not odcs.packages.list:
                raise ValueError('odcs.packages.mode == manual but none specified in odcs.packages.list')
            config["compose"]["packages"] = list(odcs.packages.list)

        # apply overrides
        config.update(config_overrides)
        return config

    def _write_cvp_owners(self):
        """
        The Container Verification Pipeline will notify image owners when their image is
        not passing CVP tests. ART knows these owners and needs to write them into distgit
        for CVP to find.
        :return:
        """
        self.logger.debug("Generating cvp-owners.yml for {}".format(self.metadata.distgit_key))
        with io.open('cvp-owners.yml', 'w', encoding="utf-8") as co:
            if self.config.owners:  # Not missing and non-empty
                yaml.safe_dump(self.config.owners.primitive(), co, default_flow_style=False)

    def _generate_repo_conf(self):
        """
        Generates a repo file in .oit/repo.conf
        """

        self.logger.debug("Generating repo file for Dockerfile {}".format(self.metadata.distgit_key))

        # Make our metadata directory if it does not exist
        if not os.path.isdir(".oit"):
            os.mkdir(".oit")

        repos = self.runtime.repos
        enabled_repos = self.config.get('enabled_repos', [])
        for t in repos.repotypes:
            with io.open('.oit/{}.repo'.format(t), 'w', encoding="utf-8") as rc:
                content = repos.repo_file(t, enabled_repos=enabled_repos)
                rc.write(content)

        with io.open('content_sets.yml', 'w', encoding="utf-8") as rc:
            rc.write(repos.content_sets(enabled_repos=enabled_repos))

    def _read_master_data(self):
        with Dir(self.distgit_dir):
            self.org_image_name = None
            self.org_version = None
            self.org_release = None
            # Read in information about the image we are about to build
            dockerfile = os.path.join(Dir.getcwd(), 'Dockerfile')
            if os.path.isfile(dockerfile):
                dfp = DockerfileParser(path=dockerfile)
                self.org_image_name = dfp.labels.get("name")
                self.org_version = dfp.labels.get("version")
                self.org_release = dfp.labels.get("release")  # occasionally no release given

    def push_image(self, tag_list, push_to_defaults, additional_registries=[], version_release_tuple=None,
                   push_late=False, dry_run=False):
        """
        Pushes the most recent image built for this distgit repo. This is
        accomplished by looking at the 'version' field in the Dockerfile or
        the version_release_tuple argument and querying
        brew for the most recent images built for that version.
        :param tag_list: The list of tags to apply to the image (overrides default tagging pattern).
        :param push_to_defaults: Boolean indicating whether group/image yaml defined registries should be pushed to.
        :param additional_registries: A list of non-default registries (optional namespace included) to push the image to.
        :param version_release_tuple: Specify a version/release to pull as the source (if None, the latest build will be pulled).
        :param push_late: Whether late pushes should be included.
        :param dry_run: Will only print the docker operations that would have taken place.
        :return: Returns True if successful (exception otherwise)
        """

        # Late pushes allow certain images to be the last of a group to be
        # pushed to mirrors. CI/CD systems may initiate operations based on the
        # update a given image and all other images need to be in place
        # when that special image is updated. The special images are there
        # pushed "late"
        # Actions that need to push all images need to push all images
        # need to make two passes/invocations of this method: one
        # with push_late=False and one with push_late=True.

        is_late_push = False
        if self.config.push.late is not Missing:
            is_late_push = self.config.push.late

        if push_late != is_late_push:
            return (self.metadata.distgit_key, True)

        push_names = []

        if push_to_defaults:
            push_names.extend(self.metadata.get_default_push_names())

        push_names.extend(self.metadata.get_additional_push_names(additional_registries))

        # Nothing to push to? We are done.
        if not push_names:
            return (self.metadata.distgit_key, True)

        with Dir(self.distgit_dir):

            if version_release_tuple:
                version = version_release_tuple[0]
                release = version_release_tuple[1]
            else:

                # History
                # We used to rely on the "release" label being set in the Dockerfile, but this is problematic for several reasons.
                # (1) If 'release' is not set, OSBS will determine one automatically that does not conflict
                #       with a pre-existing image build. This is extremely helpful since we don't have to
                #       worry about bumping the release during refresh images. This means we generally DON'T
                #       want the release label in the file and can't, therefore, rely on it being there.
                # (2) People have logged into distgit before in order to bump the release field. This happening
                #       at the wrong time breaks the build.

                # If the version & release information was not specified,
                # try to detect latest build from brew.
                # Read in version information from the Distgit dockerfile
                _, version, release = self.metadata.get_latest_build_info()

            image_name_and_version = "%s:%s-%s" % (self.config.name, version, release)

            # https://mojo.redhat.com/docs/DOC-1204856
            # we need to convert into 'registry-proxy.engineering.redhat.com/rh-osbs/openshift-ose-installer-artifacts'
            # from 'brew-pulp-docker01.web.prod.ext.phx2.redhat.com:8888/openshift/ose-installer-artifacts'
            if self.runtime.group_config.urls.brew_image_namespace is not Missing:
                url = self.runtime.group_config.urls.brew_image_host
                ns = self.runtime.group_config.urls.brew_image_namespace
                name = image_name_and_version.replace('/', '-')
                brew_image_url = "/".join(("/".join((url, ns)), name))
            else:
                brew_image_url = "/".join((self.runtime.group_config.urls.brew_image_host, image_name_and_version))

            push_tags = list(tag_list)

            # If no tags were specified, build defaults
            if not push_tags:
                push_tags = self.metadata.get_default_push_tags(version, release)

            all_push_urls = []

            for image_name in push_names:
                try:
                    repo = image_name.split('/', 1)

                    action = "push"
                    record = {
                        "distgit_key": self.metadata.distgit_key,
                        "distgit": '{}/{}'.format(self.metadata.namespace, self.metadata.name),
                        "repo": repo,  # ns/repo
                        "name": image_name,  # full registry/ns/repo
                        "version": version,
                        "release": release,
                        "message": "Unknown failure",
                        "tags": ", ".join(push_tags),
                        "status": -1,
                        # Status defaults to failure until explicitly set by success. This handles raised exceptions.
                    }

                    for push_tag in push_tags:
                        # Collect next SRC=DEST input
                        url = '{}:{}'.format(image_name, push_tag)
                        self.logger.info("Adding '{}' to push list".format(url))
                        all_push_urls.append("{}={}".format(brew_image_url, url))

                    if dry_run:
                        for push_url in all_push_urls:
                            self.logger.info('Would have tagged {} as {}'.format(brew_image_url, push_url.split('=')[1]))
                        dr = "--dry-run=true"
                    else:
                        dr = ""

                    if self.runtime.group_config.insecure_source:
                        insecure = "--insecure=true"
                    else:
                        insecure = ""

                    push_config_dir = os.path.join(self.runtime.working_dir, 'push')
                    if not os.path.isdir(push_config_dir):
                        try:
                            os.mkdir(push_config_dir)
                        except OSError as e:
                            # File exists, and it's a directory,
                            # another thread already created this dir, that's OK.
                            if e.errno == errno.EEXIST and os.path.isdir(push_config_dir):
                                pass
                            else:
                                raise

                    push_config = os.path.join(push_config_dir, self.metadata.distgit_key)

                    if os.path.isfile(push_config):
                        # just delete it to ease creating new config
                        os.remove(push_config)

                    with io.open(push_config, 'w', encoding="utf-8") as pc:
                        pc.write('\n'.join(all_push_urls))

                    mirror_cmd = 'oc image mirror --filter-by-os=amd64 {} {} --filename={}'.format(dr, insecure, push_config)

                    if dry_run:  # skip everything else if dry run
                        continue
                    else:
                        for r in range(10):
                            self.logger.info("Mirroring image [retry={}]".format(r))
                            rc, out, err = exectools.cmd_gather(mirror_cmd)
                            if rc == 0:
                                break
                            self.logger.info("Error mirroring image -- retrying in 60 seconds.\n{}".format(err))
                            time.sleep(60)

                        lstate = self.runtime.state[self.runtime.command] if self.runtime.command == 'images:push' else None

                        if rc != 0:
                            if lstate:
                                state.record_image_fail(lstate, self.metadata, 'Build failure', self.runtime.logger)
                            # Unable to push to registry
                            raise IOError('Error pushing image: {}'.format(err))
                        else:
                            if lstate:
                                state.record_image_success(lstate, self.metadata)
                            self.logger.info('Success mirroring image')

                        record["message"] = "Successfully pushed all tags"
                        record["status"] = 0

                except Exception as ex:
                    lstate = self.runtime.state[self.runtime.command] if self.runtime.command == 'images:push' else None
                    if lstate:
                        state.record_image_fail(lstate, self.metadata, str(ex), self.runtime.logger)

                    record["message"] = "Exception occurred: %s" % str(ex)
                    self.logger.info("Error pushing %s: %s" % (self.metadata.distgit_key, ex))
                    raise

                finally:
                    self.runtime.add_record(action, **record)

            return (self.metadata.distgit_key, True)

    def wait_for_build(self, who_is_waiting):
        """
        Blocks the calling thread until this image has been built by doozer or throws an exception if this
        image cannot be built.
        :param who_is_waiting: The caller's distgit_key (i.e. the waiting image).
        :return: Returns when the image has been built or throws an exception if the image could not be built.
        """
        self.logger.info("Member waiting for me to build: %s" % who_is_waiting)
        # This lock is in an acquired state until this image definitively succeeds or fails.
        # It is then released. Child images waiting on this image should block here.
        with self.build_lock:
            if not self.build_status:
                raise IOError(
                    "Error building image: %s (%s was waiting)" % (self.metadata.qualified_name, who_is_waiting))
            else:
                self.logger.info("Member successfully waited for me to build: %s" % who_is_waiting)

    def _set_wait_for(self, image_name, terminate_event):
        image = self.runtime.resolve_image(image_name, False)
        if image is None:
            self.logger.info("Skipping image build since it is not included: %s" % image_name)
            return
        parent_dgr = image.distgit_repo()
        parent_dgr.wait_for_build(self.metadata.qualified_name)
        if terminate_event.is_set():
            raise KeyboardInterrupt()

    def build_container(
            self, repo_type, repo, push_to_defaults, additional_registries, terminate_event,
            scratch=False, retries=3, realtime=False, dry_run=False):
        """
        This method is designed to be thread-safe. Multiple builds should take place in brew
        at the same time. After a build, images are pushed serially to all mirrors.
        DONT try to change cwd during this time, all threads active will change cwd
        :param repo_type: Repo type to choose from group.yml
        :param repo: A list/tuple of custom repo URLs to include for build
        :param push_to_defaults: If default registries should be pushed to.
        :param additional_registries: A list of non-default registries resultant builds should be pushed to.
        :param terminate_event: Allows the main thread to interrupt the build.
        :param scratch: Whether this is a scratch build. UNTESTED.
        :param retries: Number of times the build should be retried.
        :return: True if the build was successful
        """
        if self.org_image_name is None or self.org_version is None:
            if not os.path.isfile(os.path.join(self.distgit_dir, 'Dockerfile')):
                msg = ('No Dockerfile found in {}'.format(self.distgit_dir))
            else:
                msg = ('Unknown error loading Dockerfile information')

            self.logger.info(msg)
            state.record_image_fail(self.runtime.state[self.runtime.command], self.metadata, msg, self.runtime.logger)
            return (self.metadata.distgit_key, False)

        action = "build"
        release = self.org_release if self.org_release is not None else '?'
        record = {
            "dir": self.distgit_dir,
            "dockerfile": "%s/Dockerfile" % self.distgit_dir,
            "distgit": self.metadata.distgit_key,
            "image": self.org_image_name,
            "owners": ",".join(list(self.config.owners) if self.config.owners is not Missing else []),
            "version": self.org_version,
            "release": release,
            "message": "Unknown failure",
            "task_id": "n/a",
            "task_url": "n/a",
            "status": -1,
            "push_status": -1,
            # Status defaults to failure until explicitly set by success. This handles raised exceptions.
        }

        if self.runtime.local and release == '?':
            target_tag = self.org_version
        else:
            target_tag = "{}-{}".format(self.org_version, release)
        target_image = ":".join((self.org_image_name, target_tag))

        try:
            if not self.runtime.local and not scratch and self.org_release is not None \
                    and self.metadata.tag_exists(target_tag):
                self.logger.info("Image already built for: {}".format(target_image))
            else:
                # If this image is FROM another group member, we need to wait on that group member
                # Use .get('from',None) since from is a reserved word.
                image_from = Model(self.config.get('from', None))
                if image_from.member is not Missing:
                    self._set_wait_for(image_from.member, terminate_event)
                for builder in image_from.get('builder', []):
                    if 'member' in builder:
                        self._set_wait_for(builder['member'], terminate_event)

                # Allow an image to wait on an arbitrary image in the group. This is presently
                # just a workaround for: https://projects.engineering.redhat.com/browse/OSBS-5592
                if self.config.wait_for is not Missing:
                    self._set_wait_for(self.config.wait_for, terminate_event)

                if self.runtime.local:
                    self.build_status = self._build_container_local(target_image, repo_type, realtime)
                    if not self.build_status:
                        state.record_image_fail(self.runtime.state[self.runtime.command], self.metadata, 'Build failure', self.runtime.logger)
                    else:
                        state.record_image_success(self.runtime.state[self.runtime.command], self.metadata)
                    return (self.metadata.distgit_key, self.build_status)  # do nothing more since it's local only
                else:
                    def wait(n):
                        self.logger.info("Async error in image build thread [attempt #{}]".format(n + 1))
                        # No need to retry if the failure will just recur
                        error = self._detect_permanent_build_failures(self.runtime.group_config.image_build_log_scanner)
                        if error is not None:
                            for match in re.finditer("No package (.*) available", error):
                                self._add_missing_pkgs(match.group(1))
                            raise exectools.RetryException(
                                "Saw permanent error in build logs:\n{}\nWill not retry after {} failed attempt(s)"
                                .format(error, n + 1)
                            )
                        # Brew does not handle an immediate retry correctly, wait
                        # before trying another build, terminating if interrupted.
                        if terminate_event.wait(timeout=5 * 60):
                            raise KeyboardInterrupt()

                    exectools.retry(
                        retries=(1 if self.runtime.local else retries), wait_f=wait,
                        task_f=lambda: self._build_container(
                            target_image, repo_type, repo, terminate_event,
                            scratch, record, dry_run=dry_run))

            # Just in case someone else is building an image, go ahead and find what was just
            # built so that push_image will have a fixed point of reference and not detect any
            # subsequent builds.
            push_version, push_release = ('', '')
            if not scratch:
                _, push_version, push_release = self.metadata.get_latest_build_info()
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
            # Regardless of success, allow other images depending on this one to progress or fail.
            self.build_lock.release()

        self.push_status = True  # if if never pushes, the status is True
        if not scratch and self.build_status and (push_to_defaults or additional_registries):
            # If this is a scratch build, we aren't going to be pushing. We might be able to determine the
            # image name by parsing the build log, but not worth the effort until we need scratch builds.
            # The image name for a scratch build looks something like:
            # brew-pulp-docker01.web.prod.ext.phx2.redhat.com:8888/openshift3/ose-base:rhaos-3.7-rhel-7-docker-candidate-16066-20170829214444

            # To ensure we don't overwhelm the system building, pull & push synchronously
            with self.runtime.mutex:
                self.push_status = False
                try:
                    self.push_image([], push_to_defaults, additional_registries, version_release_tuple=(push_version, push_release))
                    self.push_status = True
                except Exception as push_e:
                    self.logger.info("Error during push after successful build: %s" % str(push_e))
                    self.push_status = False

        record['push_status'] = '0' if self.push_status else '-1'

        self.runtime.add_record(action, **record)
        lstate = self.runtime.state[self.runtime.command]
        if not (self.build_status and self.push_status):
            state.record_image_fail(lstate, self.metadata, 'Build failure', self.runtime.logger)
        else:
            state.record_image_success(lstate, self.metadata)
        return (self.metadata.distgit_key, self.build_status and self.push_status)

    def _build_container_local(self, target_image, repo_type, realtime=False):
        """
        The part of `build_container` which actually starts the build,
        separated for clarity. Local build version.
        """

        if self.image_build_method == 'imagebuilder':
            builder = 'imagebuilder -mount '
        else:
            builder = 'podman build -v '

        cmd = builder
        self.logger.info("Building image: %s" % target_image)
        cmd += '{dir}/.oit/{repo_type}.repo:/etc/yum.repos.d/{repo_type}.repo -t {name}{tag} -t {name}:latest .'

        name_split = target_image.split(':')
        name = name_split[0]
        tag = None
        if len(name_split) > 1:
            tag = name_split[1]

        args = {
            'dir': self.distgit_dir,
            'repo_type': repo_type,
            'name': name,
            'tag': ':{}'.format(tag) if tag else ''
        }

        cmd = cmd.format(**args)

        with Dir(self.distgit_dir):
            self.logger.info(cmd)
            rc, out, err = exectools.cmd_gather(cmd, realtime=True)

            if rc != 0:
                self.logger.error("Error running {}: out={}  ; err={}".format(builder, out, err))
                return False

            self.logger.debug(out + '\n\n' + err)

            self.logger.info("Successfully built image: {}".format(target_image))
        return True

    def _build_container(
            self, target_image, repo_type, repo_list, terminate_event,
            scratch, record, dry_run=False):
        """
        The part of `build_container` which actually starts the build,
        separated for clarity. Brew build version.
        """
        self.logger.info("Building image: %s" % target_image)
        cmd_list = ["rhpkg"]
        if self.runtime.rhpkg_config_lst:
            cmd_list.extend(self.runtime.rhpkg_config_lst)

        cmd_list.append("--path=%s" % self.distgit_dir)

        if self.runtime.user is not None:
            cmd_list.append("--user=%s" % self.runtime.user)

        cmd_list += (
            "container-build",
            "--nowait",  # Run the build with --nowait so that we can immediately get information about the brew task
        )

        # Determine if ODCS is enabled by looking at container.yaml.
        odcs_enabled = False
        osbs_image_config_path = os.path.join(self.distgit_dir, "container.yaml")
        if os.path.isfile(osbs_image_config_path):
            with open(osbs_image_config_path, "r") as f:
                image_config = yaml.safe_load(f)
            odcs_enabled = "compose" in image_config

        if odcs_enabled:
            self.logger.info("About to build image in ODCS mode.")
            if repo_type == 'signed':
                signing_intent = 'release'
            else:
                signing_intent = repo_type
            if signing_intent:
                cmd_list.append('--signing-intent')
                cmd_list.append(signing_intent)
        else:
            if repo_type:
                repo_list = list(repo_list)  # In case we get a tuple
                repo_list.append(self.metadata.cgit_url(".oit/" + repo_type + ".repo"))

            if repo_list:
                # rhpkg supports --repo-url [URL [URL ...]]
                cmd_list.append("--repo-url")
                cmd_list.extend(repo_list)

        if scratch:
            cmd_list.append("--scratch")

        if dry_run:
            # `rhpkg --dry-run container-build` doesn't really work. Print the command instread.
            self.logger.warning("[Dry Run] Would have run command " + " ".join(cmd_list))
            self.logger.info("[Dry Run] Successfully built image: {}".format(target_image))
            return True

        # Run the build with --nowait so that we can immediately get information about the brew task
        rc, out, err = exectools.cmd_gather(cmd_list)

        if rc != 0:
            # Probably no point in continuing.. can't contact brew?
            self.logger.info("Unable to create brew task: out={}  ; err={}".format(out, err))
            return False

        # Otherwise, we should have a brew task we can monitor listed in the stdout.
        out_lines = out.splitlines()

        # Look for a line like: "Created task: 13949050" . Extract the identifier.
        task_id = next((created_line.split(":")[1]).strip() for created_line in out_lines if
                       created_line.startswith("Created task:"))

        record["task_id"] = task_id

        # Look for a line like: "Task info: https://brewweb.engineering.redhat.com/brew/taskinfo?taskID=13948942"
        task_url = next((info_line.split(":", 1)[1]).strip() for info_line in out_lines if
                        info_line.startswith("Task info:"))

        self.logger.info("Build running: {}".format(task_url))

        record["task_url"] = task_url

        # Now that we have the basics about the task, wait for it to complete
        error = watch_task(self.runtime.group_config.urls.brewhub, self.logger.info, task_id, terminate_event)

        # Looking for something like the following to conclude the image has already been built:
        # BuildError: Build for openshift-enterprise-base-v3.7.0-0.117.0.0 already exists, id 588961
        if error is not None and "already exists" in error:
            self.logger.info("Image already built against this dist-git commit (or version-release tag): {}".format(target_image))
            error = None

        # Gather brew-logs
        logs_rc, _, logs_err = exectools.cmd_gather(["brew", "download-logs", "-d", self._logs_dir(), task_id])

        if logs_rc != 0:
            self.logger.info("Error downloading build logs from brew for task %s: %s" % (task_id, logs_err))
        else:
            self._extract_container_build_logs(task_id)
            if error is None:
                # even if the build completed without error, check the logs for problems
                error = self._detect_permanent_build_failures(self.runtime.group_config.image_build_log_scanner)

        if error is not None:
            # An error occurred. We don't have a viable build.
            self.logger.info("Error building image: {}, {}".format(task_url, error))
            return False

        self.logger.info("Successfully built image: {} ; {}".format(target_image, task_url))
        return True

    def _logs_dir(self, task_id=None):
        segments = [self.runtime.brew_logs_dir, self.metadata.distgit_key]
        if task_id is not None:
            segments.append("noarch-" + task_id)
        return os.path.join(*segments)

    def _extract_container_build_logs(self, task_id):
        """
        Look through build logs and extract the part that's just the actual docker or imagebuilder build
        so that devs do not have to wade through the full atomic_reactor logs.
        If found in a log, the extracted text is written to the same filename with prefix "container-build-"

        :param task_id: string with the build task so we can find the downloaded logs
        """
        logs_dir = self._logs_dir(task_id)
        try:
            for filename in os.listdir(logs_dir):
                rc, output, _ = exectools.cmd_gather([
                    "sed", "-nEe",
                    # look in logs for either of the build plugins to start,
                    # and look for the plugin runner to declare the plugin done to end.
                    # strip off all the noise at the front of log lines.
                    "/plugins.(imagebuilder|docker_api)/,/atomic_reactor.plugin / { s/^.*?- (DEBUG|INFO) - //; p }",
                    os.path.join(logs_dir, filename)
                ])
                if rc == 0 and output:
                    extracted = os.path.join(logs_dir, "container-build-" + filename)
                    with io.open(extracted, "w", encoding="utf-8") as extracted_file:
                        extracted_file.write(output)
        except OSError as e:
            self.logger.warning("Exception while trying to extract build logs in {}: {}".format(logs_dir, e))

    def _add_missing_pkgs(self, pkg_name):
        """
        add missing packages to runtime.missing_pkgs set
        :param pkg_name: Missing packages like: No package (.*) available
        """
        with self.runtime.mutex:
            self.runtime.missing_pkgs.add("{} image is missing package {}".format(self.metadata.distgit_key, pkg_name))

    def _detect_permanent_build_failures(self, scanner):
        """
        check logs to determine if this container build failed for a known non-flake reason
        :param scanner: Model object with fields
                        "files" (list of log files to search)
                        "matches" (list of compiled regexen for matching a line in the log)
        """
        if not scanner or not scanner.matches or not scanner.files:
            self.logger.debug("Log file scanning not specified; skipping")
            return None
        logs_dir = self._logs_dir()
        try:
            # find the most recent subdir, which should contain logs for the latest build task
            task_dirs = [os.path.join(logs_dir, d) for d in os.listdir(logs_dir)]
            task_dirs = [d for d in task_dirs if os.path.isdir(d)]
            if not task_dirs:
                self.logger.info("No task logs found under {}; cannot analyze logs".format(logs_dir))
                return None
            latest_dir = max(task_dirs, key=os.path.getmtime)

            # check the log files for recognizable problems
            found_problems = []
            for log_file in os.listdir(latest_dir):
                if log_file not in scanner.files:
                    continue
                with io.open(os.path.join(latest_dir, log_file), encoding="utf-8") as log:
                    for line in log:
                        for regex in scanner.matches:
                            match = regex.search(line)
                            if match:
                                found_problems.append(log_file + ": " + match.group(0))
            if found_problems:
                found_problems.insert(0, "Problem indicators found in logs under " + latest_dir)
                return "\n".join(found_problems)
        except OSError as e:
            self.logger.warning("Exception while trying to analyze build logs in {}: {}".format(logs_dir, e))

    def push(self):
        with Dir(self.distgit_dir):
            self.logger.info("Pushing repository")
            try:
                timeout = str(self.runtime.global_opts['rhpkg_push_timeout'])
                exectools.cmd_assert("timeout {} rhpkg push".format(timeout), retries=3)
                # rhpkg will create but not push tags :(
                # Not asserting this exec since this is non-fatal if a tag already exists,
                # and tags in dist-git can't be --force overwritten
                exectools.cmd_gather(['timeout', '60', 'git', 'push', '--tags'])
            except IOError as e:
                return (self.metadata, repr(e))
            return (self.metadata, True)

    @staticmethod
    def _mangle_yum(cmd):
        # alter the arg by splicing its content
        def splice(pos, replacement):
            return cmd[:pos[0]] + replacement + cmd[pos[1]:]
        changed = False  # were there changes aside from whitespace?

        # build a list of nodes we may want to alter from the AST
        cmd_nodes = []

        def append_nodes_from(node):
            if node.kind in ["list", "compound"]:
                sublist = node.parts if node.kind == "list" else node.list
                for subnode in sublist:
                    append_nodes_from(subnode)
            elif node.kind in ["operator", "command"]:
                cmd_nodes.append(node)

        try:
            append_nodes_from(bashlex.parse(cmd)[0])
        except bashlex.errors.ParsingError as e:
            raise IOError("Error while parsing Dockerfile RUN command:\n{}\n{}".format(cmd, e))

        # note: make changes working back from the end so that positions to splice don't change
        for subcmd in reversed(cmd_nodes):

            if subcmd.kind == "operator":
                # we lose the line breaks in the original dockerfile,
                # so try to format nicely around operators -- more readable git diffs.
                cmd = splice(subcmd.pos, "\\\n " + subcmd.op)
                continue  # not "changed" logically however

            # replace yum-config-manager with a no-op
            if re.search(r'(^|/)yum-config-manager$', subcmd.parts[0].word):
                cmd = splice(subcmd.pos, ": 'removed yum-config-manager'")
                changed = True
                continue

            # clear repo options from yum commands
            if not re.search(r'(^|/)yum$', subcmd.parts[0].word):
                continue
            next_word = None
            for word in reversed(subcmd.parts):
                if word.kind != "word":
                    next_word = None
                    continue

                # seek e.g. "--enablerepo=foo" or "--disablerepo bar"
                match = re.match(r'--(en|dis)ablerepo(=|$)', word.word)
                if match:
                    if next_word and match.group(2) != "=":
                        # no "=", next word is the repo so remove it too
                        cmd = splice(next_word.pos, "")
                    cmd = splice(word.pos, "")
                    changed = True
                next_word = word

            # note: there are a number of ways to defeat this logic, for instance by
            # wrapping commands/args in quotes, or with commands that aren't valid
            # to begin with. let's not worry about that; it need not be invulnerable.

        return changed, cmd

    def _clean_repos(self, dfp):
        """
        Remove any calls to yum --enable-repo or
        yum-config-manager in RUN instructions
        """
        for entry in reversed(dfp.structure):
            if entry['instruction'] == 'RUN':
                changed, new_value = self._mangle_yum(entry['value'])
                if changed:
                    dfp.add_lines_at(entry, "RUN " + new_value, replace=True)

    def update_distgit_dir(self, version, release, prev_release=None):
        ignore_missing_base = self.runtime.ignore_missing_base
        # A collection of comment lines that will be included in the generated Dockerfile. They
        # will be prefix by the OIT_COMMENT_PREFIX and followed by newlines in the Dockerfile.
        oit_comments = []

        with Dir(self.distgit_dir):
            # Source or not, we should find a Dockerfile in the root at this point or something is wrong
            assertion.isfile("Dockerfile", "Unable to find Dockerfile in distgit root")

            self._generate_repo_conf()

            self._write_osbs_image_config()

            self._write_cvp_owners()

            dfp = DockerfileParser(path="Dockerfile")

            self._clean_repos(dfp)

            # If no version has been specified, we will leave the version in the Dockerfile. Extract it.
            if version is None:
                version = dfp.labels.get("version", dfp.labels.get("Version", None))

                if version is None:
                    if self.runtime.local:
                        # fallback so the logic doesn't fall over
                        # but we can still build
                        version = "v0.0.0"
                    else:
                        DoozerFatalError("No version found in Dockerfile for %s" % self.metadata.qualified_name)

            uuid_tag = "%s.%s" % (version, self.runtime.uuid)

            if not self.runtime.local:
                with io.open('additional-tags', 'w', encoding="utf-8") as at:
                    at.write("%s\n" % uuid_tag)  # The uuid which we ensure we get the right
                    vsplit = version.split(".")
                    if len(vsplit) > 1:
                        at.write("%s.%s\n" % (vsplit[0], vsplit[1]))  # e.g. "v3.7.0" -> "v3.7"
                    if self.metadata.config.additional_tags is not Missing:
                        for tag in self.metadata.config.additional_tags:
                            at.write("{}\n".format(tag))

            self.logger.debug("Dockerfile contains the following labels:")
            for k, v in dfp.labels.items():
                self.logger.debug("  '%s'='%s'" % (k, v))

            # Set all labels in from config into the Dockerfile content
            if self.config.labels is not Missing:
                for k, v in self.config.labels.items():
                    dfp.labels[k] = v

            # Set the image name
            dfp.labels["name"] = self.config.name

            # Set the distgit repo name
            dfp.labels["com.redhat.component"] = self.metadata.get_component_name()

            # appregistry is managed in a separately-built metadata container (ref. ART-874)
            if "com.redhat.delivery.appregistry" in dfp.labels:
                dfp.labels["com.redhat.delivery.appregistry"] = "false"

            # record env vars that we need to set in the dockerfile
            env_vars = {}

            if 'from' in self.config:
                image_from = Model(self.config.get('from', None))

                # Collect all the parent images we're supposed to use
                parent_images = image_from.builder if image_from.builder is not Missing else []
                parent_images.append(image_from)
                if len(parent_images) != len(dfp.parent_images):
                    raise IOError("Build metadata for {name} expected {count1} image parent(s), but the upstream Dockerfile contains {count2} FROM statements. These counts must match. Detail: '{meta_parents}' vs '{upstream_parents}'.".format(
                        name=self.config.name,
                        count1=len(parent_images),
                        count2=len(dfp.parent_images),
                        meta_parents=parent_images,
                        upstream_parents=dfp.parent_images,
                    ))
                mapped_images = []

                original_parents = dfp.parent_images
                count = 0
                for image in parent_images:
                    # Does this image inherit from an image defined in a different distgit?
                    if image.member is not Missing:
                        base = image.member
                        from_image_metadata = self.runtime.resolve_image(base, False)

                        if from_image_metadata is None:
                            if not ignore_missing_base:
                                raise IOError("Unable to find base image metadata [%s] in included images. Use --ignore-missing-base to ignore." % base)
                            elif self.runtime.latest_parent_version:
                                self.logger.info('[{}] parent image {} not included. Looking up FROM tag.'.format(self.config.name, base))
                                base_meta = self.runtime.late_resolve_image(base)
                                _, v, r = base_meta.get_latest_build_info()
                                mapped_images.append("{}:{}-{}".format(base_meta.config.name, v, r))
                            # Otherwise, the user is not expecting the FROM field to be updated in this Dockerfile.
                            else:
                                mapped_images.append(original_parents[count])
                        else:
                            if self.runtime.local:
                                mapped_images.append('{}:latest'.format(from_image_metadata.config.name))
                            else:
                                # Everything in the group is going to be built with the uuid tag, so we must
                                # assume that it will exist for our parent.
                                mapped_images.append("{}:{}".format(from_image_metadata.config.name, uuid_tag))

                    # Is this image FROM another literal image name:tag?
                    elif image.image is not Missing:
                        mapped_images.append(image.image)

                    elif image.stream is not Missing:
                        stream = self.runtime.resolve_stream(image.stream)
                        # TODO: implement expriring images?
                        mapped_images.append(stream.image)

                    else:
                        raise IOError("Image in 'from' for [%s] is missing its definition." % base)

                    count += 1

                dfp.parent_images = mapped_images

            if self.source_sha is not None:
                # in a rebase, add ENV vars to each stage to relay source repo metadata
                env_vars.update(dict(
                    SOURCE_GIT_COMMIT=self.source_full_sha,
                    SOURCE_GIT_TAG=self.source_latest_tag,
                    SOURCE_GIT_URL=self.source_url,
                    SOURCE_DATE_EPOCH=self.source_date_epoch,
                ))

            # Set image name in case it has changed
            dfp.labels["name"] = self.config.name

            # Set version if it has been specified.
            if version is not None:
                dfp.labels["version"] = version
                env_vars["BUILD_VERSION"] = version

            # If the release is specified as "+", this means the user wants to bump the release.
            if release == "+":

                # increment the release that was in the Dockerfile
                release = prev_release

                if release:
                    self.logger.info("Bumping release field in Dockerfile")

                    # If release has multiple fields (e.g. 0.173.0.0), increment final field
                    if "." in release:
                        components = release.rsplit(".", 1)  # ["0.173","0"]
                        bumped_field = int(components[1]) + 1
                        release = "%s.%d" % (components[0], bumped_field)
                    else:
                        # If release is specified and a single field, just increment it
                        release = "%d" % (int(release) + 1)
                else:
                    # When 'release' is not specified in the Dockerfile, OSBS will automatically
                    # find a valid value for each build. This means OSBS is effectively auto-bumping.
                    # This is better than us doing it, so let it.
                    self.logger.info("No release label found in Dockerfile; bumping unnecessary -- osbs will automatically select unique release value at build time")

            # If a release is specified, set it. If it is not specified, remove the field.
            # If osbs finds the field, unset, it will choose a value automatically. This is
            # generally ideal for refresh-images where the only goal is to not collide with
            # a pre-existing image version-release.
            if release is not None:
                dfp.labels["release"] = release
                env_vars["BUILD_RELEASE"] = release
            else:
                if "release" in dfp.labels:
                    self.logger.info("Removing release field from Dockerfile")
                    del dfp.labels['release']

            # Delete differently cased labels that we override or use newer versions of
            for deprecated in ["Release", "Architecture", "BZComponent"]:
                if deprecated in dfp.labels:
                    del dfp.labels[deprecated]

            # specify new env vars in the dockerfile
            if env_vars:
                env_line = "ENV " + " ".join([key + "=" + val for key, val in env_vars.items()])
                dfp.add_lines(env_line, all_stages=True, at_start=True)

            # Remove any programmatic oit comments from previous management
            df_lines = dfp.content.splitlines(False)
            df_lines = [line for line in df_lines if not line.strip().startswith(OIT_COMMENT_PREFIX)]

            filtered_content = []
            in_mod_block = False
            for line in df_lines:

                # Check for begin/end of mod block, skip any lines inside
                if OIT_BEGIN in line:
                    in_mod_block = True
                    continue
                elif OIT_END in line:
                    in_mod_block = False
                    continue

                # if in mod, skip all
                if in_mod_block:
                    continue

                # remove any old instances of empty.repo mods that aren't in mod block
                if 'empty.repo' not in line:
                    if line.endswith('\n'):
                        line = line[0:-1]  # remove trailing newline, if exists
                    filtered_content.append(line)

            df_lines = filtered_content

            df_content = "\n".join(df_lines)

            with io.open('Dockerfile', 'w', encoding="utf-8") as df:
                for comment in oit_comments:
                    df.write("%s %s\n" % (OIT_COMMENT_PREFIX, comment))
                df.write(df_content)

            # remove old labels from dist-git
            for label in self.source_labels['old']:
                if label in dfp.labels:
                    del dfp.labels[label]

            # set with new source if known, otherwise leave alone for a refresh
            srclab = self.source_labels['now']
            if self.source_full_sha:
                dfp.labels[srclab['sha']] = self.source_full_sha
                if self.source_url:
                    dfp.labels[srclab['source']] = self.source_url
                    dfp.labels[srclab['source_commit']] = '{}/commit/{}'.format(self.source_url, self.source_full_sha)

            self._reflow_labels()

            self._update_csv(version, release)

            return (version, release)

    def _get_csv_file_and_refs(self, csv_config):
        gvars = self.runtime.group_config.vars
        ver = '{}.{}'.format(gvars['MAJOR'], gvars['MINOR'])
        manifests = os.path.join(self.distgit_dir, csv_config['manifests-dir'], ver)

        refs = os.path.join(manifests, 'image-references')
        if not os.path.isfile(refs):
            raise DoozerFatalError('{}: file does not exist: {}'.format(self.metadata.distgit_key, refs))
        with io.open(refs, 'r', encoding="utf-8") as f_ref:
            ref_data = yaml.full_load(f_ref)
        image_refs = ref_data.get('spec', {}).get('tags', {})
        if not image_refs:
            raise DoozerFatalError('Data in {} not valid'.format(refs))

        with Dir(manifests):
            csvs = glob.glob('*.clusterserviceversion.yaml')
            if len(csvs) < 1:
                raise DoozerFatalError('{}: did not find a *.clusterserviceversion.yaml file @ {}'.format(self.metadata.distgit_key, manifests))
            elif len(csvs) > 1:
                raise DoozerFatalError('{}: Must be exactly one *.clusterserviceversion.yaml file but found more than one @ {}'.format(self.metadata.distgit_key, manifests))
            return os.path.join(manifests, csvs[0]), image_refs

    def _update_csv(self, version, release):
        # AMH - most of this method really shouldn't be in Doozer itself
        # But right now there's no better way to handle it
        csv_config = self.metadata.config.get('update-csv', None)
        if not csv_config:
            return

        csv_file, image_refs = self._get_csv_file_and_refs(csv_config)
        registry = csv_config['registry'].rstrip("/")

        for ref in image_refs:
            try:
                name = build_image_ref_name(ref['name'])
                spec = ref['from']['name']
            except:
                raise DoozerFatalError('Error loading image-references data for {}'.format(self.metadata.distgit_key))

            try:
                if name == self.metadata.image_name_short:  # ref is current image
                    nvr = '{}:{}-{}'.format(name, version, release)
                else:
                    distgit = self.runtime.image_distgit_by_name(name)
                    # if upstream is referring to an image we don't actually build, give up.
                    if not distgit:
                        raise DoozerFatalError('Unable to find {} in image-references data for {}'.format(name, self.metadata.distgit_key))
                    meta = self.runtime.image_map.get(distgit, None)
                    if meta:  # image is currently be processed
                        nvr = '{}:{}-{}'.format(meta.image_name_short, version, release)
                    else:
                        meta = self.runtime.late_resolve_image(distgit)
                        _, v, r = meta.get_latest_build_info()
                        nvr = '{}:{}-{}'.format(meta.image_name_short, v, r)

                namespace = self.runtime.group_config.get('csv_namespace', None)
                if not namespace:
                    raise DoozerFatalError('csv_namespace is required in group.yaml when any image defines update-csv')
                replace = '{}/{}/{}'.format(registry, namespace, nvr)

                with io.open(csv_file, 'r+', encoding="utf-8") as f:
                    content = f.read()
                    content = content.replace(spec + '\n', replace + '\n')
                    content = content.replace(spec + '\"', replace + '\"')
                    f.seek(0)
                    f.truncate()
                    f.write(content)
            except Exception as e:
                self.runtime.logger.error(e)
                raise

        if version.startswith('v'):
            version = version[1:]  # strip off leading v

        x, y, z = version.split('.')[0:3]

        replace_args = {
            'MAJOR': x,
            'MINOR': y,
            'SUBMINOR': z,
            'RELEASE': release,
            'FULL_VER': '{}-{}'.format(version, release)
        }

        manifests_base = os.path.join(self.distgit_dir, csv_config['manifests-dir'])

        art_yaml = os.path.join(manifests_base, 'art.yaml')

        if os.path.isfile(art_yaml):
            with io.open(art_yaml, 'r', encoding="utf-8") as art_file:
                art_yaml_str = art_file.read()

            try:
                art_yaml_str = art_yaml_str.format(**replace_args)
                art_yaml_data = yaml.full_load(art_yaml_str)
            except Exception as ex:  # exception is low level, need to pull out the details and rethrow
                raise DoozerFatalError('Error processing art.yaml!\n{}\n\n{}'.format(str(ex), art_yaml_str))

            updates = art_yaml_data.get('updates', [])
            if not isinstance(updates, list):
                raise DoozerFatalError('`updates` key must be a list in art.yaml!')

            for u in updates:
                f = u.get('file', None)
                u_list = u.get('update_list', [])
                if not f:
                    raise DoozerFatalError('No file to update specified in art.yaml')
                if not u_list:
                    raise DoozerFatalError('update_list empty for {} in art.yaml'.format(f))

                f_path = os.path.join(manifests_base, f)
                if not os.path.isfile(f_path):
                    raise DoozerFatalError('{} does not exist as defined in art.yaml'.format(f_path))

                self.runtime.logger.info('Updating {}'.format(f_path))
                with io.open(f_path, 'r+', encoding="utf-8") as sr_file:
                    sr_file_str = sr_file.read()
                    for sr in u_list:
                        s = sr.get('search', None)
                        r = sr.get('replace', None)
                        if not s or not r:
                            raise DoozerFatalError('Must provide `search` and `replace` fields in art.yaml `update_list`')

                        sr_file_str = sr_file_str.replace(s, r)
                    sr_file.seek(0)
                    sr_file.truncate()
                    sr_file.write(sr_file_str)

    def _reflow_labels(self, filename="Dockerfile"):
        """
        The Dockerfile parser we are presently using writes all labels on a single line
        and occasionally make multiple LABEL statements. Calling this method with a
        Dockerfile in the current working directory will rewrite the file with
        labels at the end in a single statement.
        """

        dfp = DockerfileParser(path=filename)
        labels = dict(dfp.labels)  # Make a copy of the labels we need to add back

        # Delete any labels from the modeled content
        for key in dfp.labels:
            del dfp.labels[key]

        # Capture content without labels
        df_content = dfp.content.strip()

        # Write the file back out and append the labels to the end
        with io.open(filename, 'w', encoding="utf-8") as df:
            df.write("%s\n\n" % df_content)
            if labels:
                df.write("LABEL")
                for k, v in labels.items():
                    df.write(" \\\n")  # All but the last line should have line extension backslash "\"
                    escaped_v = v.replace('"', '\\"')  # Escape any " with \"
                    df.write("        %s=\"%s\"" % (k, escaped_v))
                df.write("\n\n")

    def _merge_source(self):
        """
        Pulls source defined in content.source and overwrites most things in the distgit
        clone with content from that source.
        """

        with Dir(self.source_path()):
            # gather source repo short sha for audit trail
            rc, out, _ = exectools.cmd_gather(["git", "rev-parse", "--short", "HEAD"])
            self.source_sha = out.strip()
            rc, out, _ = exectools.cmd_gather(["git", "rev-parse", "HEAD"])
            self.source_full_sha = out.strip()
            rc, out, _ = exectools.cmd_gather("git log -1 --format=%ct")
            self.source_date_epoch = out.strip()
            rc, out, _ = exectools.cmd_gather("git describe --always --tags HEAD")
            self.source_latest_tag = out.strip()

            rc, out, _ = exectools.cmd_gather(["git", "remote", "get-url", "origin"])
            out = out.strip()
            self.source_url = convert_source_url_to_https(out)

        # See if the config is telling us a file other than "Dockerfile" defines the
        # distgit image content.
        if self.config.content.source.dockerfile is not Missing:
            dockerfile_name = self.config.content.source.dockerfile
        else:
            dockerfile_name = "Dockerfile"

        # The path to the source Dockerfile we are reconciling against
        source_dockerfile_path = os.path.join(self.source_path(), dockerfile_name)

        # Clean up any files not special to the distgit repo
        ignore_list = BASE_IGNORE
        ignore_list.extend(self.runtime.group_config.get('dist_git_ignore', []))
        ignore_list.extend(self.config.get('dist_git_ignore', []))

        for ent in os.listdir("."):

            if ent in ignore_list:
                continue

            # Otherwise, clean up the entry
            if os.path.isfile(ent) or os.path.islink(ent):
                os.remove(ent)
            else:
                shutil.rmtree(ent)

        # Copy all files and overwrite where necessary
        recursive_overwrite(self.source_path(), self.distgit_dir)

        if dockerfile_name != "Dockerfile":
            # Does a non-distgit Dockerfile already exist from copying source; remove if so
            if os.path.isfile("Dockerfile"):
                os.remove("Dockerfile")

            try:
                # Rename our distgit source Dockerfile appropriately
                if not os.path.isfile(dockerfile_name):
                    options = glob.glob('Dockerfile*')
                    if options:
                        options = '\nTry one of these{}\n'.format(options)
                    else:
                        options = ''

                    url = self.source_url
                    if self.config.content.source.path is not Missing:
                        rc, out, err = exectools.cmd_gather(["git", "rev-parse", "--abbrev-ref", "HEAD"])
                        branch = out.strip()
                        url = '{}/tree/{}/{}'.format(url, branch, self.config.content.source.path)
                    raise DoozerFatalError('{}:{} does not exist in @ {}{}'
                                           .format(self.metadata.distgit_key, dockerfile_name, url, options))
                os.rename(dockerfile_name, "Dockerfile")
            except OSError as err:
                raise DoozerFatalError(str(err))

        # Clean up any extraneous Dockerfile.* that might be distractions (e.g. Dockerfile.centos)
        for ent in os.listdir("."):
            if ent.startswith("Dockerfile."):
                os.remove(ent)

        # Delete .gitignore since it may block full sync and is not needed here
        try:
            os.remove("./.gitignore")
        except OSError:
            pass

        notify_owner = False

        # In a previous implementation, we tracked a single file in .oit/Dockerfile.source.last
        # which provided a reference for the last time a Dockerfile was reconciled. If
        # we reconciled a file that did not match the Dockerfile.source.last, we would send
        # an email the Dockerfile owner that a fundamentally new reconciliation had taken place.
        # There was a problem with this approach:
        # During a sprint, we might have multiple build streams running side-by-side.
        # e.g. builds from a master branch and builds from a stage branch. If the
        # Dockerfile in these two branches happened to differ, we would notify the
        # owner as we toggled back and forth between the two versions for the alternating
        # builds. Instead, we now keep around an history of all past reconciled files.

        source_dockerfile_hash = hashlib.sha256(io.open(source_dockerfile_path, 'rb').read()).hexdigest()

        if not os.path.isdir(".oit/reconciled"):
            os.mkdir(".oit/reconciled")

        dockerfile_already_reconciled_path = '.oit/reconciled/{}.Dockerfile'.format(source_dockerfile_hash)

        # If the file does not exist, the source file has not been reconciled before.
        if not os.path.isfile(dockerfile_already_reconciled_path):
            # Something has changed about the file in source control
            notify_owner = True
            # Record that we've reconciled against this source file so that we do not notify the owner again.
            shutil.copy(source_dockerfile_path, dockerfile_already_reconciled_path)

        # Leave a record for external processes that owners will need to be notified.

        if not notify_owner:
            return

        with Dir(self.source_path()):
            author_email = None
            err = None
            rc, sha, err = exectools.cmd_gather(
                # --no-merges because the merge bot is not the real author
                # --diff-filter=a to omit the "first" commit in a shallow clone which may not be the author
                #   (though this means when the only commit is the initial add, that is omitted)
                'git log --no-merges --diff-filter=a -n 1 --pretty=format:%H {}'.format(dockerfile_name)
            )
            if rc == 0:
                rc, ae, err = exectools.cmd_gather('git show -s --pretty=format:%ae {}'.format(sha))
                if rc == 0:
                    if ae.lower().endswith('@redhat.com'):
                        self.logger.info('Last Dockerfile committer: {}'.format(ae))
                        author_email = ae
                    else:
                        err = 'Last committer email found, but is not @redhat.com address: {}'.format(ae)
            if err:
                self.logger.info('Unable to get author email for last {} commit: {}'.format(dockerfile_name, err))

        owners = []
        if self.config.owners is not Missing and isinstance(self.config.owners, list):
            owners = list(self.config.owners)
        if author_email:
            owners.append(author_email)
        sub_path = self.config.content.source.path
        if not sub_path:
            source_dockerfile_subpath = dockerfile_name
        else:
            source_dockerfile_subpath = "{}/{}".format(sub_path, dockerfile_name)
        # there ought to be a better way to determine the source alias that was registered:
        source_root = self.runtime.resolve_source(self.name, self.metadata)
        source_alias = self.config.content.source.get('alias', os.path.basename(source_root))

        self.runtime.add_record("dockerfile_notify",
                                distgit=self.metadata.qualified_name,
                                image=self.config.name,
                                owners=','.join(owners),
                                source_alias=source_alias,
                                source_dockerfile_subpath=source_dockerfile_subpath,
                                dockerfile=os.path.abspath("Dockerfile"))

    def _run_modifications(self):
        """
        Interprets and applies content.source.modify steps in the image metadata.
        """

        with io.open("Dockerfile", 'r', encoding="utf-8") as df:
            dockerfile_data = df.read()

        self.logger.debug(
            "About to start modifying Dockerfile [%s]:\n%s\n" %
            (self.metadata.distgit_key, dockerfile_data))

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
                    "component_name": self.metadata.distgit_key,
                    "kind": "Dockerfile",
                    "content": dockerfile_data,
                    "set_env": {"PATH": path},
                }
                modifier.act(context=context, ceiling_dir=os.getcwd())
                dockerfile_data = context["content"]
            else:
                raise IOError("Don't know how to perform modification action: %s" % modification.action)

        with io.open('Dockerfile', 'w', encoding="utf-8") as df:
            df.write(dockerfile_data)

    def rebase_dir(self, version, release):

        with Dir(self.distgit_dir):

            prev_release = None
            if os.path.isfile("Dockerfile"):
                dfp = DockerfileParser("Dockerfile")
                # extract previous release to enable incrementing it
                prev_release = dfp.labels.get("release")

                if version is None and not self.runtime.local:
                    # Extract the previous version and use that
                    version = dfp.labels["version"]

            # Make our metadata directory if it does not exist
            if not os.path.isdir(".oit"):
                os.mkdir(".oit")

            # If content.source is defined, pull in content from local source directory
            if self.has_source():
                self._merge_source()

                # before mods, check if upstream source version should be used
                # this will override the version fetch above
                if self.metadata.config.get('use_source_version', False):
                    dfp = DockerfileParser("Dockerfile")
                    version = dfp.labels["version"]

            # Source or not, we should find a Dockerfile in the root at this point or something is wrong
            assertion.isfile("Dockerfile", "Unable to find Dockerfile in distgit root")

            if self.config.content.source.modifications is not Missing:
                self._run_modifications()

        (real_version, real_release) = self.update_distgit_dir(version, release, prev_release)

        return (real_version, real_release)

    def _matches_commit(self, commit_hash, builds):
        """
        Given a source repo git hash, checks to see if the most recent Dockerfile
        claims that hash as its source via the label.
        If no hash given, or it matches, check whether it has been built or distgit is stale.
        :param commit_hash: if available, the commit to look for in the Dockerfile
        :param builds: dict of latest builds N => (V, R)
        Returns: bool
        """
        dfp = DockerfileParser()
        dfp.cache_content = True  # set after init, or it tries to preload content
        try:
            content = self.metadata.fetch_cgit_file("Dockerfile")
            dfp.cached_content = content.decode('utf-8') if content else None
        except Exception as e:
            self.logger.error("failed to retrieve valid Dockerfile to compare: {}".format(e))
            return False

        df_hash = dfp.labels.get(self.source_labels['now']['sha'])
        if commit_hash:
            self.logger.debug(
                'Comparing distgit Dockerfile label "{}" to source hash "{}"'
                .format(df_hash, commit_hash)
            )
            if commit_hash != df_hash:
                self.logger.debug("No match; the source is different from dist-git")
                return False

        # at this point, the distgit matches (or is) the source commit. check if it has been built.
        return self._built_or_recent(
            dfp.labels.get("version", ""),
            dfp.labels.get("release", ""),
            builds
        )

    def _built_or_recent(self, dg_version, dg_release, builds):
        component = self.metadata.get_component_name()
        if component in builds:
            b_version, b_release = builds[component]
            self.logger.debug(
                'Looking for VR "{}-{}" in distgit dockerfile to match build VR "{}-{}"'
                .format(dg_version, dg_release, b_version, b_release)
            )
            if b_version == dg_version and b_release == dg_release:
                self.logger.debug("Latest source has been built; no need to rebuild")
                return True
        self.logger.debug('No build matches distgit NVR "{}-{}-{}"'.format(component, dg_version, dg_release))

        # it hasn't been built; but has it been long enough since the distgit commit was made?
        # we want neither to spam owners about still-broken builds nor to ignore them forever.
        return self.release_is_recent(dg_release)


class RPMDistGitRepo(DistGitRepo):
    def __init__(self, metadata, autoclone=True):
        super(RPMDistGitRepo, self).__init__(metadata, autoclone)
        self.source = self.config.content.source
        if self.source.specfile is Missing:
            raise ValueError('Must specify spec file name for RPMs.')

    def _find_in_spec(self, spec, regex, desc):
        match = re.search(regex, spec)
        if match:
            return match.group(1)
        self.logger.error("No {} found in specfile '{}'".format(desc, self.source.specfile))
        return ""

    def _matches_commit(self, commit_hash, builds):
        """
        Given a source repo git hash, checks to see if the most recent distgit specfile
        includes that hash in its %global commit or Release: field
        and that it has been built or attempted recently.
        Returns: bool
        """
        # download the latest spec file from dist-git
        specfile = self.source.specfile
        try:
            spec = self.metadata.fetch_cgit_file(specfile).decode('utf-8')
        except Exception as e:
            self.logger.error("failed to retrieve valid specfile '{}' to compare: {}".format(specfile, e))
            return False  # assume it's not a match

        # look for the %global commit or the Release to match the commit_hash
        # for most RPM sources the commit_hash will match the %global commit
        global_commit_hash = self._find_in_spec(spec, r'(?x) %global \s+ commit \s+ (\w+)', "%global commit")

        # but also check if it's in the Release, because that may have a tito commit
        # pushed to the source repo with the global_commit_hash being the previous commit.
        dg_release = self._find_in_spec(spec, r'(?mix) ^ \s* Release: \s+ (\S+)', "Release: field")

        self.logger.debug(
            'Looking for source hash "{}" in distgit specfile "{}" as global commit "{}" or Release "{}"'
            .format(commit_hash, specfile, global_commit_hash, dg_release)
        )
        if commit_hash != global_commit_hash and commit_hash[:7] not in dg_release:
            return False  # source commit is not in latest spec file

        # at this point, the distgit matches the source commit. check if it has been built.
        dg_version = self._find_in_spec(spec, r'(?mix) ^ \s* Version: \s+ (\S+)', "Version: field")
        return self._built_or_recent(dg_version, dg_release, builds)

    def _built_or_recent(self, dg_version, dg_release, builds):
        dg_release = dg_release.replace("%{?dist}", "")  # remove macro for dist, if there
        if self.name in builds:
            b_version, b_release = builds[self.name]
            b_release = re.sub(r'\.\w+$', "", b_release)
            self.logger.debug(
                'Looking for VR "{}-{}" in distgit specfile {} to match build VR "{}-{}"'
                .format(dg_version, dg_release, self.source.specfile, b_version, b_release)
            )
            if b_version == dg_version and b_release == dg_release:
                self.logger.debug("Latest source has been built; no need to rebuild")
                return True
        self.logger.debug('No build matches distgit NVR "{}-{}-{}"'.format(self.name, dg_version, dg_release))

        # it hasn't been built; but has it been long enough since the distgit commit was made?
        # we want neither to spam owners about still-broken builds nor to ignore them forever.
        return self.release_is_recent(dg_release)
