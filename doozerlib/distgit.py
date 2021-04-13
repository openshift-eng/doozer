from __future__ import absolute_import, print_function, unicode_literals

import asyncio
import copy
import errno
import glob
import hashlib
import io
import os
import pathlib
import re
import shutil
import time
import traceback
from datetime import date
from multiprocessing import Event, Lock
from typing import Any, Dict, Tuple

import aiofiles
import bashlex
import yaml
from dockerfile_parse import DockerfileParser

from doozerlib import constants, state, util
from doozerlib.dblib import Record
from doozerlib.exceptions import DoozerFatalError
from doozerlib.source_modifications import SourceModifierFactory
from doozerlib.util import convert_remote_git_to_https, yellow_print

from . import assertion, exectools, logutil
from .brew import get_build_objects, watch_task
from .model import ListModel, Missing, Model
from .pushd import Dir

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


def build_image_ref_name(name):
    return 'openshift/ose-' + re.sub(pattern='^ose-', repl='', string=name)


def map_image_name(name, image_map):
    for match, replacement in image_map.items():
        if name.find(match) != -1:
            return name.replace(match, replacement)
    return name


class DistGitRepo(object):
    def __init__(self, metadata, autoclone=True):
        self.metadata = metadata
        self.config = metadata.config
        self.runtime = metadata.runtime
        self.name = self.metadata.name
        self.distgit_dir = None
        self.dg_path = None
        self.build_status = False
        self.push_status = False

        self.branch = self.runtime.branch

        self.source_sha = None
        self.source_full_sha = None
        self.source_latest_tag = None
        self.source_date_epoch = None
        self.actual_source_url = None
        self.public_facing_source_url = None
        # This will be set to True if the source contains embargoed (private) CVE fixes. Defaulting to None which means the value should be determined while rebasing.
        self.private_fix = None

        # If we are rebasing, this map can be populated with
        # variables acquired from the source path.
        self.env_vars_from_source = None

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
            self.dg_path = pathlib.Path(self.distgit_dir)

            fake_distgit = (self.runtime.local and 'content' in self.metadata.config)

            if os.path.isdir(self.distgit_dir):
                self.logger.info("Distgit directory already exists; skipping clone: %s" % self.distgit_dir)
                if self.runtime.upcycle:
                    self.logger.info("Refreshing source for '{}' due to --upcycle".format(self.distgit_dir))
                    with Dir(self.distgit_dir):
                        exectools.cmd_assert('git fetch --all', retries=3)
                        exectools.cmd_assert('git reset --hard @{upstream}', retries=3)
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

                    self.logger.info("Cloning distgit repository [branch:%s] into: %s" % (distgit_branch, self.distgit_dir))

                    # Has the user specified a specific commit to checkout from distgit on the command line?
                    distgit_commitish = self.runtime.downstream_commitish_overrides.get(self.metadata.distgit_key, None)

                    timeout = str(self.runtime.global_opts['rhpkg_clone_timeout'])
                    rhpkg_clone_depth = int(self.runtime.global_opts.get('rhpkg_clone_depth', '0'))

                    if self.metadata.namespace == 'containers' and self.runtime.cache_dir:
                        # Containers don't generally require distgit lookaside. We can rely on normal
                        # git clone & leverage git caches to greatly accelerate things if the user supplied it.
                        gitargs = ['--branch', distgit_branch]

                        if not distgit_commitish:
                            gitargs.append('--single-branch')

                        if not distgit_commitish and rhpkg_clone_depth > 0:
                            gitargs.extend(["--depth", str(rhpkg_clone_depth)])

                        self.runtime.git_clone(self.metadata.distgit_remote_url(), self.distgit_dir, gitargs=gitargs,
                                               set_env=constants.GIT_NO_PROMPTS, timeout=timeout)
                    else:
                        # Use rhpkg -- presently no idea how to cache.
                        cmd_list = ["timeout", timeout]
                        cmd_list.append("rhpkg")

                        if self.runtime.rhpkg_config_lst:
                            cmd_list.extend(self.runtime.rhpkg_config_lst)

                        if self.runtime.user is not None:
                            cmd_list.append("--user=%s" % self.runtime.user)

                        cmd_list.extend(["clone", self.metadata.qualified_name, self.distgit_dir])
                        cmd_list.extend(["--branch", distgit_branch])

                        if not distgit_commitish and rhpkg_clone_depth > 0:
                            cmd_list.extend(["--depth", str(rhpkg_clone_depth)])

                        # Clone the distgit repository. Occasional flakes in clone, so use retry.
                        exectools.cmd_assert(cmd_list, retries=3, set_env=constants.GIT_NO_PROMPTS)

                    if distgit_commitish:
                        with Dir(self.distgit_dir):
                            exectools.cmd_assert(f'git checkout {distgit_commitish}')

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
        :return: Returns the directory containing the source which should be used to populate distgit. This includes
                the source.path subdirectory if the metadata includes one.
        """

        source_root = self.runtime.resolve_source(self.metadata)
        sub_path = self.config.content.source.path

        path = source_root
        if sub_path is not Missing:
            path = os.path.join(source_root, sub_path)

        assertion.isdir(path, "Unable to find path for source [%s] for config: %s" % (path, self.metadata.config_filename))
        return path

    def source_repo_path(self):
        """
        :return: Returns the directory containing the root of the cloned source repo.
        """
        path = self.runtime.resolve_source(self.metadata)
        assertion.isdir(path, "Unable to find path for source [%s] for config: %s" % (path, self.metadata.config_filename))
        return path

    def _get_diff(self):
        return None  # to actually record a diff, child classes must override this function

    def commit(self, commit_message, log_diff=False):
        if self.runtime.local:
            return ''  # no commits if local

        with Dir(self.distgit_dir):
            self.logger.info("Adding commit to local repo: {}".format(commit_message))
            if log_diff:
                diff = self._get_diff()
                if diff and diff.strip():
                    self.runtime.add_distgits_diff(self.metadata.distgit_key, diff)
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

    def push(self):
        with Dir(self.distgit_dir):
            self.logger.info("Pushing distgit repository %s", self.name)
            try:
                # When initializing new release branches, a large amount of data needs to
                # be pushed. If every distgit within a release is being pushed at the same
                # time, a single push invocation can take hours to complete -- making the
                # timeout value counterproductive. Limit to 5 simultaneous pushes.
                with self.runtime.get_named_semaphore('rhpkg::push', count=5):
                    timeout = str(self.runtime.global_opts['rhpkg_push_timeout'])
                    exectools.cmd_assert("timeout {} rhpkg push".format(timeout), retries=3)
                    # rhpkg will create but not push tags :(
                    # Not asserting this exec since this is non-fatal if a tag already exists,
                    # and tags in dist-git can't be --force overwritten
                    exectools.cmd_gather(['timeout', '60', 'git', 'push', '--tags'])
            except IOError as e:
                return (self.metadata, repr(e))
            return (self.metadata, True)

    @exectools.limit_concurrency(limit=5)
    async def push_async(self):
        self.logger.info("Pushing distgit repository %s", self.name)
        # When initializing new release branches, an large amount of data needs to
        # be pushed. If every distgit within a release is being pushed at the same
        # time, a single push invocation can take hours to complete -- making the
        # timeout value counterproductive. Limit to 5 simultaneous pushes.
        timeout = str(self.runtime.global_opts['rhpkg_push_timeout'])
        await exectools.cmd_assert_async(["timeout", f"{timeout}", "git", "push", "--follow-tags"], cwd=self.distgit_dir, retries=3)

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
        self.rebase_event = Event()
        self.rebase_status = False
        self.logger = metadata.logger
        self.source_modifier_factory = source_modifier_factory

    def clone(self, distgits_root_dir, distgit_branch):
        super(ImageDistGitRepo, self).clone(distgits_root_dir, distgit_branch)
        self._read_master_data()

    def _get_diff(self):
        rc, out, _ = exectools.cmd_gather(["git", "-C", self.distgit_dir, "diff", "Dockerfile"])
        assertion.success(rc, 'Failed fetching distgit diff')
        return out

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
        with self.dg_path.joinpath('container.yaml').open('w', encoding="utf-8") as rc:
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
        arches = self.metadata.get_arches()

        # override image config with this dict
        config_overrides = {}
        if self.config.container_yaml is not Missing:
            config_overrides = copy.deepcopy(self.config.container_yaml.primitive())

        if self.config.content.source.pkg_managers is not Missing:
            # If a package manager is specified, then configure cachito.
            # https://mojo.redhat.com/docs/DOC-1177281#jive_content_id_Cachito_Integration
            config_overrides.update({
                'remote_source': {
                    'repo': convert_remote_git_to_https(self.actual_source_url),
                    'ref': self.source_full_sha,
                    'pkg_managers': self.config.content.source.pkg_managers.primitive(),
                }
            })

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
        with self.dg_path.joinpath('cvp-owners.yml').open('w', encoding="utf-8") as co:
            if self.config.owners:  # Not missing and non-empty
                # only spam owners on failure; ref. https://red.ht/2x0edYd
                owners = {owner: "FAILURE" for owner in self.config.owners}
                yaml.safe_dump(owners, co, default_flow_style=False)

    def _generate_repo_conf(self):
        """
        Generates a repo file in .oit/repo.conf
        """

        self.logger.debug("Generating repo file for Dockerfile {}".format(self.metadata.distgit_key))

        # Make our metadata directory if it does not exist
        util.mkdirs(self.dg_path.joinpath('.oit'))

        repos = self.runtime.repos
        enabled_repos = self.config.get('enabled_repos', [])
        non_shipping_repos = self.config.get('non_shipping_repos', [])

        for t in repos.repotypes:
            with self.dg_path.joinpath('.oit', f'{t}.repo').open('w', encoding="utf-8") as rc:
                content = repos.repo_file(t, enabled_repos=enabled_repos)
                rc.write(content)

        with self.dg_path.joinpath('content_sets.yml').open('w', encoding="utf-8") as rc:
            rc.write(repos.content_sets(enabled_repos=enabled_repos, non_shipping_repos=non_shipping_repos))

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
                   push_late=False, dry_run=False, registry_config_dir=None, filter_by_os=None):
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

        # get registry_config_json file must before with Dir(self.distgit_dir)
        # so that relative path or env like DOCKER_CONFIG will not pointed to distgit dir.
        registry_config_file = ''
        if registry_config_dir is not None:
            registry_config_file = util.get_docker_config_json(registry_config_dir)

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
            brew_image_url = self.runtime.resolve_brew_image_url(image_name_and_version)

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
                    mirror_cmd = 'oc image mirror '
                    if filter_by_os is not None:
                        mirror_cmd += "--filter-by-os={}".format(filter_by_os)
                    mirror_cmd += " {} {} --filename={}".format(dr, insecure, push_config)
                    if registry_config_file != '':
                        mirror_cmd += f" --registry-config={registry_config_file}"

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

    def wait_for_rebase(self, image_name, terminate_event):
        """ Wait for image_name to be rebased. """
        image = self.runtime.resolve_image(image_name, False)
        if image is None:
            self.logger.info("Skipping image build since it is not included: %s" % image_name)
            return
        dgr = image.distgit_repo()
        dgr.rebase_event.wait()
        if not dgr.rebase_status:  # failed to rebase
            raise IOError(f"Error building image: {self.metadata.qualified_name} ({image_name} was waiting)")
        if terminate_event.is_set():
            raise KeyboardInterrupt()
        pass

    def build_container(
            self, profile, push_to_defaults, additional_registries, terminate_event,
            scratch=False, retries=3, realtime=False, dry_run=False, registry_config_dir=None, filter_by_os=None):
        """
        This method is designed to be thread-safe. Multiple builds should take place in brew
        at the same time. After a build, images are pushed serially to all mirrors.
        DONT try to change cwd during this time, all threads active will change cwd
        :param profile: image build profile
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
                self.build_status = self._build_container_local(target_image, profile["repo_type"], realtime)
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

                if len(profile["targets"]) > 1:
                    # FIXME: Currently we don't really support building images against multiple targets,
                    # or we would overwrite the image tag when pushing to the registry.
                    # `targets` is defined as an array just because we want to keep consistency with RPM build.
                    raise DoozerFatalError("Building images against multiple targets is not currently supported.")
                target = '' if not profile["targets"] else profile["targets"][0]
                exectools.retry(
                    retries=retries, wait_f=wait,
                    task_f=lambda: self._build_container(
                        target_image, target, profile["signing_intent"], profile["repo_type"], profile["repo_list"], terminate_event,
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
                    self.push_image([], push_to_defaults, additional_registries, version_release_tuple=(push_version, push_release), registry_config_dir=registry_config_dir, filter_by_os=filter_by_os)
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

    def update_build_db(self, success_flag, task_id=None, scratch=False):

        if scratch:
            return

        with Dir(self.distgit_dir):
            commit_sha = exectools.cmd_assert('git rev-parse HEAD')[0].strip()[:8]
            invoke_ts = str(int(round(time.time() * 1000)))
            invoke_ts_iso = self.runtime.timestamp()

            with self.runtime.db.record('build', metadata=self.metadata):
                Record.set('build.time.unix', invoke_ts)
                Record.set('build.time.iso', invoke_ts_iso)
                Record.set('dg.commit', commit_sha)
                Record.set('brew.task_state', 'success' if success_flag else 'failure')
                Record.set('brew.task_id', task_id)

                dfp = DockerfileParser(str(Dir.getpath().joinpath('Dockerfile')))
                for label_name in ['version', 'release', 'name', 'com.redhat.component']:
                    Record.set(f'label.{label_name}', dfp.labels.get(label_name, ''))

                Record.set('label.version', dfp.labels.get('version', ''))
                Record.set('label.release', dfp.labels.get('release', ''))
                for k, v in dfp.labels.items():
                    if k.startswith('io.openshift.'):
                        Record.set(f'label.{k}', dfp.labels[k])

                for k, v in dfp.envs.items():
                    if k.startswith('KUBE_') or k.startswith('OS_'):
                        Record.set(f'env.{k}', dfp.envs.get(k, ''))

                Record.set('incomplete', False)
                if task_id is not None:
                    try:
                        with self.runtime.shared_koji_client_session() as kcs:
                            task_result = kcs.getTaskResult(task_id, raise_fault=False)
                            import pprint
                            pprint.pprint(task_result)
                            """
                            success example result:
                            { 'koji_builds': ['1182060'],    # build_id created by task
                              'repositories': [
                                 'registry-proxy.engineering.redhat.com/rh-osbs/openshift-ose-ose-metering-ansible-operator-metadata:latest',
                                 ...
                               ]
                            }
                            fail example result:
                            { 'faultCode': 1018,
                              'faultString': 'log entries',
                            }
                            """
                            Record.set('brew.faultCode', task_result.get('faultCode', 0))
                            build_ids = task_result.get('koji_builds', [])
                            Record.set('brew.build_ids', ','.join(build_ids))  # comma delimited list if > 1
                            image_shas = []
                            for idx, build_id in enumerate(build_ids):
                                build_info = Model(kcs.getBuild(int(
                                    build_id)))  # Example: https://gist.github.com/jupierce/fe05f8fe310fdf8aa8b5c5991cf21f05

                                main_sha = build_info.extra.typeinfo.image.index.digests  # Typically contains manifest list sha
                                if main_sha:
                                    image_shas.extend(main_sha.values())

                                for build_datum in ['id', 'source', 'version', 'nvr', 'name', 'release',
                                                    'package_id']:
                                    Record.set(f'build.{idx}.{build_datum}', build_info.get(build_datum, ''))

                                archives = ListModel(kcs.listArchives(int(build_id)))  # https://gist.github.com/jupierce/6f27ebf35e88ed5a9a2c8e66fdcd34b4
                                for archive in archives:
                                    archive_shas = archive.extra.docker.digests
                                    if archive_shas:
                                        image_shas.extend(archive_shas.values())

                            Record.set('brew.image_shas', ','.join(image_shas))

                    except:
                        Record.set('incomplete', True)
                        traceback.print_exc()
                        self.logger.error(f'Unable to extract brew task information for {task_id}')

    def _build_container(self, target_image, target, signing_intent, repo_type, repo_list, terminate_event,
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

        if target:
            cmd_list.append("--target")
            cmd_list.append(target)

        # Determine if ODCS is enabled by looking at container.yaml.
        odcs_enabled = False
        osbs_image_config_path = os.path.join(self.distgit_dir, "container.yaml")
        if os.path.isfile(osbs_image_config_path):
            with open(osbs_image_config_path, "r") as f:
                image_config = yaml.safe_load(f)
            odcs_enabled = "compose" in image_config

        if odcs_enabled:
            self.logger.info("About to build image in ODCS mode with signing intent {}.".format(signing_intent))
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

        try:

            # Run the build with --nowait so that we can immediately get information about the brew task
            rc, out, err = exectools.cmd_gather(cmd_list)

            if rc != 0:
                # Probably no point in continuing.. can't contact brew?
                self.logger.info("Unable to create brew task: out={}  ; err={}".format(out, err))
                self.update_build_db(False, scratch=scratch)
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
            error = watch_task(self.runtime.build_retrying_koji_client(), self.logger.info, task_id, terminate_event)

            # Looking for something like the following to conclude the image has already been built:
            # BuildError: Build for openshift-enterprise-base-v3.7.0-0.117.0.0 already exists, id 588961
            # Note it is possible that a Brew task fails with a build record left (https://issues.redhat.com/browse/ART-1723).
            # Didn't find a variable in the context to get the Brew NVR or ID. Extracting the build ID from the error message.
            # Hope the error message format will not change.
            match = None
            if error:
                match = re.search(r"already exists, id (\d+)", error)
            if match:
                with self.runtime.shared_koji_client_session() as kcs:
                    builds = get_build_objects([int(match[1])], kcs)
                if builds and builds[0] and builds[0].get('state') == 1:  # State 1 means complete.
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
                self.update_build_db(False, task_id=task_id, scratch=scratch)
                self.logger.info("Error building image: {}, {}".format(task_url, error))
                return False

            self.update_build_db(True, task_id=task_id, scratch=scratch)
            self.logger.info("Successfully built image: {} ; {}".format(target_image, task_url))
            return True
        finally:
            try:
                with Dir(self.distgit_dir):
                    exectools.cmd_assert(['git', 'push', '--tags', '--force'], retries=3)
            except:
                self.logger.error('Unable to push tags to distgit!')

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

        dg_path = self.dg_path
        with Dir(self.distgit_dir):
            # Source or not, we should find a Dockerfile in the root at this point or something is wrong
            assertion.isfile(dg_path.joinpath("Dockerfile"), "Unable to find Dockerfile in distgit root")

            self._generate_repo_conf()

            self._generate_config_digest()

            self._write_osbs_image_config()

            self._write_cvp_owners()

            dfp = DockerfileParser(path=str(dg_path.joinpath('Dockerfile')))

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

            # Split the version number v4.3.4 => [ 'v4', '3, '4' ]
            vsplit = version.split(".")

            major_version = vsplit[0].lstrip('v')
            # Click validation should have ensured user specified semver, but double check because of version=None flow.
            minor_version = '0' if len(vsplit) < 2 else vsplit[1]
            patch_version = '0' if len(vsplit) < 3 else vsplit[2]

            if not self.runtime.local:
                with dg_path.joinpath('additional-tags').open('w', encoding="utf-8") as at:
                    at.write("%s\n" % uuid_tag)  # The uuid which we ensure we get the right
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
                dfp.labels["com.redhat.delivery.appregistry"] = "False"

            # For each bit of maintainer information we have, create a new label in the image
            maintainer = self.metadata.get_maintainer_info()
            for k, v in maintainer.items():
                dfp.labels[f'io.openshift.maintainer.{k}'] = v

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
                                if r.endswith(".p1"):  # latest parent is embargoed
                                    self.private_fix = True  # this image should also be embargoed
                                mapped_images.append("{}:{}-{}".format(base_meta.config.name, v, r))
                            # Otherwise, the user is not expecting the FROM field to be updated in this Dockerfile.
                            else:
                                mapped_images.append(original_parents[count])
                        else:
                            if self.runtime.local:
                                mapped_images.append('{}:latest'.format(from_image_metadata.config.name))
                            else:
                                from_image_distgit = from_image_metadata.distgit_repo()
                                if from_image_distgit.private_fix:  # if the parent we are going to build is embargoed
                                    self.private_fix = True  # this image should also be embargoed
                                # Everything in the group is going to be built with the uuid tag, so we must
                                # assume that it will exist for our parent.
                                mapped_images.append("{}:{}".format(from_image_metadata.config.name, uuid_tag))

                    # Is this image FROM another literal image name:tag?
                    elif image.image is not Missing:
                        mapped_images.append(image.image)

                    elif image.stream is not Missing:
                        stream = self.runtime.resolve_stream(image.stream)
                        # TODO: implement expiring images?
                        mapped_images.append(stream.image)

                    else:
                        raise IOError("Image in 'from' for [%s] is missing its definition." % base)

                    count += 1

                dfp.parent_images = mapped_images

            # Set image name in case it has changed
            dfp.labels["name"] = self.config.name

            # If no version was specified, pull it from the Dockerfile
            if version is None:
                version = dfp.labels['version']

            # If the release is specified as "+", this means the user wants to bump the release.
            if release == "+":
                # increment the release that was in the Dockerfile
                if prev_release:
                    self.logger.info("Bumping release field in Dockerfile")
                    if self.runtime.group_config.public_upstreams and (prev_release.endswith(".p0") or prev_release.endswith(".p1")):
                        prev_release = prev_release[:-3]  # strip .p0/1
                    # If release has multiple fields (e.g. 0.173.0.0), increment final field
                    if "." in prev_release:
                        components = prev_release.rsplit(".", 1)  # ["0.173","0"]
                        bumped_field = int(components[1]) + 1
                        release = "%s.%d" % (components[0], bumped_field)
                    else:
                        # If release is specified and a single field, just increment it
                        release = "%d" % (int(prev_release) + 1)
                    if self.runtime.group_config.public_upstreams:
                        release += ".p?"  # appended '.p?' field will be replaced with .p0 or .p1 later
                else:
                    # When 'release' is not specified in the Dockerfile, OSBS will automatically
                    # find a valid value for each build. This means OSBS is effectively auto-bumping.
                    # This is better than us doing it, so let it.
                    if self.runtime.group_config.public_upstreams:
                        raise ValueError("Failed to bump the release: Neither 'release' is specified in the Dockerfile nor we can use OSBS auto-bumping when a public upstream mapping is defined in ocp-build-data.")
                    self.logger.info("No release label found in Dockerfile; bumping unnecessary -- osbs will automatically select unique release value at build time")
                    release = None

            # If a release is specified, set it. If it is not specified, remove the field.
            # If osbs finds the field, unset, it will choose a value automatically. This is
            # generally ideal for refresh-images where the only goal is to not collide with
            # a pre-existing image version-release.
            if release is not None:
                pval = '.p0'
                if self.runtime.group_config.public_upstreams:
                    if not release.endswith(".p?"):
                        raise ValueError(
                            f"'release' must end with '.p?' for an image with a public upstream but its actual value is {release}")
                    if self.private_fix is None:
                        raise ValueError("self.private_fix must be set (or determined by _merge_source) before rebasing for an image with a public upstream")
                    pval = ".p1" if self.private_fix else ".p0"

                if release.endswith(".p?"):
                    release = release[:-3]  # strip .p?
                    release += pval

                dfp.labels['release'] = release
            else:
                if self.runtime.group_config.public_upstreams:
                    raise ValueError("We are not able to let OSBS choose a release value for an image with a public upstream.")
                if "release" in dfp.labels:
                    self.logger.info("Removing release field from Dockerfile")
                    del dfp.labels['release']

            # Delete differently cased labels that we override or use newer versions of
            for deprecated in ["Release", "Architecture", "BZComponent"]:
                if deprecated in dfp.labels:
                    del dfp.labels[deprecated]

            # remove old labels from dist-git
            for label in self.source_labels['old']:
                if label in dfp.labels:
                    del dfp.labels[label]

            # set with new source if known, otherwise leave alone for a refresh
            srclab = self.source_labels['now']
            if self.source_full_sha:
                dfp.labels[srclab['sha']] = self.source_full_sha
                if self.public_facing_source_url:
                    dfp.labels[srclab['source']] = self.public_facing_source_url
                    dfp.labels[srclab['source_commit']] = '{}/commit/{}'.format(self.public_facing_source_url, self.source_full_sha)

            dfp.labels['version'] = version

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

            if release:
                release_suffix = f'-{release}'
            else:
                release_suffix = ''

            # Environment variables that will be injected into the Dockerfile.
            env_vars = {  # Set A
                'OS_GIT_MAJOR': major_version,
                'OS_GIT_MINOR': minor_version,
                'OS_GIT_PATCH': patch_version,
                'OS_GIT_VERSION': f'{major_version}.{minor_version}.{patch_version}{release_suffix}',
                'OS_GIT_TREE_STATE': 'clean',
                'SOURCE_GIT_TREE_STATE': 'clean',
                'BUILD_VERSION': version,
                'BUILD_RELEASE': release if release else '',
            }

            with dg_path.joinpath('Dockerfile').open('w', encoding="utf-8") as df:
                for comment in oit_comments:
                    df.write("%s %s\n" % (OIT_COMMENT_PREFIX, comment))
                df.write(df_content)

            self._update_environment_variables(env_vars)

            self._reflow_labels()

            self._update_csv(version, release)

            return version, release

    def _generate_config_digest(self):
        # The config digest is used by scan-sources to detect config changes
        self.logger.info("Calculating config digest...")
        digest = self.metadata.calculate_config_digest(self.runtime.group_config, self.runtime.streams)
        with self.dg_path.joinpath(".oit", "config_digest").open('w') as f:
            f.write(digest)
        self.logger.info("Saved config digest %s to .oit/config_digest", digest)

    def _get_csv_file_and_refs(self, csv_config):
        gvars = self.runtime.group_config.vars
        subdir = csv_config.get('bundle-dir', f'{gvars["MAJOR"]}.{gvars["MINOR"]}')
        manifests = os.path.join(self.distgit_dir, csv_config['manifests-dir'], subdir)

        refs = os.path.join(manifests, 'image-references')
        if not os.path.isfile(refs):
            raise DoozerFatalError('{}: file does not exist: {}'.format(self.metadata.distgit_key, refs))
        with io.open(refs, 'r', encoding="utf-8") as f_ref:
            ref_data = yaml.full_load(f_ref)
        image_refs = ref_data.get('spec', {}).get('tags', {})
        if not image_refs:
            raise DoozerFatalError('Data in {} not valid'.format(refs))

        csvs = list(pathlib.Path(manifests).glob('*.clusterserviceversion.yaml'))
        if len(csvs) < 1:
            raise DoozerFatalError('{}: did not find a *.clusterserviceversion.yaml file @ {}'.format(self.metadata.distgit_key, manifests))
        elif len(csvs) > 1:
            raise DoozerFatalError('{}: Must be exactly one *.clusterserviceversion.yaml file but found more than one @ {}'.format(self.metadata.distgit_key, manifests))
        return str(csvs[0]), image_refs

    def _update_csv(self, version, release):
        # AMH - most of this method really shouldn't be in Doozer itself
        # But right now there's no better way to handle it
        csv_config = self.metadata.config.get('update-csv', None)
        if not csv_config:
            return

        csv_file, image_refs = self._get_csv_file_and_refs(csv_config)
        registry = csv_config['registry'].rstrip("/")
        image_map = csv_config.get('image-map', {})

        for ref in image_refs:
            try:
                name = build_image_ref_name(ref['name'])
                name = map_image_name(name, image_map)
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

    def _update_environment_variables(self, update_envs, filename='Dockerfile'):
        """
        There are three distinct sets of environment variables we need to consider
        in a Dockerfile:
        Set A) those which doozer calculates on every update
        Set B) those which doozer can calculate only when upstream source code is available
               (self.env_vars_from_source). If self.env_vars_from_source=None, no merge
               has occurred and we should not try to update envs from source we find in distgit.
        Set C) those which the Dockerfile author has set for their own purposes (these cannot
               override doozer's env, but they are free to use other variables names).

        :param update_envs: The update environment variables to set (Set A).
        :param filename: The Dockerfile name in the distgit dir to edit.
        :return: N/A
        """

        # set_build_variables must be explicitly set to False. If left unset, default to True. If False,
        # we do not inject environment variables into the Dockerfile. This is occasionally necessary
        # for images like the golang builders where these environment variables pollute the environment
        # for code trying to establish their OWN src commit hash, etc.
        if self.config.content.set_build_variables is not Missing and not self.config.content.set_build_variables:
            return

        dg_path = self.dg_path
        df_path = dg_path.joinpath(filename)

        # The DockerfileParser can find & set environment variables, but is written such that it only updates the
        # last instance of the env in the Dockerfile. For example, if, at different build stages, A=1 and later A=2
        # are set, DockerfileParser will only manage the A=2 instance. We want to remove variables that doozer is
        # going to set. To do so, we repeatedly load the Dockerfile and remove the env variable.
        # In this way, from our example, on the second load and removal, A=1 should be removed.

        # Build a dict of everything we want to set.
        all_envs = dict(update_envs)
        all_envs.update(self.env_vars_from_source or {})

        while True:
            dfp = DockerfileParser(str(df_path))
            # Find the intersection between envs we want to set and those present in parser
            envs_intersect = set(list(all_envs.keys())).intersection(set(list(dfp.envs.keys())))

            if not envs_intersect:  # We've removed everything we want to ultimately set
                break

            self.logger.debug(f'Removing old env values from Dockerfile: {envs_intersect}')

            for k in envs_intersect:
                del dfp.envs[k]

            dfp_content = dfp.content
            # Write the file back out
            with df_path.open('w', encoding="utf-8") as df:
                df.write(dfp_content)

        # The env vars we want to set have been removed from the target Dockerfile.
        # Now, we want to inject the values we have available. In a Dockerfile, ENV must
        # be set for each build stage. So the ENVs must be set after each FROM.

        env_update_line_flag = '__doozer=update'
        env_merge_line_flag = '__doozer=merge'

        def get_env_set_list(env_dict):
            """
            Returns a list of 'key1=value1 key2=value2'. Used mainly to ensure
            ENV lines we inject don't change because of key iteration order.
            """
            sets = ''
            for key in sorted(env_dict.keys()):
                sets += f'{key}={env_dict[key]} '
            return sets

        # Build up an UPDATE mode environment variable line we want to inject into each stage.
        update_env_line = None
        if update_envs:
            update_env_line = f"ENV {env_update_line_flag} " + get_env_set_list(update_envs)

        # If a merge has occurred, build up a MERGE mode environment variable line we want to inject into each stage.
        merge_env_line = None
        if self.env_vars_from_source is not None:  # If None, no merge has occurred. Anything else means it has.
            self.env_vars_from_source.update(dict(
                SOURCE_GIT_COMMIT=self.source_full_sha,
                SOURCE_GIT_TAG=self.source_latest_tag,
                SOURCE_GIT_URL=self.public_facing_source_url,
                SOURCE_DATE_EPOCH=self.source_date_epoch,
                OS_GIT_VERSION=f'{update_envs["OS_GIT_VERSION"]}-{self.source_full_sha[0:7]}',
                OS_GIT_COMMIT=f'{self.source_full_sha[0:7]}'
            ))
            merge_env_line = f"ENV {env_merge_line_flag} " + get_env_set_list(self.env_vars_from_source)

        # Open again!
        dfp = DockerfileParser(str(df_path))
        df_lines = dfp.content.splitlines(False)

        with df_path.open('w', encoding="utf-8") as df:
            for line in df_lines:

                # Always remove the env line we update each time.
                if env_update_line_flag in line:
                    continue

                # If we are adding environment variables from source, remove any previous merge line.
                if merge_env_line and env_merge_line_flag in line:
                    continue

                df.write(f'{line}\n')

                if line.startswith('FROM '):
                    if update_env_line:
                        df.write(f'{update_env_line}\n')
                    if merge_env_line:
                        df.write(f'{merge_env_line}\n')

    def _reflow_labels(self, filename="Dockerfile"):
        """
        The Dockerfile parser we are presently using writes all labels on a single line
        and occasionally make multiple LABEL statements. Calling this method with a
        Dockerfile in the current working directory will rewrite the file with
        labels at the end in a single statement.
        """

        dg_path = self.dg_path
        df_path = dg_path.joinpath(filename)
        dfp = DockerfileParser(str(df_path))
        labels = dict(dfp.labels)  # Make a copy of the labels we need to add back

        # Delete any labels from the modeled content
        for key in dfp.labels:
            del dfp.labels[key]

        # Capture content without labels
        df_content = dfp.content.strip()

        # Write the file back out and append the labels to the end
        with df_path.open('w', encoding="utf-8") as df:
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

        # Initialize env_vars_from source.
        # update_distgit_dir makes a distinction between None and {}
        self.env_vars_from_source = {}
        source_dir = self.source_path()
        with Dir(source_dir):
            if self.metadata.commitish:
                self.runtime.logger.info(f"Rebasing image {self.name} from specified commit-ish {self.metadata.commitish}...")
                cmd = ["git", "checkout", self.metadata.commitish]
                exectools.cmd_assert(cmd)
            # gather source repo short sha for audit trail
            rc, out, _ = exectools.cmd_gather(["git", "rev-parse", "--short", "HEAD"])
            self.source_sha = out.strip()
            out, _ = exectools.cmd_assert(["git", "rev-parse", "HEAD"])
            self.source_full_sha = out.strip()
            rc, out, _ = exectools.cmd_gather("git log -1 --format=%ct")
            self.source_date_epoch = out.strip()
            rc, out, _ = exectools.cmd_gather("git describe --always --tags HEAD")
            self.source_latest_tag = out.strip()

            out, _ = exectools.cmd_assert(["git", "remote", "get-url", "origin"], strip=True)
            self.actual_source_url = out  # This may differ from the URL we report to the public
            self.public_facing_source_url, _ = self.runtime.get_public_upstream(out)  # Point to public upstream if there are private components to the URL

            # If private_fix has not already been set (e.g. by --embargoed), determine if the source contains private fixes by checking if the private org branch commit exists in the public org
            if self.private_fix is None and self.metadata.public_upstream_branch:
                self.private_fix = not util.is_commit_in_public_upstream(self.source_full_sha, self.metadata.public_upstream_branch, source_dir)

            self.env_vars_from_source.update(self.metadata.extract_kube_env_vars())

        # See if the config is telling us a file other than "Dockerfile" defines the
        # distgit image content.
        if self.config.content.source.dockerfile is not Missing:
            # Be aware that this attribute sometimes contains path elements too.
            dockerfile_name = self.config.content.source.dockerfile
        else:
            dockerfile_name = "Dockerfile"

        # The path to the source Dockerfile we are reconciling against.
        source_dockerfile_path = os.path.join(self.source_path(), dockerfile_name)

        # Clean up any files not special to the distgit repo
        ignore_list = BASE_IGNORE
        ignore_list.extend(self.runtime.group_config.get('dist_git_ignore', []))
        ignore_list.extend(self.config.get('dist_git_ignore', []))

        dg_path = self.dg_path
        for ent in dg_path.iterdir():

            if ent.name in ignore_list:
                continue

            # Otherwise, clean up the entry
            if ent.is_file() or ent.is_symlink():
                ent.unlink()
            else:
                shutil.rmtree(str(ent.resolve()))

        # Copy all files and overwrite where necessary
        recursive_overwrite(self.source_path(), self.distgit_dir)

        df_path = dg_path.joinpath('Dockerfile')

        if df_path.exists():
            # The w+ below will not overwrite a symlink file with real content (it will
            # be directed to the target file). So unlink explicitly.
            df_path.unlink()

        with open(source_dockerfile_path, mode='r', encoding='utf-8') as source_dockerfile, \
             open(str(df_path), mode='w+', encoding='utf-8') as distgit_dockerfile:
            # The source Dockerfile could be named virtually anything (e.g. Dockerfile.rhel) or
            # be a symlink. Ultimately, we don't care - we just need its content in distgit
            # as /Dockerfile (which OSBS requires). Read in the content and write it back out
            # to the required distgit location.
            source_dockerfile_content = source_dockerfile.read()
            distgit_dockerfile.write(source_dockerfile_content)

        # Clean up any extraneous Dockerfile.* that might be distractions (e.g. Dockerfile.centos)
        for ent in dg_path.iterdir():
            if ent.name.startswith("Dockerfile."):
                ent.unlink()

        # Delete .gitignore since it may block full sync and is not needed here
        gitignore_path = dg_path.joinpath('.gitignore')
        if gitignore_path.is_file():
            gitignore_path.unlink()

        owners = []
        if self.config.owners is not Missing and isinstance(self.config.owners, list):
            owners = list(self.config.owners)

        # If upstream has not identified the BZ component, have the pipeline send them a nag note.
        maintainer = self.metadata.get_maintainer_info()
        if owners and not maintainer.get('component', None):
            week_number = date.today().isocalendar()[1]  # Determine current week number
            oit_path = dg_path.joinpath('.oit')
            util.mkdirs(oit_path)
            notified_week_path: pathlib.Path = oit_path.joinpath('last_notified_week')
            bz_notify = True
            if notified_week_path.is_file():
                last_notified_week = notified_week_path.read_text('utf-8')
                if str(week_number) == last_notified_week.strip():
                    bz_notify = False  # We've already nagged this week

            # Populate or update the week number in distgit
            notified_week_path.write_text(str(week_number), 'utf-8')

            record_type = 'bz_maintainer_notify' if bz_notify else 'bz_maintainer_missing'
            self.runtime.add_record(record_type,
                                    distgit=self.metadata.qualified_name,
                                    image=self.config.name,
                                    owners=','.join(owners),
                                    public_upstream_url=self.public_facing_source_url)

        dockerfile_notify = False

        # Create a sha for Dockerfile. We use this to determined if we've reconciled it before.
        source_dockerfile_hash = hashlib.sha256(io.open(source_dockerfile_path, 'rb').read()).hexdigest()

        reconciled_path = dg_path.joinpath('.oit', 'reconciled')
        util.mkdirs(reconciled_path)
        reconciled_df_path = reconciled_path.joinpath(f'{source_dockerfile_hash}.Dockerfile')

        # If the file does not exist, the source file has not been reconciled before.
        if not reconciled_df_path.is_file():
            # Something has changed about the file in source control
            dockerfile_notify = True
            # Record that we've reconciled against this source file so that we do not notify the owner again.
            shutil.copy(str(source_dockerfile_path), str(reconciled_df_path))

        if dockerfile_notify:
            # Leave a record for external processes that owners will need to be notified.
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

            if author_email:
                owners.append(author_email)

            sub_path = self.config.content.source.path
            if not sub_path:
                source_dockerfile_subpath = dockerfile_name
            else:
                source_dockerfile_subpath = "{}/{}".format(sub_path, dockerfile_name)
            # there ought to be a better way to determine the source alias that was registered:
            source_root = self.runtime.resolve_source(self.metadata)
            source_alias = self.config.content.source.get('alias', os.path.basename(source_root))

            self.runtime.add_record("dockerfile_notify",
                                    distgit=self.metadata.qualified_name,
                                    image=self.config.name,
                                    owners=','.join(owners),
                                    source_alias=source_alias,
                                    source_dockerfile_subpath=source_dockerfile_subpath,
                                    dockerfile=str(dg_path.joinpath('Dockerfile')))

    def _run_modifications(self):
        """
        Interprets and applies content.source.modify steps in the image metadata.
        """
        dg_path = self.dg_path
        df_path = dg_path.joinpath('Dockerfile')
        with df_path.open('r', encoding="utf-8") as df:
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
                modifier = self.source_modifier_factory.create(**modification, distgit_path=self.dg_path)
                # pass context as a dict so that the act function can modify its content
                context = {
                    "component_name": self.metadata.distgit_key,
                    "kind": "Dockerfile",
                    "content": dockerfile_data,
                    "set_env": {"PATH": path},
                    "distgit_path": self.dg_path,
                }
                modifier.act(context=context, ceiling_dir=str(dg_path))
                new_dockerfile_data = context.get("result")
            else:
                raise IOError("Don't know how to perform modification action: %s" % modification.action)
        if new_dockerfile_data is not None and new_dockerfile_data != dockerfile_data:
            with df_path.open('w', encoding="utf-8") as df:
                df.write(new_dockerfile_data)

    def extract_version_release_private_fix(self) -> Tuple[str, str, str]:
        """
        Extract version, release, and private_fix fields from Dockerfile.
        """
        prev_release = None
        private_fix = None
        df_path = self.dg_path.joinpath('Dockerfile')
        if df_path.is_file():
            dfp = DockerfileParser(str(df_path))
            # extract previous release to enable incrementing it
            prev_release = dfp.labels.get("release")
            if prev_release:
                if prev_release.endswith(".p1"):
                    private_fix = True
                elif prev_release.endswith(".p0"):
                    private_fix = False
            version = dfp.labels["version"]
            return version, prev_release, private_fix
        return None, None, None

    def rebase_dir(self, version, release, terminate_event):
        try:
            # If this image is FROM another group member, we need to wait on that group member to determine if there are embargoes in that group member.
            image_from = Model(self.config.get('from', None))
            if image_from.member is not Missing:
                self.wait_for_rebase(image_from.member, terminate_event)

            dg_path = self.dg_path
            df_path = dg_path.joinpath('Dockerfile')
            prev_release = None
            with Dir(self.distgit_dir):
                prev_version, prev_release, prev_private_fix = self.extract_version_release_private_fix()
                if version is None and not self.runtime.local:
                    # Extract the previous version and use that
                    version = prev_version

                # Make our metadata directory if it does not exist
                util.mkdirs(dg_path.joinpath('.oit'))

                # If content.source is defined, pull in content from local source directory
                if self.has_source():
                    self._merge_source()

                    # before mods, check if upstream source version should be used
                    # this will override the version fetch above
                    if self.metadata.config.get('use_source_version', False):
                        dfp = DockerfileParser(str(df_path))
                        version = dfp.labels["version"]
                else:
                    self.private_fix = bool(prev_private_fix)  # preserve private_fix boolean for distgit-only repo

                # Source or not, we should find a Dockerfile in the root at this point or something is wrong
                assertion.isfile(df_path, "Unable to find Dockerfile in distgit root")

                if self.config.content.source.modifications is not Missing:
                    self._run_modifications()

            if self.private_fix:
                self.logger.warning("The source of this image contains embargoed fixes.")

            real_version, real_release = self.update_distgit_dir(version, release, prev_release)
            self.rebase_status = True
            return real_version, real_release
        except Exception:
            self.rebase_status = False
            raise
        finally:
            self.rebase_event.set()  # awake all threads that are waiting for this image to be rebased


class RPMDistGitRepo(DistGitRepo):

    def __init__(self, metadata, autoclone=True):
        super(RPMDistGitRepo, self).__init__(metadata, autoclone)
        self.source = self.config.content.source
        # if self.source.specfile is Missing:
        #     raise ValueError('Must specify spec file name for RPMs.')

    async def resolve_specfile_async(self) -> Tuple[pathlib.Path, Tuple[str, str, str], str]:
        """ Returns the path, NVR, and commit hash of the spec file in distgit_dir

        :return: (spec_path, NVR, commit)
        """
        specs = glob.glob(f'{self.distgit_dir}/*.spec')
        if len(specs) != 1:
            raise IOError('Unable to find .spec file in RPM distgit: ' + self.name)
        spec_path = pathlib.Path(specs[0])

        async def _get_nvr():
            cmd = ["rpmspec", "-q", "--qf", "%{name}-%{version}-%{release}", "--srpm", "--undefine", "dist", "--", spec_path]
            out, _ = await exectools.cmd_assert_async(cmd, strip=True)
            return out.rsplit("-", 2)

        async def _get_commit():
            async with aiofiles.open(spec_path, "r") as f:
                async for line in f:
                    line = line.strip()
                    k = "%global commit "
                    if line.startswith(k):
                        return line[len(k):]
            return None

        nvr, commit = await asyncio.gather(_get_nvr(), _get_commit())

        return spec_path, nvr, commit
