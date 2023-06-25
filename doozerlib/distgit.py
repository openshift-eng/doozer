import asyncio
import copy
import errno
import glob
import hashlib
import io
import json
import logging
import os
import pathlib
import re
import shutil
import sys
import time
import traceback
from datetime import date, datetime
from multiprocessing import Event, Lock
from typing import Dict, List, Optional, Tuple, Union, Set

import aiofiles
import bashlex
import requests
import yaml
from dockerfile_parse import DockerfileParser
from tenacity import (before_sleep_log, retry, retry_if_not_result,
                      stop_after_attempt, wait_fixed)

import doozerlib
from doozerlib import assertion, constants, exectools, logutil, state, util
from doozerlib.assembly import AssemblyTypes
from doozerlib.brew import get_build_objects, watch_task, BuildStates
from doozerlib.dblib import Record
from doozerlib.exceptions import DoozerFatalError
from doozerlib.model import ListModel, Missing, Model
from doozerlib.osbs2_builder import OSBS2Builder, OSBS2BuildError
from doozerlib.pushd import Dir
from doozerlib.release_schedule import ReleaseSchedule
from doozerlib.rpm_utils import parse_nvr
from doozerlib.source_modifications import SourceModifierFactory
from doozerlib.util import convert_remote_git_to_https, yellow_print
from doozerlib.comment_on_pr import CommentOnPr
from string import Template

# doozer used to be part of OIT
OIT_COMMENT_PREFIX = '#oit##'
OIT_BEGIN = '##OIT_BEGIN'
OIT_END = '##OIT_END'

CONTAINER_YAML_HEADER = """
# This file is managed by doozer: https://github.com/openshift-eng/doozer
# operated by the OpenShift Automated Release Tooling team (#aos-art on CoreOS Slack).

# Any manual changes will be overwritten by doozer on the next build.
#
# See https://source.redhat.com/groups/public/container-build-system/container_build_system_wiki/odcs_integration_with_osbs
# for more information on maintaining this file and the format and examples

---
"""

# Always ignore these files/folders when rebasing into distgit
# May be added to based on group/image config
BASE_IGNORE = [".git", ".oit"]

logger = logutil.getLogger(__name__)


def recursive_overwrite(src, dest, ignore=set()):
    """
    Use rsync to copy one file tree to a new location
    """
    exclude = ' --exclude .git '
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


def map_image_name(name, image_map):
    for match, replacement in image_map.items():
        if name.find(match) != -1:
            return name.replace(match, replacement)
    return name


class DistGitRepo(object):

    def __init__(self, metadata, autoclone=True):
        self.metadata = metadata
        self.config: Model = metadata.config
        self.runtime: "doozerlib.runtime.Runtime" = metadata.runtime
        self.name: str = self.metadata.name
        self.distgit_dir: str = None
        self.dg_path: pathlib.Path = None
        self.build_status = False
        self.push_status = False

        self.branch: str = self.runtime.branch
        self.sha: str = None

        self.source_sha: str = None
        self.source_full_sha: str = None
        self.source_latest_tag: str = None
        self.source_date_epoch = None
        self.actual_source_url: str = None
        self.public_facing_source_url: str = None

        self.uuid_tag = None

        # If this is a standard release, private_fix will be set to True if the source contains
        # embargoed (private) CVE fixes. Defaulting to None which means the value should be determined while rebasing.
        self.private_fix = None
        if self.runtime.assembly_type != AssemblyTypes.STREAM:
            # Only stream releases can have embargoed workflows.
            self.private_fix = False

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

    def pull_sources(self):
        """
        Pull any distgit sources (use only after after clone)
        """
        with Dir(self.distgit_dir):
            sources_file: pathlib.Path = self.dg_path.joinpath('sources')
            if not sources_file.exists():
                self.logger.debug('No sources file exists; skipping rhpkg sources')
                return
            exectools.cmd_assert('rhpkg sources')

    def clone(self, distgits_root_dir, distgit_branch):
        if self.metadata.prevent_cloning:
            raise IOError(f'Attempt to clone downstream {self.metadata.distgit_key} after cloning disabled; a regression has been introduced.')

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

                if fake_distgit and self.runtime.command == 'images:rebase':
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

                    if self.metadata.namespace == 'containers':
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
        self.sha, _ = exectools.cmd_assert(["git", "-C", self.distgit_dir, "rev-parse", "HEAD"], strip=True)

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

    def commit(self, cmdline_commit_msg: str, commit_attributes: Optional[Dict[str, Union[int, str, bool]]] = None, log_diff=False):
        if self.runtime.local:
            return ''  # no commits if local

        with Dir(self.distgit_dir):
            commit_payload: Dict[str, Union[int, str, bool]] = {
                'MaxFileSize': 100 * 1024 * 1024,  # 100MB push limit; see https://source.redhat.com/groups/public/release-engineering/release_engineering_rcm_wiki/dist_git_update_hooks
                'jenkins.url': None if 'unittest' in sys.modules.keys() else os.getenv('BUILD_URL'),  # Get the Jenkins build URL if available, but ignore if this is a unit test run
            }

            if self.dg_path:  # Might not be set if this is a unittest
                df_path = self.dg_path.joinpath('Dockerfile')
                if df_path.exists():
                    # This is an image distgit commit, we can help the callers by reading in env variables for the commit message.
                    # RPM commits are expected to pass these values in directly in commit_attributes.
                    dfp = DockerfileParser(str(df_path))
                    for var_name in ['version', 'release', 'io.openshift.build.source-location', 'io.openshift.build.commit.id']:
                        commit_payload[var_name] = dfp.labels.get(var_name, None)

            if commit_attributes:
                commit_payload.update(commit_attributes)

            # The commit should be a valid yaml document so we can retrieve details
            # programmatically later. The human specified portion of the commit is
            # included in comments above the yaml payload.
            cmdline_commit_msg = cmdline_commit_msg.strip().replace('\n', '\n# ')  # If multiple lines are specified, split across commented lines.
            commit_msg = f'# {cmdline_commit_msg}\n'  # Any message specified in '-m' during rebase
            commit_msg += yaml.safe_dump(commit_payload, default_flow_style=False, sort_keys=True)

            self.logger.info("Adding commit to local repo:\n{}".format(commit_msg))
            if log_diff:
                diff = self._get_diff()
                if diff and diff.strip():
                    self.runtime.add_distgits_diff(self.metadata.distgit_key, diff)
            # commit changes; if these flake there is probably not much we can do about it
            exectools.cmd_assert(["git", "add", "-A", "."])
            exectools.cmd_assert(["git", "commit", "--allow-empty", "-m", commit_msg])
            rc, sha, err = exectools.cmd_gather(["git", "rev-parse", "HEAD"])
            assertion.success(rc, "Failure fetching commit SHA for {}".format(self.distgit_dir))
        self.sha = sha.strip()
        return self.sha

    def cgit_file_available(self, filename: str = ".oit/signed.repo") -> Tuple[bool, str]:
        """ Check if the specified file associated with the commit hash pushed to distgit is available on cgit
        :return: (existence, url)
        """
        assert self.sha is not None
        self.logger.debug("Checking if distgit commit %s is available on cgit...", self.sha)
        url = self.metadata.cgit_file_url(filename, commit_hash=self.sha, branch=self.branch)
        response = requests.head(url)
        if response.status_code == 404:
            self.logger.debug("Distgit commit %s is not available on cgit", self.sha)
            return False, url
        response.raise_for_status()
        self.logger.debug("Distgit commit %s is available on cgit", self.sha)
        return True, url

    @retry(retry=retry_if_not_result(lambda r: r), wait=wait_fixed(10), stop=stop_after_attempt(60), before_sleep=before_sleep_log(logger, logging.WARNING))
    def wait_on_cgit_file(self, filename: str = ".oit/signed.repo"):
        """ Poll cgit for the specified file associated with the commit hash pushed to distgit
        """
        existence, _ = self.cgit_file_available(filename)
        return existence

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
                    # Many builds require a tag associated with a commit to be a semver
                    # and they will only fail at runtime when parsing that tag
                    # if it is not a valid semver. This means we must have a valid
                    # tag for the commit. Since 4.6+ uses assemblies and we always have
                    # unique tags with timestamps, we assert the push for 4.x
                    major, _ = self.runtime.get_major_minor_fields()
                    if major >= 4:
                        exectools.cmd_assert("timeout {} git push --tags".format(timeout), retries=3)
                    else:
                        # Not asserting this exec since this is non-fatal if a tag already exists,
                        # and tags in dist-git can't be --force overwritten. Timeouts at
                        # 60 seconds have been observed.
                        exectools.cmd_gather(['timeout', '300', 'git', 'push', '--tags'])
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
        self.org_image_name = None
        self.org_version = None
        self.org_release = None
        super(ImageDistGitRepo, self).__init__(metadata, autoclone=autoclone)
        self.build_lock = Lock()
        self.build_lock.acquire()
        self.rebase_event = Event()
        self.rebase_status = False
        self.logger: logging.Logger = metadata.logger
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
        build_method = self.runtime.group_config.default_image_build_method or "osbs2"
        # If our config specifies something, override with that.
        if self.config.image_build_method is not Missing:
            build_method = self.config.image_build_method

        return build_method

    def _write_fetch_artifacts(self):
        # Write fetch-artifacts-url.yaml for OSBS to fetch external artifacts
        # See https://osbs.readthedocs.io/en/osbs_ocp3/users.html#using-artifacts-from-koji-or-project-newcastle-aka-pnc
        config_value = None
        if self.config.content.source.artifacts.from_urls is not Missing:
            config_value = self.config.content.source.artifacts.from_urls.primitive()
        path = self.dg_path.joinpath('fetch-artifacts-url.yaml')
        if path.exists():  # upstream provides its own fetch-artifacts-url.yaml
            if not config_value:
                self.logger.info("Use fetch-artifacts-url.yaml provided by upstream.")
                return
            raise ValueError("Image config option content.source.artifacts.from_urls cannot be used if upstream source has fetch-artifacts-url.yaml")
        if not config_value:
            return  # fetch-artifacts-url.yaml is not needed.
        self.logger.info('Generating fetch-artifacts-url.yaml')
        with path.open("w") as f:
            yaml.safe_dump(config_value, f)

    def _write_osbs_image_config(self, version: str):
        # Writes OSBS image config (container.yaml).
        # For more info about the format, see https://osbs.readthedocs.io/en/latest/users.html#image-configuration.

        self.logger.info('Generating container.yaml')
        container_config = self._generate_osbs_image_config(version)

        if 'compose' in container_config:
            self.logger.info("Rebasing with ODCS support")
        else:
            self.logger.info("Rebasing without ODCS support")

        # generate yaml data with header
        content_yml = yaml.safe_dump(container_config, default_flow_style=False)
        with self.dg_path.joinpath('container.yaml').open('w', encoding="utf-8") as rc:
            rc.write(CONTAINER_YAML_HEADER + content_yml)

    def _generate_osbs_image_config(self, version: str) -> Dict:
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
        container_yaml: ... # verbatim container.yaml content (see https://source.redhat.com/groups/public/container-build-system/container_build_system_wiki/odcs_integration_with_osbs)
        """

        # list of platform (architecture) names to build this image for
        arches = self.metadata.get_arches()

        # override image config with this dict
        config_overrides = {}
        if self.config.container_yaml is not Missing:
            config_overrides = copy.deepcopy(self.config.container_yaml.primitive())

        # Cachito will be configured if `cachito.enabled` is True in image meta, `content.source.pkg_managers` is set in image meta,
        # or `cachito.enabled` is True in group config.
        # https://osbs.readthedocs.io/en/latest/users.html#remote-sources
        cachito_enabled = False
        if self.config.cachito.enabled:
            cachito_enabled = True
        elif self.config.cachito.enabled is Missing:
            if isinstance(self.config.content.source.pkg_managers, ListModel) or self.runtime.group_config.cachito.enabled:
                cachito_enabled = True
        if cachito_enabled and not self.has_source():
            self.logger.warning("Cachito integration for distgit-only image %s is not supported.", self.name)
            cachito_enabled = False
        if cachito_enabled:
            if config_overrides.get("go", {}).get("modules"):
                raise ValueError(f"Cachito integration is enabled for image {self.name}. Specifying `go.modules` in `container.yaml` is not allowed.")
            pkg_managers = []  # Note if cachito is enabled but `pkg_managers` is set to an empty array, Cachito will provide the sources with no package manager magic.
            if isinstance(self.config.content.source.pkg_managers, ListModel):
                # Use specified package managers
                pkg_managers = self.config.content.source.pkg_managers.primitive()
            elif self.config.content.source.pkg_managers in [Missing, None]:
                # Auto-detect package managers
                pkg_managers = self._detect_package_managers()
            else:
                raise ValueError(f"Invalid content.source.pkg_managers config for image {self.name}: {self.config.content.source.pkg_managers}")
            # Configure Cachito flags
            # https://github.com/containerbuildsystem/cachito#flags
            flags = []
            if isinstance(self.config.cachito.flags, ListModel):
                flags = self.config.cachito.flags.primitive()
            elif isinstance(self.runtime.group_config.cachito.flags, ListModel):
                flags = set(self.runtime.group_config.cachito.flags.primitive())
                if 'gomod' not in pkg_managers:
                    # Remove gomod related flags if gomod is not used.
                    flags -= {"cgo-disable", "gomod-vendor", "gomod-vendor-check"}
                elif not self.dg_path.joinpath('vendor').is_dir():
                    # Remove gomod-vendor-check flag if vendor/ is not present when gomod is used
                    flags -= {"gomod-vendor-check"}
                flags = list(flags)

            remote_source = {
                'repo': convert_remote_git_to_https(self.actual_source_url),
                'ref': self.source_full_sha,
                'pkg_managers': pkg_managers,
            }
            if flags:
                remote_source['flags'] = flags
            # Allow user to customize `packages` option for Cachito configuration.
            # See https://osbs.readthedocs.io/en/osbs_ocp3/users.html#remote-source-keys for details.
            if self.config.cachito.packages is not Missing:
                remote_source['packages'] = self.config.cachito.packages.primitive()
            elif self.config.content.source.path:  # source is in subdirectory
                remote_source['packages'] = {pkg_manager: [{"path": self.config.content.source.path}] for pkg_manager in pkg_managers}
            config_overrides.update({
                'remote_sources': [
                    {
                        'name': 'cachito-gomod-with-deps',  # The remote source name is always `cachito-gomod-with-deps` for backward compatibility even if gomod is not used.
                        'remote_source': remote_source,
                    }
                ]
            })

        if self.image_build_method is not Missing and self.image_build_method != "osbs2":
            config_overrides['image_build_method'] = self.image_build_method

        if arches:
            config_overrides.setdefault('platforms', {})['only'] = arches

        # Request OSBS to apply specified tags to the newly-built image as floating tags.
        # See https://osbs.readthedocs.io/en/latest/users.html?highlight=tags#image-tags
        #
        # Include the UUID in the tags. This will allow other images being rebased
        # to have a known tag to refer to this image if they depend on it - even
        # before it is built.
        floating_tags = {f"{version}.{self.runtime.uuid}"}
        if self.runtime.assembly:
            floating_tags.add(f"assembly.{self.runtime.assembly}")
        vsplit = version.split(".")  # Split the version number: v4.3.4 => [ 'v4', '3, '4' ]
        if len(vsplit) > 1:
            floating_tags.add(f"{vsplit[0]}.{vsplit[1]}")
        if len(vsplit) > 2:
            floating_tags.add(f"{vsplit[0]}.{vsplit[1]}.{vsplit[2]}")
        if self.metadata.config.additional_tags:
            floating_tags |= set(self.metadata.config.additional_tags)
        if floating_tags:
            config_overrides["tags"] = sorted(floating_tags)

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

    def _detect_package_managers(self):
        """ Detect and return package managers used by the source
        :return: a list of package managers
        """
        if not self.dg_path or not self.dg_path.is_dir():
            raise FileNotFoundError(f"Distgit directory for image {self.name} hasn't been cloned.")
        pkg_manager_files = {
            "gomod": ["go.mod"],
            "npm": ["npm-shrinkwrap.json", "package-lock.json"],
            "pip": ["requirements.txt", "requirements-build.txt"],
            "yarn": ["yarn.lock"],
        }
        pkg_managers: List[str] = []
        for pkg_manager, files in pkg_manager_files.items():
            if any(self.dg_path.joinpath(file).is_file() for file in files):
                pkg_managers.append(pkg_manager)
        return pkg_managers

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
                _, version, release = self.metadata.get_latest_build_info(complete_before_event=-1)

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
                            rc, out, err = exectools.cmd_gather(mirror_cmd, timeout=1800)
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
            self.logger.info("Skipping image rebase since it is not included: %s" % image_name)
            return
        dgr = image.distgit_repo()
        self.logger.info("Waiting for image rebase: %s" % image_name)
        dgr.rebase_event.wait()
        if not dgr.rebase_status:  # failed to rebase
            raise IOError(f"Error rebasing image: {self.metadata.qualified_name} ({image_name} was waiting)")
        self.logger.info("Image rebase for %s completed. Stop waiting." % image_name)
        if terminate_event.is_set():
            raise KeyboardInterrupt()

    def build_container(
            self, profile, push_to_defaults, additional_registries, terminate_event,
            scratch=False, retries=3, realtime=False, dry_run=False, registry_config_dir=None, filter_by_os=None, comment_on_pr=False):
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
            "has_olm_bundle": 1 if self.config['update-csv'] is not Missing else 0,
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

            if self.runtime.assembly and util.isolate_assembly_in_release(release) != self.runtime.assembly:
                # Assemblies should follow its naming convention
                raise ValueError(f"Image {self.name} is not rebased with assembly '{self.runtime.assembly}'.")

            # Allow an image to wait on an arbitrary image in the group. This is presently
            # just a workaround for: https://projects.engineering.redhat.com/browse/OSBS-5592
            if self.config.wait_for is not Missing:
                self._set_wait_for(self.config.wait_for, terminate_event)

            push_version, push_release = ('', '')
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

                if len(self.metadata.targets) > 1:
                    # FIXME: Currently we don't really support building images against multiple targets,
                    # or we would overwrite the image tag when pushing to the registry.
                    # `targets` is defined as an array just because we want to keep consistency with RPM build.
                    raise DoozerFatalError("Building images against multiple targets is not currently supported.")

                if self.image_build_method != "osbs2":
                    raise DoozerFatalError(f"Do not understand image build method {self.image_build_method}. Only osbs2 exists")
                osbs2 = OSBS2Builder(self.runtime, scratch=scratch, dry_run=dry_run)
                try:
                    task_id, task_url, build_info = asyncio.run(osbs2.build(self.metadata, profile, retries=retries))
                    record["task_id"] = task_id
                    record["task_url"] = task_url
                    if build_info:
                        record["nvrs"] = build_info["nvr"]
                    if not dry_run:
                        self.update_build_db(True, task_id=task_id, scratch=scratch)
                        if comment_on_pr:
                            try:
                                comment_on_pr_obj = CommentOnPr(distgit_dir=self.distgit_dir,
                                                                token=os.getenv(constants.GITHUB_TOKEN))
                                comment_on_pr_obj.set_repo_details()
                                comment_on_pr_obj.set_github_client()
                                comment_on_pr_obj.set_pr_from_commit()
                                # Message to be posted to the comment
                                message = Template("**[ART PR BUILD NOTIFIER]**\n\n"
                                                   "This PR has been included in build "
                                                   "[$nvr](https://brewweb.engineering.redhat.com/brew/buildinfo"
                                                   "?buildID=$build_id) "
                                                   "for distgit *$distgit_name*. \n All builds following this will "
                                                   "include this PR.")
                                comment_on_pr_obj.post_comment(message.substitute(nvr=build_info["nvr"],
                                                                                  build_id=build_info["id"],
                                                                                  distgit_name=self.metadata.name))
                            except Exception as e:
                                self.logger.error(f"Error commenting on PR for build task id {task_id} for distgit"
                                                  f"{self.metadata.name}: {e}")
                        if not scratch:
                            push_version = build_info["version"]
                            push_release = build_info["release"]
                except OSBS2BuildError as build_err:
                    record["task_id"], record["task_url"] = build_err.task_id, build_err.task_url
                    if not dry_run:
                        self.update_build_db(False, task_id=build_err.task_id, scratch=scratch)
                    raise
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

        if not self.runtime.db:
            self.logger.error('Database connection is not initialized, skipping writing record.')
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

                # Ignore io.openshift labels other than the ones specified below
                for label in [
                        'io.openshift.build.source-location',
                        'io.openshift.build.commit.id',
                        'io.openshift.build.commit.url',
                        'io.openshift.release.operator',
                        'io.openshift.build.versions']:
                    if label in dfp.labels:
                        Record.set(f'label.{label}', dfp.labels[label])

                for k, v in dfp.envs.items():
                    if k.startswith('KUBE_') or k.startswith('OS_'):
                        Record.set(f'env.{k}', dfp.envs.get(k, ''))

                Record.set('incomplete', False)
                if task_id is not None:
                    try:
                        with self.runtime.shared_koji_client_session() as kcs:
                            task_result = kcs.getTaskResult(task_id, raise_fault=False)
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

    def _logs_dir(self, task_id=None):
        segments = [self.runtime.brew_logs_dir, self.metadata.distgit_key]
        if task_id is not None:
            segments.append("noarch-" + task_id)
        return os.path.join(*segments)

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
    def _mangle_pkgmgr(cmd):
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

            # replace package manager config with a no-op
            if re.search(r'(^|/)(microdnf\s+|dnf\s+|yum-)config-manager$', subcmd.parts[0].word):
                cmd = splice(subcmd.pos, ": 'removed yum-config-manager'")
                changed = True
                continue
            if re.search(r'(^|/)(micro)?dnf$', subcmd.parts[0].word) and len(subcmd.parts) > 1 and subcmd.parts[1].word == "config-manager":
                cmd = splice(subcmd.pos, ": 'removed dnf config-manager'")
                changed = True
                continue

            # clear repo options from yum and dnf commands
            if not re.search(r'(^|/)(yum|dnf|microdnf)$', subcmd.parts[0].word):
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
                changed, new_value = self._mangle_pkgmgr(entry['value'])
                if changed:
                    dfp.add_lines_at(entry, "RUN " + new_value, replace=True)

    def _mapped_image_from_member(self, image, original_parents, count):
        base = image.member
        from_image_metadata = self.runtime.resolve_image(base, False)

        if from_image_metadata is None:
            if not self.runtime.ignore_missing_base:
                raise IOError(
                    "Unable to find base image metadata [%s] in included images. "
                    "Use --ignore-missing-base to ignore." % base
                )
            elif self.runtime.latest_parent_version or self.runtime.assembly_basis_event:
                # If there is a basis event, we must look for latest; we can't just persist
                # what is in the Dockerfile. It has to be constrained to the brew event.
                self.logger.info(
                    '[{}] parent image {} not included. Looking up FROM tag.'.format(self.config.name, base))
                base_meta = self.runtime.late_resolve_image(base)
                _, v, r = base_meta.get_latest_build_info()
                if util.isolate_pflag_in_release(r) == 'p1':  # latest parent is embargoed
                    self.private_fix = True  # this image should also be embargoed
                return "{}:{}-{}".format(base_meta.config.name, v, r)
            # Otherwise, the user is not expecting the FROM field to be updated in this Dockerfile.
            else:
                return original_parents[count]
        else:
            if self.runtime.local:
                return '{}:latest'.format(from_image_metadata.config.name)
            else:
                from_image_distgit = from_image_metadata.distgit_repo()
                if from_image_distgit.private_fix is None:  # This shouldn't happen.
                    raise ValueError(
                        f"Parent image {base} doesn't have .p0/.p1 flag determined. "
                        f"This indicates a bug in Doozer."
                    )
                # If the parent we are going to build is embargoed, this image should also be embargoed
                self.private_fix = from_image_distgit.private_fix

                # Everything in the group is going to be built with the uuid tag, so we must
                # assume that it will exist for our parent.
                return f"{from_image_metadata.config.name}:{self.uuid_tag}"

    def _mapped_image_for_assembly_build(self, parent_images, i):
        # When rebasing for an assembly build, we want to use the same parent image
        # as our corresponding basis image. To that end, we cannot rely on a stream.yml
        # entry -- which usually refers to a floating tag. Instead, we look up the latest
        # build of this image, relative to the assembly basis event, in brew. It will have
        # information on the exact parent images used at the time. We want to use that
        # specific sha.
        # If you are here trying to figure out how to change this behavior, you should
        # consider using 'from!:' in the assembly metadata for this component. This will
        # allow you to fully pin the parent images (e.g. {'from!:' ['image': <pullspec>] })
        latest_build = self.metadata.get_latest_build(default=None)
        assembly_msg = f'{self.metadata.distgit_key} in assembly {self.runtime.assembly} ' \
                       f'with basis event {self.runtime.assembly_basis_event}'
        if not latest_build:
            raise IOError(f'Unable to find latest build for {assembly_msg}')
        build_model = Model(dict_to_model=latest_build)
        if build_model.extra.image.parent_images is Missing:
            raise IOError(f'Unable to find latest build parent images in {latest_build} for {assembly_msg}')
        elif len(build_model.extra.image.parent_images) != len(parent_images):
            raise IOError(
                f'Did not find the expected cardinality ({len(parent_images)} '
                f'of parent images in {latest_build} for {assembly_msg}'
            )

        # build_model.extra.image.parent_images is an array of tags
        # (entries like openshift/golang-builder:rhel_8_golang_1.15).
        # We can't use floating tags for this, so we need to look up those tags in parent_image_builds,
        # which is also in the extras data. Example parent_image_builds:
        # {
        #     "registry-proxy.engineering.redhat.com/rh-osbs/openshift-base-rhel8:v4.6.0.20210528.150530": {
        #         "id": 1616717,
        #         "nvr": "openshift-base-rhel8-container-v4.6.0-202105281403.p0.git.f17f552"
        #     },
        #     "registry-proxy.engineering.redhat.com/rh-osbs/openshift-golang-builder:rhel_8_golang_1.15": {
        #         "id": 1542268,
        #         "nvr": "openshift-golang-builder-container-v1.15.7-202103191923.el8"
        #     }
        # }
        # Note this map actually gets us to an NVR.
        # Example latest_build return: https://gist.github.com/jupierce/57e99b80572336e8652df3c6be7bf664
        target_parent_name = build_model.extra.image.parent_images[i]  # Which parent are looking for? e.g. 'openshift/golang-builder:rhel_8_golang_1.15'
        tag_pullspec = self.runtime.resolve_brew_image_url(
            target_parent_name)  # e.g. registry-proxy.engineering.redhat.com/rh-osbs/openshift-golang-builder:rhel_8_golang_1.15
        parent_build_info = build_model.extra.image.parent_image_builds[tag_pullspec]
        if parent_build_info is Missing:
            raise IOError(
                f'Unable to resolve parent {target_parent_name} in {latest_build} for {assembly_msg}; tried {tag_pullspec}')
        parent_build_nvr = parse_nvr(parent_build_info.nvr)
        # Hang in there.. this is a long dance. Now that we know the NVR, we can construct
        # a truly unique pullspec.
        if '@' in tag_pullspec:
            unique_pullspec = tag_pullspec.rsplit('@', 1)[0]  # remove the sha
        elif ':' in tag_pullspec:
            unique_pullspec = tag_pullspec.rsplit(':', 1)[0]  # remove the tag
        else:
            raise IOError(f'Unexpected pullspec format: {tag_pullspec}')
        # qualify with the pullspec using nvr as a tag; e.g.
        # registry-proxy.engineering.redhat.com/rh-osbs/openshift-golang-builder:v1.15.7-202103191923.el8'
        unique_pullspec += f':{parent_build_nvr["version"]}-{parent_build_nvr["release"]}'
        return unique_pullspec

    def _should_match_upstream(self) -> bool:
        if self.runtime.group_config.canonical_builders_from_upstream is Missing:
            # Default case: override using ART's config
            return False
        elif self.runtime.group_config.canonical_builders_from_upstream == 'auto':
            # canonical_builders_from_upstream set to 'auto': rebase according to release schedule
            try:
                feature_freeze_date = ReleaseSchedule(self.runtime).get_ff_date()
                return datetime.now() < feature_freeze_date
            except ChildProcessError:
                # Could not access Gitlab: display a warning and fallback to default
                self.logger.error('Failed retrieving release schedule from Gitlab: fallback to using ART\'s config')
                return False
            except ValueError as e:
                # A GITLAB token env var was not provided: display a warning and fallback to default
                self.logger.error(f'Fallback to default ART config: {e}')
                return False
        elif self.runtime.group_config.canonical_builders_from_upstream in ['on', True]:
            # yaml parser converts bare 'on' to True, same for 'off' and False
            return True
        elif self.runtime.group_config.canonical_builders_from_upstream in ['off', False]:
            return False
        else:
            # Invalid value
            self.logger.warning(
                'Invalid value provided for "canonical_builders_from_upstream": %s',
                self.runtime.group_config.canonical_builders_from_upstream
            )
            return False

    def _mapped_image_from_stream(self, image, original_parent, dfp):
        stream = self.runtime.resolve_stream(image.stream)

        if not self._should_match_upstream():
            # Do typical stream resolution.
            return stream.image

        # canonical_builders_from_upstream flag is either True, or 'auto' and we are before feature freeze
        try:
            self.logger.debug('Retrieving image info for image %s', original_parent)
            cmd = f'oc image info {original_parent} -o json'
            out, _ = exectools.cmd_assert(cmd, retries=3)
            labels = json.loads(out)['config']['config']['Labels']

            # Get the exact build NVR
            build_nvr = f'{labels["com.redhat.component"]}-{labels["version"]}-{labels["release"]}'

            # Query Brew for build info
            self.logger.debug('Retrieving info for Brew build %s', build_nvr)
            with self.runtime.shared_koji_client_session() as koji_api:
                if not koji_api.logged_in:
                    koji_api.gssapi_login()
                build = koji_api.getBuild(build_nvr, strict=True)

            # Get the pullspec for the upstream equivalent
            upstream_equivalent_pullspec = build['extra']['image']['index']['pull'][1]

            # Verify whether the image exists
            self.logger.debug('Checking for upstream equivalent existence, pullspec: %s', upstream_equivalent_pullspec)
            cmd = f'oc image info {upstream_equivalent_pullspec} --filter-by-os linux/amd64 -o json'
            out, _ = exectools.cmd_assert(cmd, retries=3)

            # It does. Use this to rebase FROM directive
            digest = json.loads(out)['digest']

            mapped_image = f'{labels["name"]}@{digest}'
            # if upstream equivalent does not match ART's config, add a warning to the Dockerfile
            if mapped_image != stream.image:
                dfp.add_lines_at(
                    0,
                    "",
                    "# Parent images were rebased matching upstream equivalent that didn't match ART's config",
                    "")
                self.logger.info('Will override %s with upsteam equivalent %s', stream.image, mapped_image)
            return mapped_image

        except (KeyError, ChildProcessError) as e:
            # We could get:
            #   - a ChildProcessError when the upstream equivalent is not found
            #   - a ChildProcessError when trying to rebase our base images
            #   - a KeyError when 'com.redhat.component' label is undefined
            # In all of the above, we'll just do typical stream resolution

            self.logger.warning(f'Could not match upstream parent {original_parent}: {e}')
            dfp.add_lines_at(
                0,
                "",
                "# Failed matching upstream equivalent, ART configuration was used to rebase parent images",
                ""
            )
            return stream.image

    def _rebase_from_directives(self, dfp):
        image_from = Model(self.config.get('from', None))

        # Collect all the parent images we're supposed to use
        parent_images = image_from.builder if image_from.builder is not Missing else []
        parent_images.append(image_from)
        if len(parent_images) != len(dfp.parent_images):
            raise IOError(
                "Build metadata for {name} expected {count1} image parent(s), but the upstream Dockerfile "
                "contains {count2} FROM statements. These counts must match. Detail: '{meta_parents}' vs "
                "'{upstream_parents}'.".format(
                    name=self.config.name,
                    count1=len(parent_images),
                    count2=len(dfp.parent_images),
                    meta_parents=parent_images,
                    upstream_parents=dfp.parent_images,
                ))
        mapped_images = []

        original_parents = dfp.parent_images
        count = 0
        for i, image in enumerate(parent_images):
            # Does this image inherit from an image defined in a different group member distgit?
            if image.member is not Missing:
                mapped_images.append(self._mapped_image_from_member(image, original_parents, count))

            # Is this image FROM another literal image name:tag?
            elif image.image is not Missing:
                mapped_images.append(image.image)

            elif image.stream is not Missing:
                if self.runtime.assembly_basis_event:
                    # Rebasing for an assembly build
                    mapped_images.append(self._mapped_image_for_assembly_build(parent_images, i))
                else:
                    # Rebasing for a stream/test build
                    mapped_images.append(self._mapped_image_from_stream(image, original_parents[i], dfp))

            else:
                raise IOError("Image in 'from' for [%s] is missing its definition." % image.name)

            count += 1

        # Write rebased from directives
        dfp.parent_images = mapped_images

    def update_distgit_dir(self, version, release, prev_release=None, force_yum_updates=False):
        dg_path = self.dg_path
        with Dir(self.distgit_dir):
            # Source or not, we should find a Dockerfile in the root at this point or something is wrong
            assertion.isfile(dg_path.joinpath("Dockerfile"), "Unable to find Dockerfile in distgit root")

            self._generate_repo_conf()

            self._generate_config_digest()

            self._write_cvp_owners()

            self._write_fetch_artifacts()

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

            self._write_osbs_image_config(version)

            self.uuid_tag = "%s.%s" % (version, self.runtime.uuid)

            # Split the version number v4.3.4 => [ 'v4', '3, '4' ]
            vsplit = version.split(".")

            major_version = vsplit[0].lstrip('v')
            # Click validation should have ensured user specified semver, but double check because of version=None flow.
            minor_version = '0' if len(vsplit) < 2 else vsplit[1]
            patch_version = '0' if len(vsplit) < 3 else vsplit[2]

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

            jira_project, jira_component = self.metadata.get_jira_info()
            dfp.labels['io.openshift.maintainer.project'] = jira_project
            dfp.labels['io.openshift.maintainer.component'] = jira_component

            if 'from' in self.config:
                self._rebase_from_directives(dfp)

            # Set image name in case it has changed
            dfp.labels["name"] = self.config.name

            # If no version was specified, pull it from the Dockerfile
            if version is None:
                version = dfp.labels['version']

            # If the release is specified as "+", this means the user wants to bump the release.
            if release == "+":
                if self.runtime.assembly:
                    raise ValueError('"+" is not supported for the release value when assemblies are enabled')

                # increment the release that was in the Dockerfile
                if prev_release:
                    self.logger.info("Bumping release field in Dockerfile")
                    if self.runtime.group_config.public_upstreams and util.isolate_pflag_in_release(prev_release) in ('p0', 'p1'):
                        # We can assume .pX is a suffix because assemblies are asserted disabled earlier.
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

                if self.source_full_sha:
                    release += ".g" + self.source_full_sha[:7]

                if self.runtime.assembly:
                    release += f'.assembly.{self.runtime.assembly}'

                dfp.labels['release'] = release
            else:
                if self.runtime.assembly:
                    raise ValueError('Release value must be specified when assemblies are enabled')

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

            # Environment variables that will be always be injected into the Dockerfile.
            env_vars = {  # Set A
                'OS_GIT_MAJOR': major_version,
                'OS_GIT_MINOR': minor_version,
                'OS_GIT_PATCH': patch_version,
                'OS_GIT_VERSION': f'{major_version}.{minor_version}.{patch_version}{release_suffix}',
                'OS_GIT_TREE_STATE': 'clean',
                'SOURCE_GIT_TREE_STATE': 'clean',
                'BUILD_VERSION': version,
                'BUILD_RELEASE': release if release else '',
                '__doozer_group': self.runtime.group,
                '__doozer_key': self.metadata.distgit_key,
            }

            if self.config.envs:
                # Allow environment variables to be specified in the ART image metadata
                env_vars.update(self.config.envs.primitive())

            df_fileobj = self._update_yum_update_commands(force_yum_updates, io.StringIO(df_content))
            with dg_path.joinpath('Dockerfile').open('w', encoding="utf-8") as df:
                shutil.copyfileobj(df_fileobj, df)
                df_fileobj.close()

            self._update_environment_variables(env_vars)

            self._reflow_labels()

            self._update_csv(version, release)

            return version, release

    def _update_yum_update_commands(self, force_yum_updates: bool, df_fileobj: io.TextIOBase) -> io.StringIO:
        """ If force_yum_updates is True, inject "yum updates -y" in the final build stage; Otherwise, remove the lines we injected.
        Returns an in-memory text stream for the new Dockerfile content
        """
        if force_yum_updates and not self.config.get('enabled_repos'):
            # If no yum repos are disabled in image meta, "yum update -y" will fail with "Error: There are no enabled repositories in ...".
            # Remove "yum update -y" lines intead.
            self.logger.warning("Will not inject \"yum updates -y\" for this image because no yum repos are enabled.")
            force_yum_updates = False
        if force_yum_updates:
            self.logger.info("Injecting \"yum updates -y\" in each stage...")

        parser = DockerfileParser(fileobj=df_fileobj)

        df_lines_iter = iter(parser.content.splitlines(False))
        build_stage_num = len(parser.parent_images)
        final_stage_user = self.metadata.config.final_stage_user or 0

        yum_update_line_flag = "__doozer=yum-update"

        # The yum repo supplied to the image build is going to be appropriate for the base/final image in a
        # multistage build. If one of the stages of, for example, a RHEL8 image is a RHEL7 builder image,
        # we should not run yum update as it will fail loudly. Instead, we check the RHEL version of the
        # image at each stage at *build time* to ensure yum update only runs in appropriate stages.
        el_ver = self.metadata.branch_el_target()
        if el_ver == 7:
            # For rebuild logic, we need to be able to prioritize repos; RHEL7 requires a plugin to be installed.
            yum_update_line = "RUN yum install -y yum-plugin-priorities && yum update -y && yum clean all"
        else:
            yum_update_line = "RUN yum update -y && yum clean all"
        output = io.StringIO()
        build_stage = 0
        for line in df_lines_iter:
            if yum_update_line_flag in line:
                # Remove the lines we have injected by skipping 2 lines
                next(df_lines_iter)
                continue
            output.write(f'{line}\n')
            if not force_yum_updates or not line.startswith('FROM '):
                continue
            build_stage += 1

            if build_stage != build_stage_num:
                # If this is not the final stage, ignore this FROM
                continue

            # This should be directly after the last 'FROM' (i.e. in the final stage of the Dockerfile).
            # If the current user inherited from the base image for this stage is not root, `yum update -y` will fail
            # and we must change the user to be root.
            # However for the final stage, injecting "USER 0" without changing the original base image user
            # may cause unexpected behavior if the container makes assumption about the user at runtime.
            # Per https://github.com/openshift-eng/doozer/pull/428#issuecomment-861795424,
            # introduce a new metadata `final_stage_user` for images so we can switch the user back later.
            if final_stage_user:
                output.write(f"# {yum_update_line_flag}\nUSER 0\n")
            else:
                self.logger.warning("Will not inject `USER 0` before `yum update -y` for the final build stage because `final_stage_user` is missing (or 0) in image meta."
                                    " If this build fails with `yum update -y` permission denied error, please set correct `final_stage_user` and rebase again.")
            output.write(f"# {yum_update_line_flag}\n{yum_update_line}  # set final_stage_user in ART metadata if this fails\n")
            if final_stage_user:
                output.write(f"# {yum_update_line_flag}\nUSER {final_stage_user}\n")
        output.seek(0)
        return output

    def _generate_config_digest(self):
        # The config digest is used by scan-sources to detect config changes
        self.logger.info("Calculating config digest...")
        digest = self.metadata.calculate_config_digest(self.runtime.group_config, self.runtime.streams)
        with self.dg_path.joinpath(".oit", "config_digest").open('w') as f:
            f.write(digest)
        self.logger.info("Saved config digest %s to .oit/config_digest", digest)

    def _find_previous_versions(self, pattern_suffix='') -> Set[str]:
        """
        Returns: Searches brew for builds of this operator in order and processes them into a set of versions.
        These version may or may not have shipped.
        """
        with self.runtime.pooled_koji_client_session() as koji_api:
            component_name = self.metadata.get_component_name()
            package_info = koji_api.getPackage(component_name)  # e.g. {'id': 66873, 'name': 'atomic-openshift-descheduler-container'}
            if not package_info:
                raise IOError(f'No brew package is defined for {component_name}')
            package_id = package_info['id']  # we could just constrain package name using pattern glob, but providing package ID # should be a much more efficient DB query.
            pattern_prefix = f'{component_name}-v{self.metadata.branch_major_minor()}.'
            builds = koji_api.listBuilds(packageID=package_id,
                                         state=BuildStates.COMPLETE.value,
                                         pattern=f'{pattern_prefix}{pattern_suffix}*')
            nvrs: Set[str] = set([build['nvr'] for build in builds])
            # NVRS should now be a set including entries like 'cluster-nfd-operator-container-v4.10.0-202211280957.p0.ga42b581.assembly.stream'
            # We need to convert these into versions like "4.11.0-202205250107"
            versions: Set[str] = set()
            for nvr in nvrs:
                without_component = nvr[len(f'{component_name}-v'):]  # e.g. "4.10.0-202211280957.p0.ga42b581.assembly.stream"
                version_components = without_component.split('.')[0:3]  # e.g. ['4', '10', '0-202211280957']
                version = '.'.join(version_components)
                versions.add(version)

            return versions

    def _get_csv_file_and_refs(self, csv_config):
        gvars = self.runtime.group_config.vars
        bundle_dir = csv_config.get('bundle-dir', f'{gvars["MAJOR"]}.{gvars["MINOR"]}')
        manifests_dir = csv_config.get('manifests-dir')
        bundle_manifests_dir = os.path.join(manifests_dir, bundle_dir)

        refs = None
        ref_candidates = [
            os.path.join(self.distgit_dir, dirpath, 'image-references')
            for dirpath in [bundle_dir, manifests_dir, bundle_manifests_dir]
        ]
        for cand in ref_candidates:
            if os.path.isfile(cand):
                refs = cand
        if not refs:
            raise DoozerFatalError('{}: image-references file not found in any location: {}'.format(self.metadata.distgit_key, ref_candidates))

        with io.open(refs, 'r', encoding="utf-8") as f_ref:
            ref_data = yaml.full_load(f_ref)
        image_refs = ref_data.get('spec', {}).get('tags', {})
        if not image_refs:
            raise DoozerFatalError('Data in {} not valid'.format(refs))

        manifests = os.path.join(self.distgit_dir, manifests_dir, bundle_dir)
        csvs = list(pathlib.Path(manifests).glob('*.clusterserviceversion.yaml'))
        if len(csvs) < 1:
            raise DoozerFatalError('{}: did not find a *.clusterserviceversion.yaml file @ {}'.format(self.metadata.distgit_key, manifests))
        elif len(csvs) > 1:
            raise DoozerFatalError('{}: Must be exactly one *.clusterserviceversion.yaml file but found more than one @ {}'.format(self.metadata.distgit_key, manifests))
        return str(csvs[0]), image_refs

    def _update_csv(self, version, release):
        csv_config = self.metadata.config.get('update-csv', None)
        if not csv_config:
            return

        csv_file, image_refs = self._get_csv_file_and_refs(csv_config)
        registry = csv_config['registry'].rstrip("/")
        image_map = csv_config.get('image-map', {})

        for ref in image_refs:
            try:
                name = ref['name']
                name = map_image_name(name, image_map)
                spec = ref['from']['name']
            except:
                raise DoozerFatalError('Error loading image-references data for {}'.format(self.metadata.distgit_key))

            try:
                distgit = self.runtime.name_in_bundle_map.get(name, None)
                # fail if upstream is referring to an image we don't actually build
                if not distgit:
                    raise DoozerFatalError('Unable to find {} in image-references data for {}'.format(name, self.metadata.distgit_key))

                meta = self.runtime.image_map.get(distgit, None)
                if meta:  # image is currently be processed
                    uuid_tag = "%s.%s" % (version, self.runtime.uuid)  # applied by additional-tags
                    image_tag = '{}:{}'.format(meta.image_name_short, uuid_tag)
                else:
                    meta = self.runtime.late_resolve_image(distgit)
                    _, v, r = meta.get_latest_build_info()
                    image_tag = '{}:{}-{}'.format(meta.image_name_short, v, r)

                if self.metadata.distgit_key != meta.distgit_key:
                    if self.metadata.distgit_key not in meta.config.dependents:
                        raise DoozerFatalError(f'Related image contains {meta.distgit_key} but this does not have {self.metadata.distgit_key} in dependents')

                namespace = self.runtime.group_config.get('csv_namespace', None)
                if not namespace:
                    raise DoozerFatalError('csv_namespace is required in group.yaml when any image defines update-csv')
                replace = '{}/{}/{}'.format(registry, namespace, image_tag)

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
            'FULL_VER': '{}-{}'.format(version, release.split('.')[0])
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
                raise DoozerFatalError('`updates` key must be a list in art.yaml')

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

                        original_string = sr_file_str
                        sr_file_str = sr_file_str.replace(s, r)
                        if sr_file_str == original_string:
                            self.logger.error(f'Search `{s}` and replace was ineffective for {self.metadata.distgit_key}')
                    sr_file.seek(0)
                    sr_file.truncate()
                    sr_file.write(sr_file_str)

        previous_build_versions: List[str] = self._find_previous_versions()
        if previous_build_versions:
            # We need to inject "skips" versions for https://issues.redhat.com/browse/OCPBUGS-6066 .
            # We have the versions, but it needs to be written into the CSV under spec.skips.
            # First, find the "name" of this plugin that precedes the version. If everything
            # is correctly replaced in the CSV by art-config.yml, then metadata.name - spec.version
            # should leave us with the name the operator uses.
            csv_obj = yaml.safe_load(pathlib.Path(csv_file).read_text())
            olm_name = csv_obj['metadata']['name']  # "nfd.4.11.0-202205301910"
            olm_version = csv_obj['spec']['version']  # "4.11.0-202205301910"

            if not olm_name.endswith(olm_version):
                raise IOError(f'Expected {self.name} CSV metadata.name field ("{olm_name}" after rebase) to be suffixed by spec.version ("{olm_version}" after rebase). art-config.yml / upstream CSV metadata may be incorrect.')

            olm_name_prefix = olm_name[:-1 * len(olm_version)]  # "nfd."

            # Inject the skips..
            csv_obj['spec']['skips'] = [f'{olm_name_prefix}{old_version}' for old_version in previous_build_versions]

            # Re-write the CSV content.
            pathlib.Path(csv_file).write_text(yaml.dump(csv_obj))

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

            # TODO: move this into operator-lifecycle-manager metadata.envs in all branches
            if self.metadata.distgit_key == 'operator-lifecycle-manager':
                self.env_vars_from_source['GO_COMPLIANCE_EXCLUDE'] = 'build.*operator-lifecycle-manager/util/cpb'

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
            if self.private_fix is None:
                if self.metadata.public_upstream_branch and not self.runtime.is_branch_commit_hash(self.metadata.public_upstream_branch):
                    self.private_fix = not util.is_commit_in_public_upstream(self.source_full_sha, self.metadata.public_upstream_branch, source_dir)
                else:
                    self.private_fix = False

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

        dockerfile_notify = False

        # Create a sha for Dockerfile. We use this to determine if we've reconciled it before.
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
        Interprets and applies content.source.modifications steps in the image metadata.
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
        new_dockerfile_data = dockerfile_data

        for modification in self.config.content.source.modifications:
            if self.source_modifier_factory.supports(modification.action):
                # run additional modifications supported by source_modifier_factory
                modifier = self.source_modifier_factory.create(**modification, distgit_path=self.dg_path)
                # pass context as a dict so that the act function can modify its content
                context = {
                    "component_name": self.metadata.distgit_key,
                    "kind": "Dockerfile",
                    "content": new_dockerfile_data,
                    "set_env": {
                        "PATH": path,
                        "BREW_EVENT": f'{self.runtime.brew_event}',
                        "BREW_TAG": f'{self.metadata.candidate_brew_tag()}'
                    },
                    "distgit_path": self.dg_path,
                }
                modifier.act(context=context, ceiling_dir=str(dg_path))
                new_dockerfile_data = context.get("result", new_dockerfile_data)
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
                if util.isolate_pflag_in_release(prev_release) == 'p1':
                    private_fix = True
                elif util.isolate_pflag_in_release(prev_release) == 'p0':
                    private_fix = False
            version = dfp.labels.get("version")
            return version, prev_release, private_fix
        return None, None, None

    def rebase_dir(self, version: str, release: str, terminate_event, force_yum_updates=False) -> Tuple[str, str]:
        """
        - Copies the checked out upstream source commit over the content the checked out distgit commit.
        - Runs any configured source modifications for the component.
        - Updates the version and release fields in the appropriate files in the checked out distgit.
        :return: Returns a Tuple[applied_version, applied_release]. This may not match the incoming 'version'
                    and 'release' fields since the called may not have supplied literal values (e.g. release == '+').
        """
        try:
            # If this image is FROM another group member, we need to wait on that group
            # member to determine if there are embargoes in that group member.
            image_from = Model(self.config.get('from', None))
            if image_from.member is not Missing:
                self.wait_for_rebase(image_from.member, terminate_event)
            for builder in image_from.get("builder", []):
                if "member" in builder:
                    self.wait_for_rebase(builder["member"], terminate_event)

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

            real_version, real_release = self.update_distgit_dir(version, release, prev_release, force_yum_updates)
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
