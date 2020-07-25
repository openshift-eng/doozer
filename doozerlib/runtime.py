from __future__ import absolute_import, print_function, unicode_literals
from future import standard_library
standard_library.install_aliases()
from future.utils import as_native_str
from multiprocessing.dummy import Pool as ThreadPool
from contextlib import contextmanager
from collections import namedtuple

import os
import sys
import tempfile
import threading
import shutil
import atexit
import datetime
import re
import yaml
import click
import logging
import functools
import traceback
import urllib.parse
import signal
import io
import pathlib
import koji
from typing import Optional
import time

from doozerlib import gitdata
from . import logutil
from . import assertion
from . import exectools
from . import dblib
from .pushd import Dir

from .image import ImageMetadata
from .rpmcfg import RPMMetadata
from doozerlib import state
from .model import Model, Missing
from multiprocessing import Lock, RLock
from .repos import Repos
from doozerlib.exceptions import DoozerFatalError
from doozerlib import constants
from doozerlib import util
from doozerlib import brew

# Values corresponds to schema for group.yml: freeze_automation. When
# 'yes', doozer itself will inhibit build/rebase related activity
# (exiting with an error if someone tries). Other values can
# be interpreted & enforced by the build pipelines (e.g. by
# invoking config:read-config).
FREEZE_AUTOMATION_YES = 'yes'
FREEZE_AUTOMATION_SCHEDULED = 'scheduled'  # inform the pipeline that only manually run tasks should be permitted
FREEZE_AUTOMATION_NO = 'no'


# doozer cancel brew builds on SIGINT (Ctrl-C)
# but Jenkins sends a SIGTERM when cancelling a job.
def handle_sigterm(*_):
    raise KeyboardInterrupt()


signal.signal(signal.SIGTERM, handle_sigterm)


# Registered atexit to close out debug/record logs
def close_file(f):
    f.close()


def remove_tmp_working_dir(runtime):
    if runtime.remove_tmp_working_dir:
        shutil.rmtree(runtime.working_dir)
    else:
        click.echo("Temporary working directory preserved by operation: %s" % runtime.working_dir)


class WrapException(Exception):
    """ https://bugs.python.org/issue13831 """
    def __init__(self):
        super(WrapException, self).__init__()
        exc_type, exc_value, exc_tb = sys.exc_info()
        self.exception = exc_value
        self.formatted = "".join(
            traceback.format_exception(exc_type, exc_value, exc_tb))

    @as_native_str()
    def __str__(self):
        return "{}\nOriginal traceback:\n{}".format(Exception.__str__(self), self.formatted)


def wrap_exception(func):
    """ Decorate a function, wrap exception if it occurs. """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            raise WrapException()
    return wrapper


def _unpack_tuple_args(func):
    """ Decorate a function for unpacking the tuple argument `args`
        This is used to workaround Python 3 lambda not unpacking tuple arguments (PEP-3113)
    """
    @functools.wraps(func)
    def wrapper(args):
        return func(*args)
    return wrapper


# A named tuple for caching the result of Runtime._resolve_source.
SourceResolution = namedtuple('SourceResolution', [
    'source_path', 'url', 'branch', 'public_upstream_url', 'public_upstream_branch'
])


# ============================================================================
# Runtime object definition
# ============================================================================


class Runtime(object):
    # Use any time it is necessary to synchronize feedback from multiple threads.
    mutex = RLock()

    # Serialize access to the shared koji session
    koji_lock = RLock()

    # Serialize access to the console, and record log
    log_lock = Lock()

    def __init__(self, **kwargs):
        # initialize defaults in case no value is given
        self.verbose = False
        self.quiet = False
        self.load_wip = False
        self.load_disabled = False
        self.data_path = None
        self.data_dir = None
        self.latest_parent_version = False
        self.rhpkg_config = None
        self._koji_client_session = None
        self.db = None
        self.session_pool = {}
        self.session_pool_available = {}

        # Cooperative threads can request exclusive access to directories.
        # This is usually only necessary if two threads want to make modifications
        # to the same global source alias. The empty string key serves as a lock for the
        # data structure.
        self.dir_locks = {'': Lock()}

        for key, val in kwargs.items():
            self.__dict__[key] = val

        if self.latest_parent_version:
            self.ignore_missing_base = True

        self._remove_tmp_working_dir = False
        self.group_config = None

        self.cwd = os.getcwd()

        # If source needs to be cloned by oit directly, the directory in which it will be placed.
        self.sources_dir = None

        self.distgits_dir = None

        self.record_log = None
        self.record_log_path = None

        self.debug_log_path = None

        self.brew_logs_dir = None

        self.flags_dir = None

        # Map of dist-git repo name -> ImageMetadata object. Populated when group is set.
        self.image_map = {}

        # Map of dist-git repo name -> RPMMetadata object. Populated when group is set.
        self.rpm_map = {}

        # Map of source code repo aliases (e.g. "ose") to a tuple representing the source resolution cache.
        # See registry_repo.
        self.source_resolutions = {}

        # Map of source code repo aliases (e.g. "ose") to a (public_upstream_url, public_upstream_branch) tuple.
        # See registry_repo.
        self.public_upstreams = {}

        # Map of stream alias to image name.
        self.stream_alias_overrides = {}

        self.initialized = False

        # Will be loaded with the streams.yml Model
        self.streams = {}

        # Create a "uuid" which will be used in FROM fields during updates
        self.uuid = datetime.datetime.now().strftime("%Y%m%d.%H%M%S")

        # Optionally available if self.fetch_rpms_for_tag() is called
        self.rpm_list = None
        self.rpm_search_tree = None

        # Used for image build ordering
        self.image_tree = {}
        self.image_order = []
        # allows mapping from name or distgit to meta
        self.image_name_map = {}

        # holds untouched group config
        self.raw_group_config = {}

        # Used to capture missing packages for 4.x build
        self.missing_pkgs = set()

        # Whether to prevent builds for this group. Defaults to 'no'.
        self.freeze_automation = FREEZE_AUTOMATION_NO

        self.rhpkg_config_lst = []
        if self.rhpkg_config:
            if not os.path.isfile(self.rhpkg_config):
                raise DoozerFatalError('--rhpkg-config option given is not a valid file! {}'.format(self.rhpkg_config))
            self.rhpkg_config = ' --config {} '.format(self.rhpkg_config)
            self.rhpkg_config_lst = self.rhpkg_config.split()
        else:
            self.rhpkg_config = ''

    def get_named_lock(self, absolute_path):
        with self.dir_locks['']:
            p = pathlib.Path(absolute_path).absolute()  # normalize (e.g. strip trailing /)
            if p in self.dir_locks:
                return self.dir_locks[p]
            else:
                new_lock = Lock()
                self.dir_locks[p] = new_lock
                return new_lock

    def get_group_config(self):
        # group.yml can contain a `vars` section which should be a
        # single level dict containing keys to str.format(**dict) replace
        # into the YAML content. If `vars` found, the format will be
        # preformed and the YAML model will reloaded from that result
        tmp_config = Model(self.gitdata.load_data(key='group').data)
        replace_vars = tmp_config.vars
        if replace_vars is not Missing:
            try:
                group_yml = yaml.safe_dump(tmp_config.primitive(), default_flow_style=False)
                self.raw_group_config = yaml.full_load(group_yml.format(**replace_vars))
                tmp_config = Model(dict(self.raw_group_config))
            except KeyError as e:
                raise ValueError('group.yml contains template key `{}` but no value was provided'.format(e.args[0]))

        return tmp_config

    def init_state(self):
        self.state_file = os.path.join(self.working_dir, 'state.yaml')
        self.state = dict(state.TEMPLATE_BASE_STATE)
        if os.path.isfile(self.state_file):
            with io.open(self.state_file, 'r', encoding='utf-8') as f:
                self.state = yaml.full_load(f)
            self.state.update(state.TEMPLATE_BASE_STATE)

    def save_state(self):
        with io.open(self.state_file, 'w', encoding='utf-8') as f:
            yaml.safe_dump(self.state, f, default_flow_style=False)

    def initialize(self, mode='images', clone_distgits=True,
                   validate_content_sets=False,
                   no_group=False, clone_source=True, disabled=None,
                   config_excludes=None,
                   upstream_commitish_overrides={}):

        if self.initialized:
            return

        if self.quiet and self.verbose:
            click.echo("Flags --quiet and --verbose are mutually exclusive")
            exit(1)

        # We could mark these as required and the click library would do this for us,
        # but this seems to prevent getting help from the various commands (unless you
        # specify the required parameters). This can probably be solved more cleanly, but TODO
        if not no_group and self.group is None:
            click.echo("Group must be specified")
            exit(1)

        if self.working_dir is None:
            self.working_dir = tempfile.mkdtemp(".tmp", "oit-")
            # This can be set to False by operations which want the working directory to be left around
            self.remove_tmp_working_dir = True
            atexit.register(remove_tmp_working_dir, self)
        else:
            self.working_dir = os.path.abspath(os.path.expanduser(self.working_dir))
            if not os.path.isdir(self.working_dir):
                os.makedirs(self.working_dir)

        self.distgits_dir = os.path.join(self.working_dir, "distgits")
        if not os.path.isdir(self.distgits_dir):
            os.mkdir(self.distgits_dir)

        self.distgits_diff_dir = os.path.join(self.working_dir, "distgits-diffs")
        if not os.path.isdir(self.distgits_diff_dir):
            os.mkdir(self.distgits_diff_dir)

        self.sources_dir = os.path.join(self.working_dir, "sources")
        if not os.path.isdir(self.sources_dir):
            os.mkdir(self.sources_dir)

        if disabled is not None:
            self.load_disabled = disabled

        self.initialize_logging()

        self.init_state()

        self.db = dblib.DB(self, self.datastore)
        self.logger.info(f'Initial execution (cwd) directory: {os.getcwd()}')

        if no_group:
            return  # nothing past here should be run without a group

        self.resolve_metadata()

        self.record_log_path = os.path.join(self.working_dir, "record.log")
        self.record_log = io.open(self.record_log_path, 'a', encoding='utf-8')
        atexit.register(close_file, self.record_log)

        # Directory where brew-logs will be downloaded after a build
        self.brew_logs_dir = os.path.join(self.working_dir, "brew-logs")
        if not os.path.isdir(self.brew_logs_dir):
            os.mkdir(self.brew_logs_dir)

        # Directory for flags between invocations in the same working-dir
        self.flags_dir = os.path.join(self.working_dir, "flags")
        if not os.path.isdir(self.flags_dir):
            os.mkdir(self.flags_dir)

        if self.cache_dir:
            self.cache_dir = os.path.abspath(self.cache_dir)

        self.group_dir = self.gitdata.data_dir
        self.group_config = self.get_group_config()

        # register the sources
        # For each "--source alias path" on the command line, register its existence with
        # the runtime.
        for r in self.source:
            self.register_source_alias(r[0], r[1])

        if self.sources:
            with io.open(self.sources, 'r', encoding='utf-8') as sf:
                source_dict = yaml.full_load(sf)
                if not isinstance(source_dict, dict):
                    raise ValueError('--sources param must be a yaml file containing a single dict.')
                for key, val in source_dict.items():
                    self.register_source_alias(key, val)

        with Dir(self.group_dir):

            # Flattens multiple comma/space delimited lists like [ 'x', 'y,z' ] into [ 'x', 'y', 'z' ]
            def flatten_list(names):
                if not names:
                    return []
                # split csv values
                result = []
                for n in names:
                    result.append([x for x in n.replace(' ', ',').split(',') if x != ''])
                # flatten result and remove dupes using set
                return list(set([y for x in result for y in x]))

            def filter_wip(n, d):
                return d.get('mode', 'enabled') in ['wip', 'enabled']

            def filter_enabled(n, d):
                return d.get('mode', 'enabled') == 'enabled'

            def filter_disabled(n, d):
                return d.get('mode', 'enabled') in ['enabled', 'disabled']

            cli_arches_override = flatten_list(self.arches)

            if cli_arches_override:  # Highest priority overrides on command line
                self.arches = cli_arches_override
            elif self.group_config.arches_override:  # Allow arches_override in group.yaml to temporarily override GA architectures
                self.arches = self.group_config.arches_override
            else:
                self.arches = self.group_config.get('arches', ['x86_64'])

            # If specified, signed repo files will be generated to enforce signature checks.
            self.gpgcheck = self.group_config.build_profiles.image.signed.gpgcheck
            if self.gpgcheck is Missing:
                # We should only really be building the latest release with unsigned RPMs, so default to True
                self.gpgcheck = True

            self.repos = Repos(self.group_config.repos, self.arches, self.gpgcheck)
            self.freeze_automation = self.group_config.freeze_automation or FREEZE_AUTOMATION_NO

            if validate_content_sets:
                self.repos.validate_content_sets()

            if self.group_config.name != self.group:
                raise IOError(
                    "Name in group.yml does not match group name. Someone may have copied this group without updating group.yml (make sure to check branch)")

            if self.branch is None:
                if self.group_config.branch is not Missing:
                    self.branch = self.group_config.branch
                    self.logger.info("Using branch from group.yml: %s" % self.branch)
                else:
                    self.logger.info("No branch specified either in group.yml or on the command line; all included images will need to specify their own.")
            else:
                self.logger.info("Using branch from command line: %s" % self.branch)

            scanner = self.group_config.image_build_log_scanner
            if scanner is not Missing:
                # compile regexen and fail early if they don't
                regexen = []
                for val in scanner.matches:
                    try:
                        regexen.append(re.compile(val))
                    except Exception as e:
                        raise ValueError(
                            "could not compile image build log regex for group:\n{}\n{}"
                            .format(val, e)
                        )
                scanner.matches = regexen

            exclude_keys = flatten_list(self.exclude)
            image_ex = list(exclude_keys)
            rpm_ex = list(exclude_keys)
            image_keys = flatten_list(self.images)

            self.upstream_commitish_overrides = upstream_commitish_overrides

            rpm_keys = flatten_list(self.rpms)

            filter_func = None
            if self.load_wip and self.load_disabled:
                pass  # use no filter, load all
            elif self.load_wip:
                filter_func = filter_wip
            elif self.load_disabled:
                filter_func = filter_disabled
            else:
                filter_func = filter_enabled

            replace_vars = {}
            if self.group_config.vars:
                replace_vars = self.group_config.vars.primitive()

            if config_excludes:
                excludes = self.group_config.get(config_excludes, {})
                image_ex.extend(excludes.get('images', []))
                rpm_ex.extend(excludes.get('rpms', []))

            # pre-load the image data to get the names for all images
            # eventually we can use this to allow loading images by
            # name or distgit. For now this is used elsewhere
            image_name_data = self.gitdata.load_data(path='images')

            for img in image_name_data.values():
                name = img.data.get('name')
                short_name = name.split('/')[1]
                self.image_name_map[name] = img.key
                self.image_name_map[short_name] = img.key

            image_data = self.gitdata.load_data(path='images', keys=image_keys,
                                                exclude=image_ex,
                                                replace_vars=replace_vars,
                                                filter_funcs=None if len(image_keys) else filter_func)

            try:
                rpm_data = self.gitdata.load_data(path='rpms', keys=rpm_keys,
                                                  exclude=rpm_ex,
                                                  replace_vars=replace_vars,
                                                  filter_funcs=None if len(rpm_keys) else filter_func)
            except gitdata.GitDataPathException:
                # some older versions have no RPMs, that's ok.
                rpm_data = {}

            missed_include = set(image_keys + rpm_keys) - set(list(image_data.keys()) + list(rpm_data.keys()))
            if len(missed_include) > 0:
                raise DoozerFatalError('The following images or rpms were either missing or filtered out: {}'.format(', '.join(missed_include)))

            if mode in ['images', 'both']:
                for i in image_data.values():
                    metadata = ImageMetadata(self, i, self.upstream_commitish_overrides.get(i.key))
                    self.image_map[metadata.distgit_key] = metadata
                if not self.image_map:
                    self.logger.warning("No image metadata directories found for given options within: {}".format(self.group_dir))

                for image in self.image_map.values():
                    image.resolve_parent()

                # now that ancestry is defined, make sure no cyclic dependencies
                for image in self.image_map.values():
                    for child in image.children:
                        if image.is_ancestor(child):
                            raise DoozerFatalError('{} cannot be both a parent and dependent of {}'.format(child.distgit_key, image.distgit_key))

                self.generate_image_tree()

            if mode in ['rpms', 'both']:
                for r in rpm_data.values():
                    metadata = RPMMetadata(self, r, clone_source=clone_source)
                    self.rpm_map[metadata.distgit_key] = metadata
                if not self.rpm_map:
                    self.logger.warning("No rpm metadata directories found for given options within: {}".format(self.group_dir))

        # Make sure that the metadata is not asking us to check out the same exact distgit & branch.
        # This would almost always indicate someone has checked in duplicate metadata into a group.
        no_collide_check = {}
        for meta in list(self.rpm_map.values()) + list(self.image_map.values()):
            key = '{}/{}/#{}'.format(meta.namespace, meta.name, meta.branch())
            if key in no_collide_check:
                raise IOError('Complete duplicate distgit & branch; something wrong with metadata: {} from {} and {}'.format(key, meta.config_filename, no_collide_check[key].config_filename))
            no_collide_check[key] = meta

        # Read in the streams definite for this group if one exists
        streams = self.gitdata.load_data(key='streams')
        if streams:
            self.streams = Model(self.gitdata.load_data(key='streams', replace_vars=replace_vars).data)

        if clone_distgits:
            self.clone_distgits()

        self.initialized = True

    def initialize_logging(self):

        if self.initialized:
            return

        # Three flags control the output modes of the command:
        # --verbose prints logs to CLI as well as to files
        # --debug increases the log level to produce more detailed internal
        #         behavior logging
        # --quiet opposes both verbose and debug
        if self.debug:
            log_level = logging.DEBUG
        elif self.quiet:
            log_level = logging.WARN
        else:
            log_level = logging.INFO

        default_log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

        root_logger = logging.getLogger()
        root_logger.setLevel(logging.WARN)
        root_stream_handler = logging.StreamHandler()
        root_stream_handler.setFormatter(default_log_formatter)
        root_logger.addHandler(root_stream_handler)

        # If in debug mode, let all modules log
        if not self.debug:
            # Otherwise, only allow children of ocp to log
            root_logger.addFilter(logging.Filter("ocp"))

        # Get a reference to the logger for doozer
        self.logger = logutil.getLogger()
        self.logger.propagate = False

        # levels will be set at the handler level. Make sure master level is low.
        self.logger.setLevel(logging.DEBUG)

        main_stream_handler = logging.StreamHandler()
        main_stream_handler.setFormatter(default_log_formatter)
        main_stream_handler.setLevel(log_level)
        self.logger.addHandler(main_stream_handler)

        self.debug_log_path = os.path.join(self.working_dir, "debug.log")
        debug_log_handler = logging.FileHandler(self.debug_log_path)
        # Add thread information for debug log
        debug_log_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s (%(thread)d) %(message)s'))
        debug_log_handler.setLevel(logging.DEBUG)
        self.logger.addHandler(debug_log_handler)

    def build_retrying_koji_client(self):
        """
        :return: Returns a new koji client instance that will automatically retry
        methods when it receives common exceptions (e.g. Connection Reset)
        """
        return brew.KojiWrapper(koji.ClientSession(self.group_config.urls.brewhub))

    @contextmanager
    def shared_koji_client_session(self):
        """
        Context manager which offers a shared koji client session. You hold a koji specific lock in this context
        manager giving your thread exclusive access. The lock is reentrant, so don't worry about
        call a method that acquires the same lock while you hold it.
        """
        with self.koji_lock:
            if self._koji_client_session is None:
                self._koji_client_session = self.build_retrying_koji_client()
            yield self._koji_client_session

    @contextmanager
    def pooled_koji_client_session(self):
        """
        Context manager which offers a koji client session from a limited pool. You hold a lock on this
        session until you return. It is not recommended to call other methods that acquire their
        own pooled sessions, because that may lead to deadlock if the pool is exhausted.
        """
        session = None
        session_id = None
        while True:
            with self.mutex:
                if len(self.session_pool_available) == 0:
                    if len(self.session_pool) < 30:
                        # pool has not grown to max size;
                        new_session = self.build_retrying_koji_client()
                        session_id = len(self.session_pool)
                        self.session_pool[session_id] = new_session
                        session = new_session  # This is what we wil hand to the caller
                        break
                    else:
                        # Caller is just going to have to wait and try again
                        pass
                else:
                    session_id, session = self.session_pool_available.popitem()
                    break

            time.sleep(5)

        # Arriving here, we have a session to use.
        try:
            yield session
        finally:
            # Put it back into the pool
            with self.mutex:
                self.session_pool_available[session_id] = session

    @staticmethod
    def timestamp():
        return datetime.datetime.utcnow().isoformat()

    def assert_mutation_is_permitted(self):
        """
        In group.yml, it is possible to instruct doozer to prevent all builds / mutation of distgits.
        Call this method if you are about to mutate anything. If builds are disabled, an exception will
        be thrown.
        """
        if self.freeze_automation == FREEZE_AUTOMATION_YES:
            raise DoozerFatalError('Automation (builds / mutations) for this group is currently frozen (freeze_automation set to {}). Coordinate with the group owner to change this if you believe it is incorrect.'.format(FREEZE_AUTOMATION_YES))

    def image_metas(self):
        return list(self.image_map.values())

    def ordered_image_metas(self):
        return [self.image_map[dg] for dg in self.image_order]

    def get_global_arches(self):
        """
        :return: Returns a list of architectures that are enabled globally in group.yml.
        """
        return list(self.arches)

    def filter_failed_image_trees(self, failed):
        for i in self.ordered_image_metas():
            if i.parent and i.parent.distgit_key in failed:
                failed.append(i.distgit_key)

        for f in failed:
            if f in self.image_map:
                del self.image_map[f]

        # regen order and tree
        self.generate_image_tree()

        return failed

    def generate_image_tree(self):
        self.image_tree = {}
        image_lists = {0: []}

        def add_child_branch(child, branch, level=1):
            if level not in image_lists:
                image_lists[level] = []
            for sub_child in child.children:
                if sub_child.distgit_key not in self.image_map:
                    continue  # don't add images that have been filtered out
                branch[sub_child.distgit_key] = {}
                image_lists[level].append(sub_child.distgit_key)
                add_child_branch(sub_child, branch[sub_child.distgit_key], level + 1)

        for image in self.image_map.values():
            if not image.parent:
                self.image_tree[image.distgit_key] = {}
                image_lists[0].append(image.distgit_key)
                add_child_branch(image, self.image_tree[image.distgit_key])

        levels = list(image_lists.keys())
        levels.sort()
        self.image_order = []
        for level in levels:
            for i in image_lists[level]:
                if i not in self.image_order:
                    self.image_order.append(i)

    def image_distgit_by_name(self, name):
        """Returns image meta but full name, short name, or distgit"""
        return self.image_name_map.get(name, None)

    def rpm_metas(self):
        return list(self.rpm_map.values())

    def all_metas(self):
        return self.image_metas() + self.rpm_metas()

    def register_source_alias(self, alias, path):
        self.logger.info("Registering source alias %s: %s" % (alias, path))
        path = os.path.abspath(path)
        assertion.isdir(path, "Error registering source alias %s" % alias)
        with Dir(path):
            url = None
            origin_url = "?"
            rc1, out_origin, err_origin = exectools.cmd_gather(
                ["git", "config", "--get", "remote.origin.url"])
            if rc1 == 0:
                url = out_origin.strip()
                origin_url = url
                # Usually something like "git@github.com:openshift/origin.git"
                # But we want an https hyperlink like http://github.com/openshift/origin
                if origin_url.startswith("git@"):
                    origin_url = origin_url[4:]  # remove git@
                    origin_url = origin_url.replace(":", "/", 1)  # replace first colon with /

                    if origin_url.endswith(".git"):
                        origin_url = origin_url[:-4]  # remove .git

                    origin_url = "https://%s" % origin_url
            else:
                self.logger.error("Failed acquiring origin url for source alias %s: %s" % (alias, err_origin))

            branch = None
            rc2, out_branch, err_branch = exectools.cmd_gather(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"])
            if rc2 == 0:
                branch = out_branch.strip()
            else:
                self.logger.error("Failed acquiring origin branch for source alias %s: %s" % (alias, err_branch))

            if self.group_config.public_upstreams:
                if not (url and branch):
                    raise DoozerFatalError(f"Couldn't detect source URL or branch for local source {path}. Is it a valid Git repo?")
                public_upstream_url, public_upstream_branch = self.get_public_upstream(url)
                self.source_resolutions[alias] = SourceResolution(path, url, branch, public_upstream_url, public_upstream_branch or branch)
            else:
                self.source_resolutions[alias] = SourceResolution(path, url, branch, None, None)

            if 'source_alias' not in self.state:
                self.state['source_alias'] = {}
            self.state['source_alias'][alias] = {
                'url': origin_url,
                'branch': branch or '?',
                'path': path
            }
            self.add_record("source_alias", alias=alias, origin_url=origin_url, branch=branch or '?', path=path)

    def register_stream_alias(self, alias, image):
        self.logger.info("Registering image stream alias override %s: %s" % (alias, image))
        self.stream_alias_overrides[alias] = image

    @property
    def remove_tmp_working_dir(self):
        """
        Provides thread safe method of checking whether runtime should clean up the working directory.
        :return: Returns True if the directory should be deleted
        """
        with self.log_lock:
            return self._remove_tmp_working_dir

    @remove_tmp_working_dir.setter
    def remove_tmp_working_dir(self, remove):
        """
        Provides thread safe method of setting whether runtime should clean up the working directory.
        :param remove: True if the directory should be removed. Only the last value set impacts the decision.
        """
        with self.log_lock:
            self._remove_tmp_working_dir = remove

    def add_record(self, record_type, **kwargs):
        """
        Records an action taken by oit that needs to be communicated to outside
        systems. For example, the update a Dockerfile which needs to be
        reviewed by an owner. Each record is encoded on a single line in the
        record.log. Records cannot contain line feeds -- if you need to
        communicate multi-line data, create a record with a path to a file in
        the working directory.

        :param record_type: The type of record to create.
        :param kwargs: key/value pairs

        A record line is designed to be easily parsed and formatted as:
        record_type|key1=value1|key2=value2|...|
        """

        # Multiple image build processes could be calling us with action simultaneously, so
        # synchronize output to the file.
        with self.log_lock:
            record = "%s|" % record_type
            for k, v in kwargs.items():
                assert ("\n" not in str(k))
                # Make sure the values have no linefeeds as this would interfere with simple parsing.
                v = str(v).replace("\n", " ;;; ").replace("\r", "")
                record += "%s=%s|" % (k, v)

            # Add the record to the file
            self.record_log.write("%s\n" % record)
            self.record_log.flush()

    def add_distgits_diff(self, distgit, diff):
        """
        Records the diff of changes applied to a distgit repo.
        """

        with io.open(os.path.join(self.distgits_diff_dir, distgit + '.patch'), 'w', encoding='utf-8') as f:
            f.write(diff)

    def resolve_image(self, distgit_name, required=True):
        if distgit_name not in self.image_map:
            if not required:
                return None
            raise DoozerFatalError("Unable to find image metadata in group / included images: %s" % distgit_name)
        return self.image_map[distgit_name]

    def late_resolve_image(self, distgit_name, add=False):
        """Resolve image and retrieve meta, optionally adding to image_map.
        If image not found, error will be thrown"""

        if distgit_name in self.image_map:
            return self.image_map[distgit_name]

        replace_vars = {}
        if self.group_config.vars:
            replace_vars = self.group_config.vars.primitive()
        data_obj = self.gitdata.load_data(path='images', key=distgit_name, replace_vars=replace_vars)
        if not data_obj:
            raise DoozerFatalError('Unable to resolve image metadata for {}'.format(distgit_name))

        meta = ImageMetadata(self, data_obj, self.upstream_commitish_overrides.get(data_obj.key))
        if add:
            self.image_map[distgit_name] = meta
        return meta

    def resolve_brew_image_url(self, image_name_and_version):
        """
        :param image_name_and_version: The image name to resolve. The image can contain a version tag or sha.
        :return: Returns the pullspec of this image in brew.
        e.g. "openshift/jenkins:5"  => "registry-proxy.engineering.redhat.com/rh-osbs/openshift-jenkins:5"
        """

        if self.group_config.urls.brew_image_namespace is not Missing:
            # if there is a namespace, we need to flatten the image name.
            # e.g. openshift/image:latest => openshift-image:latest
            # ref: https://mojo.redhat.com/docs/DOC-1204856
            url = self.group_config.urls.brew_image_host
            ns = self.group_config.urls.brew_image_namespace
            name = image_name_and_version.replace('/', '-')
            url = "/".join((url, ns, name))
        else:
            # If there is no namespace, just add the image name to the brew image host
            url = "/".join((self.group_config.urls.brew_image_host, image_name_and_version))

        if ':' not in url.split('/')[-1]:
            # oc image info will return information about all tagged images. So be explicit
            # in indicating :latest if there is no tag.
            url += ':latest'

        return url

    def resolve_stream(self, stream_name):
        """
        :param stream_name: The name of the stream to resolve.
        :return: Resolves and returns the image stream name into its literal value.
                This is usually a lookup in streams.yaml, but can also be overridden on the command line. If
                the stream_name cannot be resolved, an exception is thrown.
        """

        # If the stream has an override from the command line, return it.
        if stream_name in self.stream_alias_overrides:
            return self.stream_alias_overrides[stream_name]

        if stream_name not in self.streams:
            raise IOError("Unable to find definition for stream: %s" % stream_name)

        return self.streams[stream_name]

    def get_stream_names(self):
        """
        :return: Returns a list of all streams defined in streams.yaml.
        """
        return list(self.streams.keys())

    def get_public_upstream(self, remote_git: str) -> (str, Optional[str]):
        """
        Some upstream repo are private in order to allow CVE workflows. While we
        may want to build from a private upstream, we don't necessarily want to confuse
        end-users by referencing it in our public facing image labels / etc.
        In group.yaml, you can specify a mapping in "public_upstreams". It
        represents private_url_prefix => public_url_prefix. Remote URLs passed to this
        method which contain one of the private url prefixes will be translated
        into a new string with the public prefix in its place. If there is not
        applicable mapping, the incoming url will still be normalized into https.
        :param remote_git: The URL to analyze for private repo patterns.
        :return: tuple (url, branch)
            - url: An https normalized remote address with private repo information replaced.
            - branch: Optional public branch name if the public upstream source use a different branch name from the private upstream.
        """
        remote_https = util.convert_remote_git_to_https(remote_git)

        if self.group_config.public_upstreams:

            # We prefer the longest match in the mapping, so iterate through the entire
            # map and keep track of the longest matching private remote.
            target_priv_prefix = None
            target_pub_prefix = None
            target_pub_branch = None
            for upstream in self.group_config.public_upstreams:
                priv = upstream["private"]
                pub = upstream["public"]
                # priv can be a full repo, or an organization (e.g. git@github.com:openshift)
                # It will be treated as a prefix to be replaced
                https_priv_prefix = util.convert_remote_git_to_https(priv)  # Normalize whatever is specified in group.yaml
                https_pub_prefix = util.convert_remote_git_to_https(pub)
                if remote_https.startswith(f'{https_priv_prefix}/') or remote_https == https_priv_prefix:
                    # If we have not set the prefix yet, or if it is longer than the current contender
                    if not target_priv_prefix or len(https_priv_prefix) > len(target_pub_prefix):
                        target_priv_prefix = https_priv_prefix
                        target_pub_prefix = https_pub_prefix
                        target_pub_branch = upstream.get("public_branch")

            if target_priv_prefix:
                return (f'{target_pub_prefix}{remote_https[len(target_priv_prefix):]}', target_pub_branch)

        return (remote_https, None)

    def git_clone(self, remote_url, target_dir, gitargs=None, set_env=None, timeout=0):
        gitargs = gitargs or []
        set_env = set_env or []

        if self.cache_dir:
            git_cache_dir = os.path.join(self.cache_dir, self.user or "default", 'git')
            util.mkdirs(git_cache_dir)
            normalized_url = util.convert_remote_git_to_https(remote_url)
            # Strip special chars out of normalized url to create a human friendly, but unique filename
            file_friendly_url = normalized_url.split('//')[-1].replace('/', '_')
            repo_dir = os.path.join(git_cache_dir, file_friendly_url)
            self.logger.info(f'Cache for {remote_url} going to {repo_dir}')

            if not os.path.exists(repo_dir):
                self.logger.info(f'Initializing cache directory for git remote: {remote_url}')

                # If the cache directory for this repo does not exist yet, we will create one.
                # But we must do so carefully to minimize races with any other doozer instance
                # running on the machine.
                with self.get_named_lock(repo_dir):  # also make sure we cooperate with other threads in this process.
                    tmp_repo_dir = tempfile.mkdtemp(dir=git_cache_dir)
                    exectools.cmd_assert(f'git init --bare {tmp_repo_dir}')
                    with Dir(tmp_repo_dir):
                        exectools.cmd_assert(f'git remote add origin {remote_url}')

                    try:
                        os.rename(tmp_repo_dir, repo_dir)
                    except:
                        # There are two categories of failure
                        # 1. Another doozer instance already created the directory, in which case we are good to go.
                        # 2. Something unexpected is preventing the rename.
                        if not os.path.exists(repo_dir):
                            # Not sure why the rename failed. Raise to user.
                            raise

            # If we get here, we have a bare repo with a remote set
            # Pull content to update the cache. This should be safe for multiple doozer instances to perform.
            self.logger.info(f'Updating cache directory for git remote: {remote_url}')
            # Fire and forget this fetch -- just used to keep cache as fresh as possible
            exectools.fire_and_forget(repo_dir, 'git fetch --all')
            gitargs.extend(['--dissociate', '--reference-if-able', repo_dir])

        self.logger.info(f'Cloning to: {target_dir}')

        # Perform the clone (including --reference args if cache_dir was set)
        cmd = []
        if timeout:
            cmd.extend(['timeout', f'{timeout}'])
        cmd.extend(['git', 'clone', remote_url])
        cmd.extend(gitargs)
        cmd.append(target_dir)
        exectools.cmd_assert(cmd, retries=3, on_retry=["rm", "-rf", target_dir], set_env=set_env)

    def resolve_source(self, meta):
        """
        Looks up a source alias and returns a path to the directory containing
        that source. Sources can be specified on the command line, or, failing
        that, in group.yml.
        If a source specified in group.yaml has not be resolved before,
        this method will clone that source to checkout the group's desired
        branch before returning a path to the cloned repo.
        :param meta: The MetaData object to resolve source for
        :return: Returns the source path or None if upstream source is not defined
        """
        source = meta.config.content.source

        if not source:
            return None

        parent = f'{meta.namespace}_{meta.name}'

        # This allows passing `--source <distgit_key> path` to
        # override any source to something local without it
        # having been configured for an alias
        if self.local and meta.distgit_key in self.source_resolutions:
            source['alias'] = meta.distgit_key
            if 'git' in source:
                del source['git']

        source_details = None
        if 'git' in source:
            git_url = urllib.parse.urlparse(source.git.url)
            name = os.path.splitext(os.path.basename(git_url.path))[0]
            alias = '{}_{}'.format(parent, name)
            source_details = dict(source.git)
        elif 'alias' in source:
            alias = source.alias
        else:
            return None

        self.logger.debug("Resolving local source directory for alias {}".format(alias))
        if alias in self.source_resolutions:
            path, _, _, meta.public_upstream_url, meta.public_upstream_branch = self.source_resolutions[alias]
            self.logger.debug("returning previously resolved path for alias {}: {}".format(alias, path))
            return path

        # Where the source will land, check early so we know if old or new style
        sub_path = '{}{}'.format('global_' if source_details is None else '', alias)
        source_dir = os.path.join(self.sources_dir, sub_path)

        if not source_details:  # old style alias was given
            if self.group_config.sources is Missing or alias not in self.group_config.sources:
                raise DoozerFatalError("Source alias not found in specified sources or in the current group: %s" % alias)
            source_details = self.group_config.sources[alias]

        self.logger.debug("checking for source directory in source_dir: {}".format(source_dir))

        with self.get_named_lock(source_dir):
            if alias in self.source_resolutions:  # we checked before, but check again inside the lock
                path, _, _, meta.public_upstream_url, meta.public_upstream_branch = self.source_resolutions[alias]
                self.logger.debug("returning previously resolved path for alias {}: {}".format(alias, path))
                return path

            # If this source has already been extracted for this working directory
            if os.path.isdir(source_dir):
                # Store so that the next attempt to resolve the source hits the map
                self.register_source_alias(alias, source_dir)
                if self.group_config.public_upstreams:
                    _, _, _, meta.public_upstream_url, meta.public_upstream_branch = self.source_resolutions[alias]
                self.logger.info("Source '{}' already exists in (skipping clone): {}".format(alias, source_dir))
                return source_dir

            url = source_details["url"]
            clone_branch, _ = self.detect_remote_source_branch(source_details)
            if self.group_config.public_upstreams:
                meta.public_upstream_url, meta.public_upstream_branch = self.get_public_upstream(url)
                if not meta.public_upstream_branch:  # default to the same branch name as private upstream
                    meta.public_upstream_branch = clone_branch

            self.logger.info("Attempting to checkout source '%s' branch %s in: %s" % (url, clone_branch, source_dir))
            try:
                self.logger.info("Attempting to checkout source '%s' branch %s in: %s" % (url, clone_branch, source_dir))
                # clone all branches as we must sometimes reference master /OWNERS for maintainer information
                gitargs = [
                    '--no-single-branch', '--branch', clone_branch
                ]
                if not self.group_config.public_upstreams:
                    # Do a shallow clone only if public upstreams are not specified
                    gitargs.extend(['--depth', '1'])
                self.git_clone(url, source_dir, gitargs=gitargs, set_env=constants.GIT_NO_PROMPTS)
                if meta.commitish:
                    # Commit-ish may not be in the history because of shallow clone. Fetch from upstream if not existing.
                    self.logger.info(f"Determining if commit-ish {meta.commitish} exists")
                    cmd = ["git", "-C", source_dir, "branch", "--contains", meta.commitish]
                    rc, _, _ = exectools.cmd_gather(cmd)
                    if rc != 0:  # commit-ish does not exist
                        self.logger.info(f"Fetching commit-ish {meta.commitish} from upstream")
                        cmd = ["git", "-C", source_dir, "fetch", "origin", "--depth", "1", meta.commitish]
                        exectools.cmd_assert(cmd)
                # fetch public upstream source
                if meta.public_upstream_branch:
                    util.setup_and_fetch_public_upstream_source(meta.public_upstream_url, meta.public_upstream_branch, source_dir)

            except IOError as e:
                self.logger.info("Unable to checkout branch {}: {}".format(clone_branch, str(e)))
                shutil.rmtree(source_dir)
                raise DoozerFatalError("Error checking out target branch of source '%s' in: %s" % (alias, source_dir))

            # Store so that the next attempt to resolve the source hits the map
            self.register_source_alias(alias, source_dir)
            return source_dir

    def detect_remote_source_branch(self, source_details):
        """Find a configured source branch that exists, or raise DoozerFatalError. Returns branch name and git hash"""
        git_url = source_details["url"]
        branches = source_details["branch"]

        branch = branches["target"]
        fallback_branch = branches.get("fallback", None)
        if self.group_config.use_source_fallback_branch == "always" and fallback_branch:
            # only use the fallback (unless none is given)
            branch, fallback_branch = fallback_branch, None
        elif self.group_config.use_source_fallback_branch == "never":
            # ignore the fallback
            fallback_branch = None
        stage_branch = branches.get("stage", None) if self.stage else None

        if stage_branch:
            self.logger.info('Normal branch overridden by --stage option, using "{}"'.format(stage_branch))
            result = self._get_remote_branch_ref(git_url, stage_branch)
            if result:
                return stage_branch, result
            raise DoozerFatalError('--stage option specified and no stage branch named "{}" exists for {}'.format(stage_branch, git_url))

        result = self._get_remote_branch_ref(git_url, branch)
        if result:
            return branch, result
        elif not fallback_branch:
            raise DoozerFatalError('Requested target branch {} does not exist and no fallback provided'.format(branch))

        self.logger.info('Target branch does not exist in {}, checking fallback branch {}'.format(git_url, fallback_branch))
        result = self._get_remote_branch_ref(git_url, fallback_branch)
        if result:
            return fallback_branch, result
        raise DoozerFatalError('Requested fallback branch {} does not exist'.format(branch))

    def _get_remote_branch_ref(self, git_url, branch):
        """Detect whether a single branch exists on a remote repo; returns git hash if found"""
        self.logger.info('Checking if target branch {} exists in {}'.format(branch, git_url))
        try:
            out, _ = exectools.cmd_assert('git ls-remote --heads {} {}'.format(git_url, branch), retries=3)
        except Exception as err:
            self.logger.error('Unable to check if target branch {} exists: {}'.format(branch, err))
            return None
        result = out.strip()  # any result means the branch is found
        return result.split()[0] if result else None

    def resolve_source_head(self, meta):
        """
        Attempts to resolve the branch a given source alias has checked out. If not on a branch
        returns SHA of head.
        :param meta: The MetaData object to resolve source for
        :return: The name of the checked out branch or None (if required=False)
        """
        source_dir = self.resolve_source(meta)

        if not source_dir:
            return None

        with io.open(os.path.join(source_dir, '.git/HEAD'), encoding="utf-8") as f:
            head_content = f.read().strip()
            # This will either be:
            # a SHA like: "52edbcd8945af0dc728ad20f53dcd78c7478e8c2"
            # a local branch name like: "ref: refs/heads/master"
            if head_content.startswith("ref:"):
                return head_content.split('/', 2)[2]  # limit split in case branch name contains /

            # Otherwise, just return SHA
            return head_content

    def export_sources(self, output):
        self.logger.info('Writing sources to {}'.format(output))
        with io.open(output, 'w', encoding='utf-8') as sources_file:
            yaml.dump({k: v.path for k, v in self.source_resolutions.items()}, sources_file, default_flow_style=False)

    def auto_version(self, repo_type):
        """
        Find and return the version of the atomic-openshift package in the OCP
        RPM repository.

        This repository is the primary input for OCP images.  The group_config
        for a group specifies the location for both signed and unsigned
        rpms.  The caller must indicate which to use.
        """

        repo_url = self.repos['rhel-server-ose-rpms'].baseurl(repo_type, 'x86_64')
        self.logger.info(
            "Getting version from atomic-openshift package in {}".format(
                repo_url)
        )

        # create a randomish repo name to avoid erroneous cache hits
        repoid = "oit" + datetime.datetime.now().strftime("%s")
        version_query = ["/usr/bin/repoquery", "--quiet", "--tempcache",
                         "--repoid", repoid,
                         "--repofrompath", repoid + "," + repo_url,
                         "--queryformat", "%{VERSION}",
                         "atomic-openshift"]
        rc, auto_version, err = exectools.cmd_gather(version_query)
        if rc != 0:
            raise RuntimeError(
                "Unable to get OCP version from RPM repository: {}".format(err)
            )

        version = "v" + auto_version.strip()

        self.logger.info("Auto-detected OCP version: {}".format(version))
        return version

    def valid_version(self, version):
        """
        Check if a version string matches an accepted pattern.
        A single lower-case 'v' followed by one or more decimal numbers,
        separated by a dot.  Examples below are not exhaustive
        Valid:
          v1, v12, v3.4, v2.12.0

        Not Valid:
          1, v1..2, av3.4, .v12  .99.12, v13-55
        """
        return re.match(r"^v\d+((\.\d+)+)?$", version) is not None

    @classmethod
    def _parallel_exec(cls, f, args, n_threads, timeout=None):
        pool = ThreadPool(n_threads)
        ret = pool.map_async(wrap_exception(f), args)
        pool.close()
        if timeout is None:
            # If a timeout is not specified, the KeyboardInterrupt exception won't be delivered.
            # Use polling as a workaround. See https://stackoverflow.com/questions/1408356/keyboard-interrupts-with-pythons-multiprocessing-pool.
            while not ret.ready():
                ret.wait(60)
        return ret.get(timeout)

    def clone_distgits(self, n_threads=None):
        with util.timer(self.logger.info, 'Full runtime clone'):
            if n_threads is None:
                n_threads = self.global_opts['distgit_threads']
            return self._parallel_exec(
                lambda m: m.distgit_repo(),
                self.all_metas(),
                n_threads=n_threads)

    def push_distgits(self, n_threads=None):
        self.assert_mutation_is_permitted()

        if n_threads is None:
            n_threads = self.global_opts['distgit_threads']
        return self._parallel_exec(
            lambda m: m.distgit_repo().push(),
            self.all_metas(),
            n_threads=n_threads)

    def parallel_exec(self, f, args, n_threads=None):
        """
        :param f: A function to invoke for all arguments
        :param args: A list of argument tuples. Each tuple will be used to invoke the function once.
        :param n_threads: preferred number of threads to use during the work
        :return:
        """
        n_threads = n_threads if n_threads is not None else len(args)
        terminate_event = threading.Event()
        pool = ThreadPool(n_threads)
        # Python 3 doesn't allow to unpack tuple argument in a lambdas or functions (PEP-3113).
        # `_unpack_tuple_args` is a workaround that unpacks the tuple as arguments for the function passed to `ThreadPool.map_async`.
        # `starmap_async` can be used in the future when we don't keep compatibility with Python 2.
        ret = pool.map_async(
            wrap_exception(_unpack_tuple_args(f)),
            [(a, terminate_event) for a in args])
        pool.close()
        try:
            # `wait` without a timeout disables signal handling
            while not ret.ready():
                ret.wait(60)
        except KeyboardInterrupt:
            self.logger.warn('SIGINT received, signaling threads to terminate...')
            terminate_event.set()
        pool.join()
        return ret

    def get_default_brew_tag(self):
        return self.branch

    def get_default_candidate_brew_tag(self):
        return self.branch + '-candidate' if self.branch else None

    def builds_for_group_branch(self):
        # return a dict of all the latest builds for this group, according to
        # the branch's candidate tag in brew. each entry is name => tuple(version, release).
        tag = self.get_default_candidate_brew_tag()
        output, _ = exectools.cmd_assert(
            "brew list-tagged --quiet --latest {}".format(tag),
            retries=3,
        )
        builds = [
            # each line like "build tag owner" split into build NVR
            line.split()[0].rsplit("-", 2)
            for line in output.strip().split("\n")
            if line.strip()
        ]
        return {n: (v, r) for n, v, r in builds}

    def scan_distgit_sources(self):
        return self.parallel_exec(
            lambda meta, _: (meta, meta.needs_rebuild()),
            self.image_metas() + self.rpm_metas(),
            n_threads=100,
        ).get()

    def resolve_metadata(self):
        """
        The group control data can be on a local filesystem, in a git
        repository that can be checked out, or some day in a database

        If the scheme is empty, assume file:///...
        Allow http, https, ssh and ssh+git (all valid git clone URLs)
        """

        if self.data_path is None:
            raise DoozerFatalError(
                ("No metadata path provided. Must be set via one of:\n"
                 "* data_path key in {}\n"
                 "* doozer --data-path [PATH|URL]\n"
                 "* Environment variable DOOZER_DATA_PATH\n"
                 ).format(self.cfg_obj.full_path))

        self.gitdata = gitdata.GitData(data_path=self.data_path, clone_dir=self.working_dir,
                                       branch=self.group, logger=self.logger)
        self.data_dir = self.gitdata.data_dir
