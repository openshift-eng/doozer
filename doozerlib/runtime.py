from multiprocessing import Lock
from multiprocessing.dummy import Pool as ThreadPool
from pykwalify.core import Core
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
import urlparse

import gitdata

import logutil
import assertion
import exectools
from pushd import Dir

from image import ImageMetadata
from rpmcfg import RPMMetadata
from doozerlib import state
from model import Model, Missing
from multiprocessing import Lock
from repos import Repos
import brew
from exceptions import DoozerFatalError


# Registered atexit to close out debug/record logs
def close_file(f):
    f.close()


# Iterates through a list of strings, detecting if any entries have a
# comma delimited entry. If an entry contains a comma, it is split into
# multiple entries.
# The extended list is returned.
def flatten_comma_delimited_entries(l):
    nl = []
    for e in l:
        nl.extend(e.split(","))
    return nl


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

    def __str__(self):
        return "{}\nOriginal traceback:\n{}".format(
            Exception.__str__(self), self.formatted)


def wrap_exception(func):
    """ Decorate a function, wrap exception if it occurs. """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            raise WrapException()
    return wrapper

# ============================================================================
# Runtime object definition
# ============================================================================


class Runtime(object):
    # Use any time it is necessary to synchronize feedback from multiple threads.
    mutex = Lock()

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

        for key, val in kwargs.items():
            self.__dict__[key] = val

        if self.latest_parent_version:
            self.ignore_missing_base = True

        self._remove_tmp_working_dir = False
        self.group_config = None

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

        # Map of source code repo aliases (e.g. "ose") to a path on the filesystem where it has been cloned.
        # See registry_repo.
        self.source_paths = {}

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
            with open(self.state_file, 'r') as f:
                self.state = yaml.full_load(f)
            self.state.update(state.TEMPLATE_BASE_STATE)

    def save_state(self):
        with open(self.state_file, 'w') as f:
            yaml.safe_dump(self.state, f, default_flow_style=False)

    def initialize(self, mode='images', clone_distgits=True,
                   validate_content_sets=False,
                   no_group=False, clone_source=True, disabled=None,
                   config_excludes=None):

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

        if no_group:
            return  # nothing past here should be run without a group

        self.resolve_metadata()

        self.record_log_path = os.path.join(self.working_dir, "record.log")
        self.record_log = open(self.record_log_path, 'a')
        atexit.register(close_file, self.record_log)

        # Directory where brew-logs will be downloaded after a build
        self.brew_logs_dir = os.path.join(self.working_dir, "brew-logs")
        if not os.path.isdir(self.brew_logs_dir):
            os.mkdir(self.brew_logs_dir)

        # Directory for flags between invocations in the same working-dir
        self.flags_dir = os.path.join(self.working_dir, "flags")
        if not os.path.isdir(self.flags_dir):
            os.mkdir(self.flags_dir)

        self.group_dir = self.gitdata.data_dir

        # register the sources
        # For each "--source alias path" on the command line, register its existence with
        # the runtime.
        for r in self.source:
            self.register_source_alias(r[0], r[1])

        if self.sources:
            with open(self.sources, 'r') as sf:
                source_dict = yaml.full_load(sf)
                if not isinstance(source_dict, dict):
                    raise ValueError('--sources param must be a yaml file containing a single dict.')
                for key, val in source_dict.items():
                    self.register_source_alias(key, val)

        with Dir(self.group_dir):
            self.group_config = self.get_group_config()
            self.arches = self.group_config.get('arches', ['x86_64'])
            self.repos = Repos(self.group_config.repos, self.arches)

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

            # Flattens a list like like [ 'x', 'y,z' ] into [ 'x.yml', 'y.yml', 'z.yml' ]
            # for later checking we need to remove from the lists, but they are tuples. Clone to list
            def flatten_list(names):
                if not names:
                    return []
                # split csv values
                result = []
                for n in names:
                    result.append([x for x in n.replace(' ', ',').split(',') if x != ''])
                # flatten result and remove dupes
                return list(set([y for x in result for y in x]))

            def filter_wip(n, d):
                return d.get('mode', 'enabled') in ['wip', 'enabled']

            def filter_enabled(n, d):
                return d.get('mode', 'enabled') == 'enabled'

            def filter_disabled(n, d):
                return d.get('mode', 'enabled') in ['enabled', 'disabled']

            exclude_keys = flatten_list(self.exclude)
            image_ex = list(exclude_keys)
            rpm_ex = list(exclude_keys)
            image_keys = flatten_list(self.images)
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

            for img in image_name_data.itervalues():
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

            missed_include = set(image_keys + rpm_keys) - set(image_data.keys() + rpm_data.keys())
            if len(missed_include) > 0:
                raise DoozerFatalError('The following images or rpms were either missing or filtered out: {}'.format(', '.join(missed_include)))

            if mode in ['images', 'both']:
                for i in image_data.itervalues():
                    metadata = ImageMetadata(self, i)
                    self.image_map[metadata.distgit_key] = metadata
                if not self.image_map:
                    self.logger.warning("No image metadata directories found for given options within: {}".format(self.group_dir))

                for image in self.image_map.itervalues():
                    image.resolve_parent()

                # now that ancestry is defined, make sure no cyclic dependencies
                for image in self.image_map.itervalues():
                    for child in image.children:
                        if image.is_ancestor(child):
                            raise DoozerFatalError('{} cannot be both a parent and dependent of {}'.format(child.distgit_key, image.distgit_key))

                self.image_tree = {}
                image_lists = {0: []}

                def add_child_branch(child, branch, level=1):
                    if level not in image_lists:
                        image_lists[level] = []
                    for sub_child in child.children:
                        branch[sub_child.distgit_key] = {}
                        image_lists[level].append(sub_child.distgit_key)
                        add_child_branch(sub_child, branch[sub_child.distgit_key], level + 1)

                for image in self.image_map.itervalues():
                    if not image.parent:
                        self.image_tree[image.distgit_key] = {}
                        image_lists[0].append(image.distgit_key)
                        add_child_branch(image, self.image_tree[image.distgit_key])

                levels = image_lists.keys()
                levels.sort()
                self.image_order = []
                for l in levels:
                    for i in image_lists[l]:
                        if i not in self.image_order:
                            self.image_order.append(i)

            if mode in ['rpms', 'both']:
                for r in rpm_data.itervalues():
                    metadata = RPMMetadata(self, r, clone_source=clone_source)
                    self.rpm_map[metadata.distgit_key] = metadata
                if not self.rpm_map:
                    self.logger.warning("No rpm metadata directories found for given options within: {}".format(self.group_dir))

        # Make sure that the metadata is not asking us to check out the same exact distgit & branch.
        # This would almost always indicate someone has checked in duplicate metadata into a group.
        no_collide_check = {}
        for meta in self.rpm_map.values() + self.image_map.values():
            key = '{}/{}/#{}'.format(meta.namespace, meta.name, meta.branch())
            if key in no_collide_check:
                raise IOError('Complete duplicate distgit & branch; something wrong with metadata: {} from {} and {}'.format(key, meta.config_filename, no_collide_check[key].config_filename))
            no_collide_check[key] = meta

        # Read in the streams definite for this group if one exists
        streams = self.gitdata.load_data(key='streams')
        if streams:
            self.streams = Model(self.gitdata.load_data(key='streams').data)

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

    @staticmethod
    def timestamp():
        return datetime.datetime.utcnow().isoformat()

    def image_metas(self):
        return self.image_map.values()

    def ordered_image_metas(self):
        return [self.image_map[dg] for dg in self.image_order]

    def image_distgit_by_name(self, name):
        """Returns image meta but full name, short name, or distgit"""
        return self.image_name_map.get(name, None)

    def rpm_metas(self):
        return self.rpm_map.values()

    def all_metas(self):
        return self.image_metas() + self.rpm_metas()

    def register_source_alias(self, alias, path):
        self.logger.info("Registering source alias %s: %s" % (alias, path))
        path = os.path.abspath(path)
        assertion.isdir(path, "Error registering source alias %s" % alias)
        self.source_paths[alias] = path
        with Dir(path):
            origin_url = "?"
            rc1, out_origin, err_origin = exectools.cmd_gather(
                ["git", "config", "--get", "remote.origin.url"])
            if rc1 == 0:
                origin_url = out_origin.strip()
                # Usually something like "git@github.com:openshift/origin.git"
                # But we want an https hyperlink like http://github.com/openshift/origin
                if origin_url.startswith("git@"):
                    origin_url = origin_url[4:]  # remove git@
                    origin_url = origin_url[:-4]  # remove .git
                    origin_url = origin_url.replace(":", "/", 1)  # replace first colon with /
                    origin_url = "https://%s" % origin_url
            else:
                self.logger.error("Failed acquiring origin url for source alias %s: %s" % (alias, err_origin))

            branch = "?"
            rc2, out_branch, err_branch = exectools.cmd_gather(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"])
            if rc2 == 0:
                branch = out_branch.strip()
            else:
                self.logger.error("Failed acquiring origin branch for source alias %s: %s" % (alias, err_branch))

            if 'source_alias' not in self.state:
                self.state['source_alias'] = {}
            self.state['source_alias'][alias] = {
                'url': origin_url,
                'branch': branch,
                'path': path
            }
            self.add_record("source_alias", alias=alias, origin_url=origin_url, branch=branch, path=path)

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
            for k, v in kwargs.iteritems():
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

        with open(os.path.join(self.distgits_diff_dir, distgit + '.patch'), 'w') as f:
            f.write(diff)

    def resolve_image(self, distgit_name, required=True):
        if distgit_name not in self.image_map:
            if not required:
                return None
            raise DoozerFatalError("Unable to find image metadata in group / included images: %s" % distgit_name)
        return self.image_map[distgit_name]

    def late_resolve_image(self, distgit_name, add=False):
        """Resolve image and retrieve meta, optionally adding to image_map.
        If add==True and image not found, error will be thrown"""

        if distgit_name in self.image_map:
            return self.image_map[distgit_name]

        data_obj = self.gitdata.load_data(path='images', key=distgit_name)
        if add and not data_obj:
            raise DoozerFatalError('Unable to resovle image metadata for {}'.format(distgit_name))

        meta = ImageMetadata(self, data_obj)
        if add:
            self.image_map[distgit_name] = meta
        return meta

    def resolve_stream(self, stream_name):

        # If the stream has an override from the command line, return it.
        if stream_name in self.stream_alias_overrides:
            return self.stream_alias_overrides[stream_name]

        if stream_name not in self.streams:
            raise IOError("Unable to find definition for stream: %s" % stream_name)

        return self.streams[stream_name]

    def resolve_source(self, parent, meta):
        """
        Looks up a source alias and returns a path to the directory containing
        that source. Sources can be specified on the command line, or, failing
        that, in group.yml.
        If a source specified in group.yaml has not be resolved before,
        this method will clone that source to checkout the group's desired
        branch before returning a path to the cloned repo.
        :param parent: Name of parent the source belongs to
        :param meta: The MetaData object to resolve source for
        :return: Returns the source path
        """

        source = meta.config.content.source

        # This allows passing `--source <distgit_key> path` to
        # override any source to something local without it
        # having been configured for an alias
        if self.local and meta.distgit_key in self.source_paths:
            source['alias'] = meta.distgit_key
            if 'git' in source:
                del source['git']

        source_details = None
        if 'git' in source:
            git_url = urlparse.urlparse(source.git.url)
            name = os.path.splitext(os.path.basename(git_url.path))[0]
            alias = '{}_{}'.format(parent, name)
            source_details = dict(source.git)
        elif 'alias' in source:
            alias = source.alias
        else:
            raise DoozerFatalError('Error while processing source for {}'.format(parent))

        self.logger.debug("Resolving local source directory for alias {}".
                          format(alias))
        if alias in self.source_paths:
            self.logger.debug(
                "returning previously resolved path for alias {}: {}".
                format(alias, self.source_paths[alias]))
            return self.source_paths[alias]

        # Where the source will land, check early so we know if old or new style
        sub_path = '{}{}'.format('global_' if source_details is None else '', alias)
        source_dir = os.path.join(self.sources_dir, sub_path)

        if not source_details:  # old style alias was given
            if (self.group_config.sources is Missing or alias not in self.group_config.sources):
                raise DoozerFatalError("Source alias not found in specified sources or in the current group: %s" % alias)
            source_details = self.group_config.sources[alias]

        self.logger.debug("checking for source directory in source_dir: {}".
                          format(source_dir))

        # If this source has already been extracted for this working directory
        if os.path.isdir(source_dir):
            # Store so that the next attempt to resolve the source hits the map
            self.source_paths[alias] = source_dir
            self.logger.info(
                "Source '{}' already exists in (skipping clone): {}".
                format(alias, source_dir))
            return source_dir

        clone_branch, _ = self.detect_remote_source_branch(source_details)

        url = source_details["url"]
        try:
            self.logger.info("Attempting to checkout source '%s' branch %s in: %s" % (url, clone_branch, source_dir))
            exectools.cmd_assert(
                # get a little history to enable finding a recent Dockerfile change, but not too much.
                "git clone -b {} --single-branch {} --depth 50 {}".format(clone_branch, url, source_dir),
                retries=3,
                on_retry=["rm", "-rf", source_dir],
            )
        except IOError as e:
            self.logger.info("Unable to checkout branch {}: {}".format(clone_branch, e.message))
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

    def resolve_source_head(self, parent, meta):
        """
        Attempts to resolve the branch a given source alias has checked out. If not on a branch
        returns SHA of head.
        :param parent: Name of parent requesting the source
        :param meta: The MetaData object to resolve source for
        :param required: Whether an error should be thrown or None returned if it cannot be determined
        :return: The name of the checked out branch or None (if required=False)
        """
        source_dir = self.resolve_source(parent, meta)

        if not source_dir:
            return None

        with open(os.path.join(source_dir, '.git/HEAD')) as f:
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
        with open(output, 'w') as sources_file:
            yaml.dump(self.source_paths, sources_file, default_flow_style=False)

    def _flag_file(self, flag_name):
        return os.path.join(self.flags_dir, flag_name)

    def flag_create(self, flag_name, msg=""):
        with open(self._flag_file(flag_name), 'w') as f:
            f.write(msg)

    def flag_exists(self, flag_name):
        return os.path.isfile(self._flag_file(flag_name))

    def flag_remove(self, flag_name):
        if self.flag_exists(flag_name):
            os.remove(self._flag_file(flag_name))

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
        return re.match("^v\d+((\.\d+)+)?$", version) is not None

    @classmethod
    def _parallel_exec(self, f, args, n_threads):
        pool = ThreadPool(n_threads)
        ret = pool.map_async(wrap_exception(f), args)
        pool.close()
        pool.join()
        return ret

    def clone_distgits(self, n_threads=None):
        if n_threads is None:
            n_threads = self.global_opts['distgit_threads']
        return self._parallel_exec(
            lambda m: m.distgit_repo(),
            self.all_metas(),
            n_threads=n_threads).get()

    def push_distgits(self, n_threads=None):
        if n_threads is None:
            n_threads = self.global_opts['distgit_threads']
        return self._parallel_exec(
            lambda m: m.distgit_repo().push(),
            self.all_metas(),
            n_threads=n_threads).get()

    def parallel_exec(self, f, args, n_threads=None):
        n_threads = n_threads if n_threads is not None else len(args)
        terminate_event = threading.Event()
        pool = ThreadPool(n_threads)
        ret = pool.map_async(
            wrap_exception(f),
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

    def builds_for_group_branch(self):
        # return a dict of all the latest builds for this group, according to
        # the branch's candidate tag in brew. each entry is name => tuple(version, release).
        output, _ = exectools.cmd_assert(
            "brew list-tagged --quiet --latest {}-candidate".format(self.branch),
            retries=3,
        )
        builds = [
            # each line like "build tag owner" split into build NVR
            line.split()[0].rsplit("-", 2)
            for line in output.strip().split("\n")
            if line.strip()
        ]
        return { n: (v, r) for n, v, r in builds }

    def scan_distgit_sources(self):
        builds = self.builds_for_group_branch()  # query for builds only once
        return self.parallel_exec(
            lambda m: (m[0], m[0].distgit_repo(autoclone=False).matches_source_commit(builds)),
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

        try:
            self.gitdata = gitdata.GitData(data_path=self.data_path, clone_dir=self.working_dir,
                                           branch=self.group, logger=self.logger)
            self.data_dir = self.gitdata.data_dir
        except gitdata.GitDataException as ex:
            raise DoozerFatalError(ex.message)
