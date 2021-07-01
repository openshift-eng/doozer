from typing import Dict, Optional, List, Tuple, NamedTuple
import urllib.parse
import yaml
from collections import OrderedDict, namedtuple
import pathlib
import json
import traceback
import datetime
import time
import re
from enum import Enum

from xml.etree import ElementTree
from typing import NamedTuple
import dateutil.parser

from .pushd import Dir
from .distgit import ImageDistGitRepo, RPMDistGitRepo
from . import exectools
from . import logutil
from .brew import BuildStates

from .model import Model, Missing
from doozerlib.assembly import assembly_metadata_config, assembly_basis_event


class CgitAtomFeedEntry(NamedTuple):
    title: str
    updated: datetime.datetime
    id: str
    content: str


#
# These are used as labels to index selection of a subclass.
#
DISTGIT_TYPES = {
    'image': ImageDistGitRepo,
    'rpm': RPMDistGitRepo
}

CONFIG_MODES = [
    'enabled',  # business as usual
    'disabled',  # manually disabled from automatically building
    'wip',  # Work in Progress, do not build
]

CONFIG_MODE_DEFAULT = CONFIG_MODES[0]


class RebuildHintCode(Enum):
    NO_COMPONENT = (True, 0)
    NO_LATEST_BUILD = (True, 1)
    DISTGIT_ONLY_COMMIT_OLDER = (False, 2)
    DISTGIT_ONLY_COMMIT_NEWER = (True, 3)
    DELAYING_NEXT_ATTEMPT = (False, 4)
    LAST_BUILD_FAILED = (True, 5)
    NEW_UPSTREAM_COMMIT = (True, 6)
    UPSTREAM_COMMIT_MISMATCH = (True, 7)
    BUILD_IS_UP_TO_DATE = (False, 8)
    ANCESTOR_CHANGING = (True, 9)
    CONFIG_CHANGE = (True, 10)
    BUILDER_CHANGING = (True, 11)
    BUILD_ROOT_CHANGING = (True, 12)
    PACKAGE_CHANGE = (True, 13)
    ARCHES_CHANGE = (True, 14)


class RebuildHint(NamedTuple):
    code: RebuildHintCode
    reason: str

    @property
    def rebuild(self):
        return self.code.value[0]


class Metadata(object):
    def __init__(self, meta_type: str, runtime, data_obj: Dict, commitish: Optional[str] = None, prevent_cloning: Optional[bool] = False):
        """
        :param meta_type - a string. Index to the sub-class <'rpm'|'image'>.
        :param runtime - a Runtime object.
        :param data_obj - a dictionary for the metadata configuration
        :param commitish: If not None, build from the specified upstream commit-ish instead of the branch tip.
        :param prevent_cloning: Throw an exception if upstream/downstream cloning operations are attempted.
        """
        self.meta_type = meta_type
        self.runtime = runtime
        self.data_obj = data_obj
        self.config_filename = data_obj.filename
        self.full_config_path = data_obj.path
        self.commitish = commitish

        # For efficiency, we want to prevent some verbs from introducing changes that
        # trigger distgit or upstream cloning. Setting this flag to True will cause
        # an exception if it is attempted.
        self.prevent_cloning = prevent_cloning

        # URL and branch of public upstream source are set later by Runtime.resolve_source()
        self.public_upstream_url = None
        self.public_upstream_branch = None

        # Some config filenames have suffixes to avoid name collisions; strip off the suffix to find the real
        # distgit repo name (which must be combined with the distgit namespace).
        # e.g. openshift-enterprise-mediawiki.apb.yml
        #      distgit_key=openshift-enterprise-mediawiki.apb
        #      name (repo name)=openshift-enterprise-mediawiki

        self.distgit_key = data_obj.key
        self.name = self.distgit_key.split('.')[0]   # Split off any '.apb' style differentiator (if present)

        self.runtime.logger.debug("Loading metadata from {}".format(self.full_config_path))

        self.raw_config = Model(data_obj.data)  # Config straight from ocp-build-data
        assert (self.raw_config.name is not Missing)

        self.config = assembly_metadata_config(runtime.get_releases_config(), runtime.assembly, meta_type, self.distgit_key, self.raw_config)
        self.namespace, self._component_name = Metadata.extract_component_info(meta_type, self.name, self.config)

        self.mode = self.config.get('mode', CONFIG_MODE_DEFAULT).lower()
        if self.mode not in CONFIG_MODES:
            raise ValueError('Invalid mode for {}'.format(self.config_filename))

        self.enabled = (self.mode == CONFIG_MODE_DEFAULT)

        self.qualified_name = "%s/%s" % (self.namespace, self.name)
        self.qualified_key = "%s/%s" % (self.namespace, self.distgit_key)

        # Includes information to identify the metadata being used with each log message
        self.logger = logutil.EntityLoggingAdapter(logger=self.runtime.logger, extra={'entity': self.qualified_key})

        self._distgit_repo = None

        # List of Brew targets.
        # The first target is the primary target, against which tito will direct build.
        # Others are secondary targets. We will use Brew API to build against secondary
        # targets with the same distgit commit as the primary target.
        self.targets = self.determine_targets()

    def determine_targets(self) -> List[str]:
        """ Determine Brew targets for building this component
        """
        key = 'targets' if not self.runtime.hotfix else "hotfix_targets"
        targets = self.config.get(key)
        if not targets:
            # If not specified in rpm meta, load from group config
            profile_name = self.runtime.profile or self.runtime.group_config.get(f"default_{self.meta_type}_build_profile")
            if profile_name:
                targets = self.runtime.group_config.build_profiles.primitive()[self.meta_type][profile_name].get(key)
        if not targets:
            # If group config doesn't define the targets either, the target name will be derived from the distgit branch name
            targets = [self.default_brew_target()]
        return targets

    def save(self):
        self.data_obj.data = self.config.primitive()
        self.data_obj.save()

    def distgit_remote_url(self):
        pkgs_host = self.runtime.group_config.urls.get('pkgs_host', 'pkgs.devel.redhat.com')
        # rhpkg uses a remote named like this to pull content from distgit
        if self.runtime.user:
            return f'ssh://{self.runtime.user}@{pkgs_host}/{self.qualified_name}'
        return f'ssh://{pkgs_host}/{self.qualified_name}'

    def distgit_repo(self, autoclone=True) -> RPMDistGitRepo:
        if self._distgit_repo is None:
            self._distgit_repo = DISTGIT_TYPES[self.meta_type](self, autoclone=autoclone)
        return self._distgit_repo

    def branch(self) -> str:
        if self.config.distgit.branch is not Missing:
            return self.config.distgit.branch
        return self.runtime.branch

    def branch_major_minor(self) -> str:
        """
        :return: Extracts and returns '{major}.{minor}' from the distgit branch.
        """
        split = self.branch().split('-')  # e.g. ['rhaos', '4.8', 'rhel', '8']
        return split[1]

    def build_root_tag(self):
        return '{}-build'.format(self.branch())

    def candidate_brew_tag(self):
        return '{}-candidate'.format(self.branch())

    def hotfix_brew_tag(self):
        return f'{self.branch()}-hotfix'

    def default_brew_tag(self):
        return self.hotfix_brew_tag() if self.runtime.hotfix else self.candidate_brew_tag()

    def default_brew_target(self):
        return NotImplementedError()

    def candidate_brew_tags(self):
        return [self.candidate_brew_tag()]

    def get_arches(self):
        """
        :return: Returns the list of architecture this image/rpm should build for. This is an intersection
        of config specific arches & globally enabled arches in group.yml
        """
        if self.config.arches:
            ca = self.config.arches
            intersection = list(set(self.runtime.get_global_arches()) & set(ca))
            if len(intersection) != len(ca):
                self.logger.info(f'Arches are being pruned by group.yml. Using computed {intersection} vs config list {ca}')
            if not intersection:
                raise ValueError(f'No arches remained enabled in {self.qualified_key}')
            return intersection
        else:
            return list(self.runtime.get_global_arches())

    def cgit_atom_feed(self, commit_hash: Optional[str] = None, branch: Optional[str] = None) -> List[CgitAtomFeedEntry]:
        """
        :param commit_hash: Specify to receive an entry for the specific commit (branch ignored if specified).
                            Returns a feed with a single entry.
        :param branch: branch name; None implies the branch specified in ocp-build-data (XOR commit_hash).
                            Returns a feed with several of the most recent entries.

        Returns a representation of the cgit atom feed. This information includes
        feed example: https://gist.github.com/jupierce/ab006c0fc83050b714f6de2ec30f1072 . This
        feed provides timestamp and commit information without having to clone distgits.

        Example urls..
        http://pkgs.devel.redhat.com/cgit/containers/cluster-etcd-operator/atom/?h=rhaos-4.8-rhel-8
        or
        http://pkgs.devel.redhat.com/cgit/containers/cluster-etcd-operator/atom/?id=35ecfa4436139442edc19585c1c81ebfaca18550
        """
        cgit_url_base = self.runtime.group_config.urls.cgit
        if not cgit_url_base:
            raise ValueError("urls.cgit is not set in group config")
        url = f"{cgit_url_base}/{urllib.parse.quote(self.qualified_name)}/atom/"
        params = {}
        if commit_hash:
            params["id"] = commit_hash
        else:
            if branch is None:
                branch = self.branch()
            if not commit_hash and branch:
                params["h"] = branch
        if params:
            url += "?" + urllib.parse.urlencode(params)

        req = exectools.retry(
            3, lambda: urllib.request.urlopen(url),
            check_f=lambda req: req.code == 200)

        content = req.read()
        et = ElementTree.fromstring(content)

        entry_list = list()
        for et_entry in et.findall('{http://www.w3.org/2005/Atom}entry'):
            entry = CgitAtomFeedEntry(
                title=et_entry.find('{http://www.w3.org/2005/Atom}title').text,
                updated=dateutil.parser.parse(et_entry.find('{http://www.w3.org/2005/Atom}updated').text),
                id=et_entry.find('{http://www.w3.org/2005/Atom}id').text,
                content=et_entry.find('{http://www.w3.org/2005/Atom}content[@type="text"]').text
            )
            entry_list.append(entry)

        return entry_list

    def cgit_file_url(self, filename: str, commit_hash: Optional[str] = None, branch: Optional[str] = None) -> str:
        """ Construct a cgit URL to a given file associated with the commit hash pushed to distgit
        :param filename: a relative path
        :param commit_hash: commit hash; None implies the current HEAD
        :param branch: branch name; None implies the branch specified in ocp-build-data
        :return: a cgit URL
        """
        cgit_url_base = self.runtime.group_config.urls.cgit
        if not cgit_url_base:
            raise ValueError("urls.cgit is not set in group config")
        ret = f"{cgit_url_base}/{urllib.parse.quote(self.qualified_name)}/plain/{urllib.parse.quote(filename)}"
        params = {}
        if branch is None:
            branch = self.branch()
        if branch:
            params["h"] = branch
        if commit_hash:
            params["id"] = commit_hash
        if params:
            ret += "?" + urllib.parse.urlencode(params)
        return ret

    def fetch_cgit_file(self, filename):
        url = self.cgit_file_url(filename)
        req = exectools.retry(
            3, lambda: urllib.request.urlopen(url),
            check_f=lambda req: req.code == 200)
        return req.read()

    def get_latest_build(self, default=-1, assembly=None, extra_pattern='*', build_state: BuildStates = BuildStates.COMPLETE, el_target=None, honor_is=True):
        """
        :param default: A value to return if no latest is found (if not specified, an exception will be thrown)
        :param assembly: A non-default assembly name to search relative to. If not specified, runtime.assembly
                         will be used. If runtime.assembly is also None, the search will return true latest.
                         If the assembly parameter is set to '', this search will also return true latest.
        :param extra_pattern: An extra glob pattern that must be matched in the middle of the
                         build's release field. Pattern must match release timestamp and components
                         like p? and git commit (up to, but not including ".assembly.<name>" release
                         component). e.g. "*.git.<commit>.*   or '*.p1.*'
        :param build_state: 0=BUILDING, 1=COMPLETE, 2=DELETED, 3=FAILED, 4=CANCELED
        :param el_target: In the case of an RPM, which can build for multiple targets, you can specify
                            '7' for el7, '8' for el8, etc. You can also pass in a brew target that
                            contains '....-rhel-?..' and the number will be extraced. If you want the true
                            latest, leave as None.
        :param honor_is: If True, and an assembly component specifies 'is', that nvr will be returned.
        :return: Returns the most recent build object from koji for this package & assembly.
                 Example https://gist.github.com/jupierce/57e99b80572336e8652df3c6be7bf664
        """
        component_name = self.get_component_name()
        builds = []

        with self.runtime.pooled_koji_client_session() as koji_api:
            package_info = koji_api.getPackage(component_name)  # e.g. {'id': 66873, 'name': 'atomic-openshift-descheduler-container'}
            if not package_info:
                raise IOError(f'No brew package is defined for {component_name}')
            package_id = package_info['id']  # we could just constrain package name using pattern glob, but providing package ID # should be a much more efficient DB query.
            # listBuilds returns all builds for the package; We need to limit the query to the builds
            # relevant for our major/minor.

            rpm_suffix = ''  # By default, find the latest RPM build - regardless of el7, el8, ...

            if self.meta_type == 'image':
                ver_prefix = 'v'  # openshift-enterprise-console-container-v4.7.0-202106032231.p0.git.d9f4379
            else:
                # RPMs do not have a 'v' in front of their version; images do.
                ver_prefix = ''  # openshift-clients-4.7.0-202106032231.p0.git.e29b355.el8
                if el_target:
                    el_target = f'-rhel-{el_target}'  # Whether the incoming value is an int, decimal str, or a target, normalize for regex
                    target_match = re.match(r'.*-rhel-(\d+)(?:-|$)', str(el_target))  # tolerate incoming int with str()
                    if target_match:
                        rpm_suffix = f'.el{target_match.group(1)}'
                    else:
                        raise IOError(f'Unable to determine rhel version from specified el_target: {el_target}')

            pattern_prefix = f'{component_name}-{ver_prefix}{self.branch_major_minor()}.'

            if assembly is None:
                assembly = self.runtime.assembly

            def default_return():
                msg = f"No builds detected for using prefix: '{pattern_prefix}', extra_pattern: '{extra_pattern}', assembly: '{assembly}', build_state: '{build_state.name}', el_target: '{el_target}'"
                if default != -1:
                    self.logger.info(msg)
                    return default
                raise IOError(msg)

            def latest_build_list(pattern_suffix):
                # Include * after pattern_suffix to tolerate:
                # 1. Matching an unspecified RPM suffix (e.g. .el7).
                # 2. Other release components that might be introduced later.
                builds = koji_api.listBuilds(packageID=package_id,
                                             state=None if build_state is None else build_state.value,
                                             pattern=f'{pattern_prefix}{extra_pattern}{pattern_suffix}*{rpm_suffix}',
                                             queryOpts={'limit': 1, 'order': '-creation_event_id'})

                # Ensure the suffix ends the string OR at least terminated by a '.' .
                # This latter check ensures that 'assembly.how' doesn't not match a build from
                # "assembly.howdy'.
                refined = [b for b in builds if b['nvr'].endswith(pattern_suffix) or f'{pattern_suffix}.' in b['nvr']]

                if refined and build_state == BuildStates.COMPLETE:
                    # A final sanity check to see if the build is tagged with something we
                    # respect. There is a chance that a human may untag a build. There
                    # is no standard practice at present in which they should (they should just trigger
                    # a rebuild). If we find the latest build is not tagged appropriately, blow up
                    # and let a human figure out what happened.
                    check_nvr = refined[0]['nvr']
                    for i in range(2):
                        tags = {tag['name'] for tag in koji_api.listTags(build=check_nvr)}
                        if tags:
                            break
                        # Observed that a complete build needs some time before it gets tagged. Give it some
                        # time if not immediately available.
                        time.sleep(60)

                    # RPMS have multiple targets, so our self.branch() isn't perfect.
                    # We should permit rhel-8/rhel-7/etc.
                    tag_prefix = self.branch().rsplit('-', 1)[0] + '-'   # String off the rhel version.
                    accepted_tags = [name for name in tags if name.startswith(tag_prefix)]
                    if not accepted_tags:
                        raise IOError(f'Expected to find at least one tag starting with {self.branch()} on latest build {check_nvr} but found [{tags}]; something has changed tags in an unexpected way')

                return refined

            if honor_is and self.config['is']:
                if build_state != BuildStates.COMPLETE:
                    # If this component is defined by 'is', history failures, etc, do not matter.
                    return default_return()

                # under 'is' for RPMs, we expect 'el7' and/or 'el8', etc. For images, just 'nvr'.
                isd = self.config['is']
                if self.meta_type == 'rpm':
                    if el_target is None:
                        raise ValueError(f'Expected el_target to be set when querying a pinned RPM component {self.distgit_key}')
                    is_nvr = isd[f'el{el_target}']
                    if not is_nvr:
                        return default_return()
                else:
                    # The image metadata (or, more likely, the currently assembly) has the image
                    # pinned. Return only the pinned NVR. When a child image is being rebased,
                    # it uses get_latest_build to find the parent NVR to use (if it is not
                    # included in the "-i" doozer argument). We need it to find the pinned NVR
                    # to place in its Dockerfile.
                    # Pinning also informs gen-payload when attempting to assemble a release.
                    is_nvr = isd.nvr
                    if not is_nvr:
                        raise ValueError(f'Did not find nvr field in pinned Image component {self.distgit_key}')

                # strict means raise an exception if not found.
                found_build = koji_api.getBuild(is_nvr, strict=True)
                # Different brew apis return different keys here; normalize to make the rest of doozer not need to change.
                found_build['id'] = found_build['build_id']
                return found_build

            if not assembly:
                # if assembly is '' (by parameter) or still None after runtime.assembly,
                # we are returning true latest.
                builds = latest_build_list('')
            else:
                basis_event = assembly_basis_event(self.runtime.get_releases_config(), assembly=assembly)
                if basis_event:
                    self.logger.warning(f'Constraining image search to stream assembly due to assembly basis event {basis_event}')
                    # If an assembly has a basis event, its latest images can only be sourced from
                    # "is:" or the stream assembly. We've already checked for "is" above.
                    assembly = 'stream'

                # Assemblies without a basis will return assembly qualified builds for their
                # latest images. This includes "stream" and "test", but could also include
                # an assembly that is customer specific  with its own branch.
                builds = latest_build_list(f'.assembly.{assembly}')
                if not builds:
                    if assembly != 'stream':
                        builds = latest_build_list('.assembly.stream')
                    if not builds:
                        # Fall back to true latest
                        builds = latest_build_list('')
                        if builds and '.assembly.' in builds[0]['release']:
                            # True latest belongs to another assembly. In this case, just return
                            # that they are no builds for this assembly.
                            builds = []

        if not builds:
            return default_return()

        found_build = builds[0]
        # Different brew apis return different keys here; normalize to make the rest of doozer not need to change.
        found_build['id'] = found_build['build_id']
        return found_build

    def get_latest_build_info(self, default=-1, **kwargs):
        """
        Queries brew to determine the most recently built release of the component
        associated with this image. This method does not rely on the "release"
        label needing to be present in the Dockerfile. kwargs will be passed on
        to get_latest_build.
        :param default: A value to return if no latest is found (if not specified, an exception will be thrown)
        :return: A tuple: (component name, version, release); e.g. ("registry-console-docker", "v3.6.173.0.75", "1")
        """
        build = self.get_latest_build(default=default, **kwargs)
        if default != -1 and build == default:
            return default
        return build['name'], build['version'], build['release']

    @classmethod
    def extract_component_info(cls, meta_type: str, meta_name: str, config_model: Model) -> Tuple[str, str]:
        """
        Determine the component information for either RPM or Image metadata
        configs.
        :param meta_type: 'rpm' or 'image'
        :param meta_name: The name of the component's distgit
        :param config_model: The configuration for the metadata.
        :return: Return (namespace, component_name)
        """

        # Choose default namespace for config data
        if meta_type == "image":
            namespace = "containers"
        else:
            namespace = "rpms"

        # Allow config data to override namespace
        if config_model.distgit.namespace is not Missing:
            namespace = config_model.distgit.namespace

        if namespace == "rpms":
            # For RPMS, component names must match package name and be in metadata config
            return namespace, config_model.name

        # For RPMs, by default, the component is the name of the distgit,
        # but this can be overridden in the config yaml.
        component_name = meta_name

        # For apbs, component name seems to have -apb appended.
        # ex. http://dist-git.host.prod.eng.bos.redhat.com/cgit/apbs/openshift-enterprise-mediawiki/tree/Dockerfile?h=rhaos-3.7-rhel-7
        if namespace == "apbs":
            component_name = "%s-apb" % component_name

        if namespace == "containers":
            component_name = "%s-container" % component_name

        if config_model.distgit.component is not Missing:
            component_name = config_model.distgit.component

        return namespace, component_name

    def get_component_name(self) -> str:
        """
        :return: Returns the component name of the metadata. This is the name in the nvr
        that brew assigns to component build. Component name is synonymous with package name.
        For RPMs, spec files declare the package name. For images, it is usually based on
        the distgit repo name + '-container'.
        """
        return self._component_name

    def needs_rebuild(self):
        if self.config.targets:
            # If this meta has multiple build targets, check currency of each
            for target in self.config.targets:
                hint = self._target_needs_rebuild(el_target=target)
                if hint.rebuild or hint.code == RebuildHintCode.DELAYING_NEXT_ATTEMPT:
                    # No need to look for more
                    return hint
            return hint
        else:
            return self._target_needs_rebuild(el_target=None)

    def _target_needs_rebuild(self, el_target=None) -> RebuildHint:
        """
        Checks whether the current upstream commit has a corresponding successful downstream build.
        Take care to not unnecessarily trigger a clone of the distgit
        or upstream source as it will dramatically increase the time needed for scan-sources.
        :param el_target: A brew build target or literal '7', '8', or rhel to perform the search for.
        :return: Returns (rebuild:<bool>, message: description of why).
        """
        now = datetime.datetime.now(datetime.timezone.utc)

        # If a build fails, how long will we wait before trying again
        rebuild_interval = self.runtime.group_config.scan_freshness.threshold_hours or 6

        component_name = self.get_component_name()

        latest_build = self.get_latest_build(default=None, el_target=el_target)

        if not latest_build:
            return RebuildHint(code=RebuildHintCode.NO_LATEST_BUILD,
                               reason=f'Component {component_name} has no latest build for assembly: {self.runtime.assembly}')

        latest_build_creation = dateutil.parser.parse(latest_build['creation_time'])
        latest_build_creation = latest_build_creation.replace(tzinfo=datetime.timezone.utc)  # If time lacks timezone info, interpret as UTC

        # Log scan-sources coordinates throughout to simplify setting up scan-sources
        # function tests to reproduce real-life scenarios.
        self.logger.debug(f'scan-sources coordinate: latest_build: {latest_build}')
        self.logger.debug(f'scan-sources coordinate: latest_build_creation_datetime: {latest_build_creation}')

        # If downstream has been locked to a commitish, only check the atom feed at that moment.
        distgit_commitish = self.runtime.downstream_commitish_overrides.get(self.distgit_key, None)
        atom_entries = self.cgit_atom_feed(commit_hash=distgit_commitish, branch=self.branch())
        if not atom_entries:
            raise IOError(f'No atom feed entries exist for {component_name} in {self.branch()}. Does branch exist?')

        dgr = self.distgit_repo(autoclone=False)  # For scan-sources speed, we need to avoid cloning
        if not dgr.has_source():
            # This is a distgit only artifact (no upstream source)

            latest_entry = atom_entries[0]  # Most recent commit's information
            dg_commit = latest_entry.id
            self.logger.debug(f'scan-sources coordinate: dg_commit: {dg_commit}')
            dg_commit_dt = latest_entry.updated
            self.logger.debug(f'scan-sources coordinate: distgit_head_commit_datetime: {dg_commit_dt}')

            if latest_build_creation > dg_commit_dt:
                return RebuildHint(code=RebuildHintCode.DISTGIT_ONLY_COMMIT_OLDER,
                                   reason='Distgit only repo commit is older than most recent build')

            # Two possible states here:
            # 1. A user has made a commit to this dist-git only branch and there has been no build attempt
            # 2. We've already tried a build and the build failed.

            # Check whether a build attempt for this assembly has failed.
            last_failed_build = self.get_latest_build(default=None,
                                                      build_state=BuildStates.FAILED,
                                                      el_target=el_target)  # How recent was the last failed build?
            if not last_failed_build:
                return RebuildHint(code=RebuildHintCode.DISTGIT_ONLY_COMMIT_NEWER,
                                   reason='Distgit only commit is newer than last successful build')

            last_failed_build_creation = dateutil.parser.parse(last_failed_build['creation_time'])
            last_failed_build_creation = last_failed_build_creation.replace(tzinfo=datetime.timezone.utc)  # If time lacks timezone info, interpret as UTC
            if last_failed_build_creation + datetime.timedelta(hours=rebuild_interval) > now:
                return RebuildHint(code=RebuildHintCode.DELAYING_NEXT_ATTEMPT,
                                   reason=f'Waiting at least {rebuild_interval} hours after last failed build')

            return RebuildHint(code=RebuildHintCode.LAST_BUILD_FAILED,
                               reason=f'Last build failed > {rebuild_interval} hours ago; making another attempt')

        # Otherwise, we have source. In the case of git source, check the upstream with ls-remote.
        # In the case of alias (only legacy stuff afaik), check the cloned repo directory.

        if "git" in self.config.content.source:
            remote_branch = self.runtime.detect_remote_source_branch(self.config.content.source.git)[0]
            out, _ = exectools.cmd_assert(["git", "ls-remote", self.config.content.source.git.url, remote_branch], strip=True)
            # Example output "296ac244f3e7fd2d937316639892f90f158718b0	refs/heads/openshift-4.8"
            upstream_commit_hash = out.split()[0]
        elif self.config.content.source.alias and self.runtime.group_config.sources and self.config.content.source.alias in self.runtime.group_config.sources:
            # This is a new style alias with url information in group config
            source_details = self.runtime.group_config.sources[self.config.content.source.alias]
            remote_branch = self.runtime.detect_remote_source_branch(source_details)[0]
            out, _ = exectools.cmd_assert(["git", "ls-remote", source_details.url, remote_branch], strip=True)
            # Example output "296ac244f3e7fd2d937316639892f90f158718b0	refs/heads/openshift-4.8"
            upstream_commit_hash = out.split()[0]
        else:
            # If it is not git, we will need to punt to the rest of doozer to get the upstream source for us.
            with Dir(dgr.source_path()):
                upstream_commit_hash, _ = exectools.cmd_assert('git rev-parse HEAD', strip=True)

        self.logger.debug(f'scan-sources coordinate: upstream_commit_hash: {upstream_commit_hash}')
        git_component = f'.git.{upstream_commit_hash[:7]}'

        # Scan for any build in this assembly which also includes the git commit.
        upstream_commit_build = self.get_latest_build(default=None,
                                                      extra_pattern=f'*{git_component}*',
                                                      el_target=el_target)  # Latest build for this commit.

        if not upstream_commit_build:
            # There is no build for this upstream commit. Two options to assess:
            # 1. This is a new commit and needs to be built
            # 2. Previous attempts at building this commit have failed

            # Check whether a build attempt with this commit has failed before.
            failed_commit_build = self.get_latest_build(default=None,
                                                        extra_pattern=f'*{git_component}*',
                                                        build_state=BuildStates.FAILED,
                                                        el_target=el_target)  # Have we tried before and failed?

            # If not, this is a net-new upstream commit. Build it.
            if not failed_commit_build:
                return RebuildHint(code=RebuildHintCode.NEW_UPSTREAM_COMMIT,
                                   reason='A new upstream commit exists and needs to be built')

            # Otherwise, there was a failed attempt at this upstream commit on record.
            # Make sure provide at least rebuild_interval hours between such attempts
            last_attempt_time = dateutil.parser.parse(failed_commit_build['creation_time'])
            last_attempt_time = last_attempt_time.replace(tzinfo=datetime.timezone.utc)  # If time lacks timezone info, interpret as UTC

            if last_attempt_time + datetime.timedelta(hours=rebuild_interval) < now:
                return RebuildHint(code=RebuildHintCode.LAST_BUILD_FAILED,
                                   reason=f'It has been {rebuild_interval} hours since last failed build attempt')

            return RebuildHint(code=RebuildHintCode.DELAYING_NEXT_ATTEMPT,
                               reason=f'Last build of upstream commit {upstream_commit_hash} failed, but holding off for at least {rebuild_interval} hours before next attempt')

        if latest_build['nvr'] != upstream_commit_build['nvr']:
            return RebuildHint(code=RebuildHintCode.UPSTREAM_COMMIT_MISMATCH,
                               reason=f'Latest build {latest_build["nvr"]} does not match upstream commit build {upstream_commit_build["nvr"]}; commit reverted?')

        return RebuildHint(code=RebuildHintCode.BUILD_IS_UP_TO_DATE,
                           reason=f'Build already exists for current upstream commit {upstream_commit_hash}: {latest_build}')

    def get_maintainer_info(self):
        """
        :return: Returns a dict of identifying maintainer information. Dict might be empty if no maintainer information is available.
            fields are generally [ component: '...', subcomponent: '...', and product: '...' ] if available. These
            are coordinates for product security to figure out where to file bugs when an image or RPM has an issue.
        """

        # We are trying to discover some team information that indicates which BZ or Jira board bugs for this
        # component should be filed against. This information can be stored in the doozer metadata OR
        # in upstream source. Metadata overrides, as usual.

        source_dir = self.runtime.resolve_source(self)

        # Maintainer info can be defined in metadata, so try there first.
        maintainer = self.config.maintainer.copy() or dict()

        # This tuple will also define key ordering in the returned OrderedDict
        known_fields = ('product', 'component', 'subcomponent')

        # Fill in any missing attributes from upstream source
        if source_dir:
            with Dir(source_dir):
                # Not every repo has a master branch, they may have a different default; detect it.
                if self.public_upstream_url:
                    # If there is a public upstream, query it for the default branch. The openshift-priv
                    # clones seem to be non-deterministic on which branch is set as default.
                    remote_info, _ = exectools.cmd_assert('git remote show public_upstream')
                else:
                    remote_info, _ = exectools.cmd_assert('git remote show origin')
                head_branch_lines = [i for i in remote_info.splitlines() if i.strip().startswith('HEAD branch:')]  # e.g. [ "  HEAD branch: master" ]
                if not head_branch_lines:
                    raise IOError('Error trying to detect remote default branch')
                default_branch = head_branch_lines[0].strip().split()[-1]  # [ "  HEAD branch: master" ] => "master"

                _, owners_yaml, _ = exectools.cmd_gather(f'git --no-pager show origin/{default_branch}:OWNERS')
                if owners_yaml.strip():
                    owners = yaml.safe_load(owners_yaml)
                    for field in known_fields:
                        if field not in maintainer and field in owners:
                            maintainer[field] = owners[field]

        if 'product' not in maintainer:
            maintainer['product'] = 'OpenShift Container Platform'  # Safe bet - we are ART.

        # Just so we return things in a defined order (avoiding unnecessary changes in git commits)
        sorted_maintainer = OrderedDict()
        for k in known_fields:
            if k in maintainer:
                sorted_maintainer[k] = maintainer[k]

        # Add anything remaining in alpha order
        for k in sorted(maintainer.keys()):
            if k not in sorted_maintainer:
                sorted_maintainer[k] = maintainer[k]

        return sorted_maintainer

    def extract_kube_env_vars(self) -> Dict[str, str]:
        """
        Analyzes the source_base_dir for either Godeps or go.mod and looks for information about
        which version of Kubernetes is being utilized by the repository. Side effect is cloning distgit
        and upstream source if it has not already been done.
        :return: A Dict of environment variables that should be added to the Dockerfile / rpm spec.
                Variables like KUBE_GIT_VERSION, KUBE_GIT_COMMIT, KUBE_GIT_MINOR, ...
                May be empty if there is no kube information in the source dir.
        """
        envs = dict()

        upstream_source_path: pathlib.Path = self.runtime.resolve_source(self)
        if not upstream_source_path:
            # distgit only. Return empty.
            return envs

        kube_version_fields: List[str] = None  # Populate ['x', 'y', 'z'] this from godeps or gomod
        kube_commit_hash: str = None  # Populate with kube repo hash like '2f054b7646dc9e98f6dea458d2fb65e1d2c1f731'
        with Dir(upstream_source_path):
            out, _ = exectools.cmd_assert(["git", "rev-parse", "HEAD"])
            source_full_sha = out.strip()

            # First determine if this source repository is using Godeps. Godeps is ultimately
            # being replaced by gomod, but older versions of OpenShift continue to use it.
            godeps_file = pathlib.Path(upstream_source_path, 'Godeps', 'Godeps.json')
            if godeps_file.is_file():
                try:
                    with godeps_file.open('r', encoding='utf-8') as f:
                        godeps = json.load(f)
                        # Reproduce https://github.com/openshift/origin/blob/6f457bc317f8ca8e514270714db6597ec1cb516c/hack/lib/build/version.sh#L82
                        # Example of what we are after: https://github.com/openshift/origin/blob/6f457bc317f8ca8e514270714db6597ec1cb516c/Godeps/Godeps.json#L10-L15
                        for dep in godeps.get('Deps', []):
                            if dep.get('ImportPath', '') == 'k8s.io/kubernetes/pkg/api':
                                kube_commit_hash = dep.get('Rev', '')
                                raw_kube_version = dep.get('Comment', '')  # e.g. v1.14.6-152-g117ba1f
                                # drop release information.
                                base_kube_version = raw_kube_version.split('-')[0]  # v1.17.1-152-g117ba1f => v1.17.1
                                kube_version_fields = base_kube_version.lstrip('v').split('.')  # v1.17.1 => [ '1', '17', '1']
                except:
                    self.logger.error(f'Error parsing godeps {str(godeps_file)}')
                    traceback.print_exc()

            go_sum_file = pathlib.Path(upstream_source_path, 'go.sum')
            if go_sum_file.is_file():
                try:
                    # we are looking for a line like: https://github.com/openshift/kubernetes/blob/5241b27b8acd73cdc99a0cac281645189189f1d8/go.sum#L602
                    # e.g. "k8s.io/kubernetes v1.19.0-rc.2/go.mod h1:zomfQQTZYrQjnakeJi8fHqMNyrDTT6F/MuLaeBHI9Xk="
                    with go_sum_file.open('r', encoding='utf-8') as f:
                        for line in f.readlines():
                            if line.startswith('k8s.io/kubernetes '):
                                entry_split = line.split()  # => ['k8s.io/kubernetes', 'v1.19.0-rc.2/go.mod', 'h1:zomfQQTZYrQjnakeJi8fHqMNyrDTT6F/MuLaeBHI9Xk=']
                                base_kube_version = entry_split[1].split('/')[0].strip()  # 'v1.19.0-rc.2/go.mod' => 'v1.19.0-rc.2'
                                kube_version_fields = base_kube_version.lstrip('v').split('.')  # 'v1.19.0-rc.2' => [ '1', '19', '0-rc.2']
                                # upstream kubernetes creates a tag for each version. Go find its sha.
                                rc, out, err = exectools.cmd_gather('git ls-remote https://github.com/kubernetes/kubernetes {base_kube_version}')
                                out = out.strip()
                                if out:
                                    # Expecting something like 'a26dc584ac121d68a8684741bce0bcba4e2f2957	refs/tags/v1.19.0-rc.2'
                                    kube_commit_hash = out.split()[0]
                                else:
                                    # That's strange, but let's not kill the build for it.  Poke in our repo's hash.
                                    kube_commit_hash = source_full_sha
                                break
                except:
                    self.logger.error(f'Error parsing go.sum {str(go_sum_file)}')
                    traceback.print_exc()

            if kube_version_fields:
                # For historical consistency with tito's flow, we add +OS_GIT_COMMIT[:7] to the kube version
                envs['KUBE_GIT_VERSION'] = f"v{'.'.join(kube_version_fields)}+{source_full_sha[:7]}"
                envs['KUBE_GIT_MAJOR'] = '0' if len(kube_version_fields) < 1 else kube_version_fields[0]
                godep_kube_minor = '0' if len(kube_version_fields) < 2 else kube_version_fields[1]
                envs['KUBE_GIT_MINOR'] = f'{godep_kube_minor}+'  # For historical reasons, append a '+' since OCP patches its vendored kube.
                envs['KUBE_GIT_COMMIT'] = kube_commit_hash
                envs['KUBE_GIT_TREE_STATE'] = 'clean'
            elif self.name in ('openshift-enterprise-hyperkube', 'openshift', 'atomic-openshift'):
                self.logger.critical(f'Unable to acquire KUBE vars for {self.name}. This must be fixed or platform addons can break: https://bugzilla.redhat.com/show_bug.cgi?id=1861097')
                raise IOError(f'Unable to determine KUBE vars for {self.name}')

            return envs
