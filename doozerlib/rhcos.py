
import json, koji
from typing import Dict, List, Tuple, Optional

from doozerlib.rpm_utils import parse_nvr

from tenacity import retry, stop_after_attempt, wait_fixed
from urllib import request
from urllib.error import URLError
from doozerlib.util import brew_suffix_for_arch, isolate_el_version_in_release
from doozerlib import exectools
from doozerlib.model import ListModel, Model
from doozerlib import brew

RHCOS_BASE_URL = "https://releases-rhcos-art.apps.ocp-virt.prod.psi.redhat.com/storage/releases"
# Historically the only RHCOS container was 'machine-os-content'; see
# https://github.com/openshift/machine-config-operator/blob/master/docs/OSUpgrades.md
# But in the future this will change, see
# https://github.com/coreos/enhancements/blob/main/os/coreos-layering.md
default_primary_container = dict(
    name="machine-os-content",
    build_metadata_key="oscontainer",
    primary=True)


class RhcosMissingContainerException(Exception):
    """
    Thrown when group.yml configuration expects an RHCOS container but it is
    not available as specified in the RHCOS metadata.
    """
    pass


def get_container_configs(runtime):
    """
    look up the group.yml configuration for RHCOS container(s) for this group, or create if missing.
    @return ListModel with Model entries like ^^ default_primary_container
    """
    return runtime.group_config.rhcos.payload_tags or ListModel([default_primary_container])


def get_container_names(runtime):
    """
    look up the payload tags of the group.yml-configured RHCOS container(s) for this group
    @return list of container names
    """
    return {tag.name for tag in get_container_configs(runtime)}


def get_primary_container_conf(runtime):
    """
    look up the group.yml-configured primary RHCOS container for this group.
    @return Model with entries for name and build_metadata_key
    """
    for tag in get_container_configs(runtime):
        if tag.primary:
            return tag
    raise Exception("Need to provide a group.yml rhcos.payload_tags entry with primary=true")


def get_primary_container_name(runtime):
    """
    convenience method to retrieve configured primary RHCOS container name
    @return primary container name (used in payload tag)
    """
    return get_primary_container_conf(runtime).name


def get_container_pullspec(build_meta: dict, container_conf: Model) -> str:
    """
    determine the container pullspec from the RHCOS build meta and config
    @return full container pullspec string (registry/repo@sha256:...)
    """
    key = container_conf.build_metadata_key
    if key not in build_meta:
        raise RhcosMissingContainerException(f"RHCOS build {build_meta['buildid']} has no '{key}' attribute in its metadata")

    container = build_meta[key]

    if 'digest' in container:
        # "oscontainer": {
        #   "digest": "sha256:04b54950ce2...",
        #   "image": "quay.io/openshift-release-dev/ocp-v4.0-art-dev"
        # },
        return container['image'] + "@" + container['digest']

    # "base-oscontainer": {
    #     "image": "registry.ci.openshift.org/rhcos/rhel-coreos@sha256:b8e1064cae637f..."
    # },
    return container['image']


class RHCOSNotFound(Exception):
    pass


class RHCOSBuildFinder:

    def __init__(self, runtime, version: str, brew_arch: str = "x86_64", private: bool = False, custom: bool = False):
        """
        @param runtime  The Runtime object passed in from the CLI
        @param version  The 4.y ocp version as a string (e.g. "4.6")
        @param brew_arch  architecture we are interested in (e.g. "s390x")
        @param private  boolean, true for private stream, false for public (currently, no effect)
        @param custom If the caller knows this build is custom, the library will only search in the -custom buckets. When the RHCOS pipeline runs a custom build, artifacts
            should be stored in a different area; e.g. https://releases-rhcos-art.apps.ocp-virt.prod.psi.redhat.com/storage/releases/rhcos-4.8-custom/48.84.....-0/x86_64/commitmeta.json
            This is done by ART's RHCOS pipeline code when a custom build is indicated: https://gitlab.cee.redhat.com/openshift-art/rhcos-upshift/-/blob/fdad7917ebdd9c8b47d952010e56e511394ed348/Jenkinsfile#L30
        """
        self.runtime = runtime
        self.version = version
        self.brew_arch = brew_arch
        self.private = private
        self.custom = custom
        self._primary_container = None

    def get_primary_container_conf(self):
        """
        look up the group.yml-configured primary RHCOS container on demand and retain it.
        @return Model with entries for name and build_metadata_key
        """
        if not self._primary_container:
            self._primary_container = get_primary_container_conf(self.runtime)
        return self._primary_container

    def rhcos_release_url(self) -> str:
        """
        base url for a release stream in the release browser (AWS bucket).
        @return e.g. "https://releases-rhcos-art...com/storage/releases/rhcos-4.6-s390x"
        """
        # TODO: create private rhcos builds and do something with "private" here
        bucket = self.brew_arch
        if self.custom:
            bucket += '-custom'

        multi_url = self.runtime.group_config.urls.rhcos_release_base["multi"]
        bucket_url = self.runtime.group_config.urls.rhcos_release_base[bucket]
        if multi_url:
            if bucket_url:
                raise ValueError(f"Multiple rhcos_release_base urls found in group config: `multi` and `{bucket}`")
            return multi_url
        if bucket_url:
            return bucket_url

        bucket_suffix = brew_suffix_for_arch(self.brew_arch)
        if self.custom:
            bucket_suffix += '-custom'

        return f"{RHCOS_BASE_URL}/rhcos-{self.version}{bucket_suffix}"

    @retry(reraise=True, stop=stop_after_attempt(10), wait=wait_fixed(3))
    def latest_rhcos_build_id(self) -> Optional[str]:
        """
        :return: Returns the build id for the latest RHCOS build for the specific CPU arch. Return None if not found.
        """
        # this is hard to test with retries, so wrap testable method
        return self._latest_rhcos_build_id()

    def _latest_rhcos_build_id(self) -> Optional[str]:
        # returns the build id string or None (raises RHCOSNotFound for failure to retrieve)
        # (may want to return "schema-version" also if this ever gets more complex)
        url = f"{self.rhcos_release_url()}/builds.json"
        try:
            with request.urlopen(url) as req:
                data = json.loads(req.read().decode())
        except URLError as ex:
            raise RHCOSNotFound(f"Loading RHCOS build at {url} failed: {ex}")

        if not data["builds"]:
            return None

        multi_url = self.runtime.group_config.urls.rhcos_release_base["multi"]
        build_id = None
        if multi_url:
            # Make sure all rhcos arch builds are complete
            arches_building = self.runtime.group_config.arches
            for b in data["builds"]:
                if len(b["arches"]) == len(arches_building):
                    build_id = b["id"]
                    break
        else:
            build_id = data["builds"][0]["id"]
        return build_id

    @retry(reraise=True, stop=stop_after_attempt(10), wait=wait_fixed(3))
    def rhcos_build_meta(self, build_id: str, meta_type: str = "meta") -> Dict:
        """
        Queries the RHCOS release browser to return metadata about the specified RHCOS build.
        :param build_id: The RHCOS build_id to check (e.g. 410.81.20200520.0)
        :param meta_type: The data to retrieve. "commitmeta" (aka OS Metadata - ostree content) or "meta" (aka Build Metadata / Build record).
        :return: Returns a Dict containing the parsed requested metadata. See the RHCOS release browser for examples: https://releases-rhcos-art.apps.ocp-virt.prod.psi.redhat.com/

        Example 'meta.json':
         https://releases-rhcos-art.apps.ocp-virt.prod.psi.redhat.com/storage/releases/rhcos-4.1/410.81.20200520.0/meta.json
         {
             "buildid": "410.81.20200520.0",
             ...
             "oscontainer": {
                 "digest": "sha256:b0997c9fe4363c8a0ed3b52882b509ade711f7cdb620cc7a71767a859172f423"
                 "image": "quay.io/openshift-release-dev/ocp-v4.0-art-dev"
             },
             ...
         }
        """
        # this is hard to test with retries, so wrap testable method
        return self._rhcos_build_meta(build_id, meta_type)

    def _rhcos_build_meta(self, build_id: str, meta_type: str = "meta") -> Dict:
        """
        See public API rhcos_build_meta for details.
        """
        url = f"{self.rhcos_release_url()}/{build_id}/"
        # before 4.3 the arch was not included in the path
        vtuple = tuple(int(f) for f in self.version.split("."))
        url += f"{meta_type}.json" if vtuple < (4, 3) else f"{self.brew_arch}/{meta_type}.json"
        with request.urlopen(url) as req:
            return json.loads(req.read().decode())

    def latest_container(self, container_conf: dict = None) -> Tuple[Optional[str], Optional[str]]:
        """
        :param container_conf: a payload tag conf Model from group.yml (with build_metadata_key)
        :return: Returns (rhcos build id, image pullspec) or (None, None) if not found.
        """
        build_id = self.latest_rhcos_build_id()
        if build_id is None:
            return None, None
        return build_id, get_container_pullspec(
            self.rhcos_build_meta(build_id),
            container_conf or self.get_primary_container_conf()
        )


class RHCOSBuildInspector:

    def __init__(self, runtime, pullspec_for_tag: Dict[str, str], brew_arch: str, build_id: Optional[str] = None):
        self.runtime = runtime
        self.brew_arch = brew_arch
        self.pullspec_for_tag = pullspec_for_tag
        self.build_id = build_id

        # Remember the pullspec(s) provided in case it does not match what is in the releases.yaml.
        # Because of an incident where we needed to repush RHCOS and get a new SHA for 4.10 GA,
        # trust the exact pullspec in releases.yml instead of what we find in the RHCOS release
        # browser.
        for tag, pullspec in pullspec_for_tag.items():
            try:
                image_info_str, _ = exectools.cmd_assert(f'oc image info -o json {pullspec}', retries=3)
            except ChildProcessError as e:
                raise Exception(f'Error fetching RHCOS build {build_id}: {e}')

            image_info = Model(json.loads(image_info_str))
            image_build_id = image_info.config.config.Labels.version
            if not image_build_id:
                raise Exception(f'Unable to determine RHCOS build_id from tag {tag} pullspec {pullspec}. Retrieved image info: {image_info_str}')
            if self.build_id and self.build_id != image_build_id:
                raise Exception(f'Found divergent RHCOS build_id for {pullspec_for_tag}. {image_build_id} versus'
                                f' {self.build_id}')
            self.build_id = image_build_id

        # The first digits of the RHCOS build are the major.minor of the rhcos stream name.
        # Which, near branch cut, might not match the actual release stream.
        # Sadly we don't have any other labels or anything to look at to determine the stream.
        version = self.build_id.split('.')[0]
        self.stream_version = version[0] + '.' + version[1:]  # e.g. 43.82.202102081639.0 -> "4.3"

        try:
            finder = RHCOSBuildFinder(runtime, self.stream_version, self.brew_arch)
            self._build_meta = finder.rhcos_build_meta(self.build_id, meta_type='meta')
            self._os_commitmeta = finder.rhcos_build_meta(self.build_id, meta_type='commitmeta')
        except Exception:
            # Fall back to trying to find a custom build
            finder = RHCOSBuildFinder(runtime, self.stream_version, self.brew_arch, custom=True)
            self._build_meta = finder.rhcos_build_meta(self.build_id, meta_type='meta')
            self._os_commitmeta = finder.rhcos_build_meta(self.build_id, meta_type='commitmeta')

    def __repr__(self):
        return f'RHCOSBuild:{self.brew_arch}:{self.build_id}'

    def get_os_metadata(self) -> Dict:
        """
        :return: Returns a dict representing the RHCOS build's OS metadata (aka commitmeta.json)
        """
        return self._os_commitmeta

    def get_build_metadata(self) -> Dict:
        """
        :return: Returns a dict representing the RHCOS build's metadata (aka meta.json)
        """
        return self._build_meta

    def get_os_metadata_rpm_list(self) -> List[List]:
        """
        :return: Returns the raw RPM entries from the OS metadata. Example entry: ['NetworkManager', '1', '1.14.0', '14.el8', 'x86_64' ]
        Also include entries from the build meta.json extensions manifest. We don't have epoch for
        these so we just use 0 which may not be correct. So far nothing looks at epoch so it's not a problem.
        """
        entries = self.get_os_metadata()['rpmostree.rpmdb.pkglist']
        if not entries:
            raise Exception(f"no pkglist in OS Metadata for build {self.build_id}")

        # items like kernel-rt that are only present in extensions are not listed in the os
        # metadata, so we need to add them in separately.
        try:
            extensions = self.get_build_metadata()['extensions']['manifest']
        except KeyError:
            extensions = dict()  # no extensions before 4.8; ignore missing
        for name, vra in extensions.items():
            # e.g. "kernel-rt-core": "4.18.0-372.32.1.rt7.189.el8_6.x86_64"
            # or "qemu-img": "15:6.2.0-11.module+el8.6.0+16538+01ea313d.6.x86_64"
            version, ra = vra.rsplit('-', 1)
            # if epoch is not specified, just use 0. for some reason it's included in the version in
            # RHCOS metadata as "epoch:version"; but if we query brew for it that way, it does not
            # like the format, so we separate it out from the version.
            epoch, version = version.split(':', 1) if ':' in version else ('0', version)
            release, arch = ra.rsplit('.', 1)
            entries.append([name, epoch, version, release, arch])

        return entries

    def get_rpm_nvrs(self) -> List[str]:
        """
        :return: Returns a list of RPM nvrs that are installed in this build according to OS metadata.
                 Note that these are RPMs and not package brew builds. You cannot use koji.getBuild on
                 these NVRs.
        """
        rpm_nvrs: List[str] = list()
        for rpm_entry in self.get_os_metadata_rpm_list():
            # Example entry ['NetworkManager', '1', '1.14.0', '14.el8', 'x86_64' ]
            # rpm_entry[1] is epoch.
            rpm_nvrs.append(f'{rpm_entry[0]}-{rpm_entry[2]}-{rpm_entry[3]}')

        return rpm_nvrs

    def get_rpm_nvras(self) -> List[str]:
        """
        :return: Returns a list of nvras that are installed in this build according to OS metadata.
                 Note that these are RPMs and not package brew builds. You cannot use koji.getBuild on
                 these NVRAs.
        """
        rpm_nvras: List[str] = list()
        for rpm_entry in self.get_os_metadata_rpm_list():
            # Example entry ['NetworkManager', '1', '1.14.0', '14.el8', 'x86_64' ]
            # rpm_entry[1] is epoch.
            rpm_nvras.append(f'{rpm_entry[0]}-{rpm_entry[2]}-{rpm_entry[3]}.{rpm_entry[4]}')

        return rpm_nvras

    def get_package_build_objects(self) -> Dict[str, Dict]:
        """
        :return: Returns a Dict containing records for package builds corresponding to
                 RPMs used by this RHCOS build.
                 Maps package_name -> brew build dict for package.
        """

        aggregate: Dict[str, Dict] = dict()
        with self.runtime.pooled_koji_client_session() as koji_api:
            for nvra in self.get_rpm_nvras():
                try:
                    rpm_def = koji_api.getRPM(nvra, strict=True)
                except koji.GenericError as e:
                    if self.runtime.group_config.rhcos.allow_missing_brew_rpms:
                        continue  # if conigured, just skip RPMs brew doesn't know about
                    raise Exception(f"Failed to find RPM {nvra} in brew: {e}")

                package_build = koji_api.getBuild(rpm_def['build_id'], brew.KojiWrapperOpts(caching=True), strict=True)
                package_name = package_build['package_name']
                aggregate[package_name] = package_build

        return aggregate

    def get_primary_container_conf(self):
        """
        look up the group.yml-configured primary RHCOS container.
        @return Model with entries for name and build_metadata_key
        """
        return get_primary_container_conf(self.runtime)

    def get_container_configs(self):
        """
        look up the group.yml-configured RHCOS containers and return their configs as a list
        @return list(Model) with entries for name and build_metadata_key
        """
        return get_container_configs(self.runtime)

    def get_container_pullspec(self, container_config: Model = None) -> str:
        """
        Determine the pullspec corresponding to the container config given (the
        primary by default), either as specified at instantiation or from the
        build metadata.

        @param container_config: Model with fields "name" and "build_metadata_key"
        :return: pullspec for the requested container image
        """
        container_config = container_config or self.get_primary_container_conf()
        if container_config.name in self.pullspec_for_tag:
            # per note above... when given a pullspec, prefer that to the build record
            return self.pullspec_for_tag[container_config.name]
        return get_container_pullspec(self.get_build_metadata(), container_config)

    def get_container_digest(self, container_config: Model = None) -> str:
        """
        Extract the image digest for (by default) the primary container image
        associated with this build, historically the sha of the
        machine-os-content image published out on quay.

        @param container_config: Model with fields "name" and "build_metadata_key"
        :return: shasum from the pullspec for the requested container image
        """
        return self.get_container_pullspec(container_config).split("@")[1]

    def get_rhel_base_version(self) -> int:
        """
        Determines whether this RHCOS is based on RHEL 8, 9, ...
        """
        # OS metadata has changed a bit over time (i.e. there may be newer/cleaner ways
        # to determine this), but one thing that seems backwards compatible
        # is finding 'el' information in RPM list.
        for nvr in self.get_rpm_nvrs():
            el_ver = isolate_el_version_in_release(nvr)
            if el_ver:
                return el_ver

        raise IOError(f'Unable to determine RHEL version base for rhcos {self.build_id}')

    def find_non_latest_rpms(self) -> List[Tuple[str, str]]:
        """
        If the packages installed in this image overlap packages in the candidate tag,
        return NVRs of the latest candidate builds that are not also installed in this image.
        This indicates that the image has not picked up the latest from candidate.

        Note that this is completely normal for non-STREAM assemblies. In fact, it is
        normal for any assembly other than the assembly used for nightlies.

        Unfortunately, rhcos builds are not performed in sync with all other builds.
        Thus, it is natural for them to lag behind when RPMs change. The should catch
        up with the next RHCOS build.

        :return: Returns a list of Tuple[INSTALLED_NVRs, NEWEST_NVR] where
        newest is from the "latest" state of the specified candidate tag
        if same the package installed into this archive is not the same NVR.
        """

        # Find the default candidate tag appropriate for the RHEL version used by this RHCOS build.
        candidate_brew_tag = self.runtime.get_default_candidate_brew_tag(el_target=self.get_rhel_base_version())

        # N.B. the "rpms" installed in an image archive are individual RPMs, not brew rpm package builds.
        # we compare against the individual RPMs from latest candidate rpm package builds.
        with self.runtime.shared_build_status_detector() as bs_detector:
            candidate_rpms: Dict[str, Dict] = {
                # the RPMs are collected by name mainly to de-duplicate (same RPM, multiple arches)
                rpm['name']: rpm for rpm in
                bs_detector.find_unshipped_candidate_rpms(candidate_brew_tag, self.runtime.brew_event)
            }

        old_nvrs: List[Tuple[str, str]] = []
        # Translate the package builds into a list of individual RPMs. Build dict[rpm_name] -> nvr for every NVR installed
        # in this RHCOS build.
        installed_nvr_map: Dict[str, str] = {parse_nvr(installed_nvr)['name']: installed_nvr for installed_nvr in self.get_rpm_nvrs()}
        # we expect only a few unshipped candidates most of the the time, so we'll just search for those.
        for name, rpm in candidate_rpms.items():
            rpm_nvr = rpm['nvr']
            if name in installed_nvr_map:
                installed_nvr = installed_nvr_map[name]
                if rpm_nvr != installed_nvr:
                    old_nvrs.append((installed_nvr, rpm_nvr))

        return old_nvrs
