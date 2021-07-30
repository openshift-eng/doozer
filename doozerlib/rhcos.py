from __future__ import absolute_import, print_function, unicode_literals

import json
from typing import Dict, List, Tuple, Optional

from kobo.rpmlib import parse_nvr

from tenacity import retry, stop_after_attempt, wait_fixed
from urllib import request
from doozerlib.util import brew_suffix_for_arch, isolate_el_version_in_release
from doozerlib import exectools
from doozerlib.model import Model
from doozerlib import brew

RHCOS_BASE_URL = "https://releases-rhcos-art.cloud.privileged.psi.redhat.com/storage/releases"


def rhcos_release_url(version, brew_arch="x86_64", private=False) -> str:
    """
    base url for a release stream in the release browser (AWS bucket).

    @param version  The 4.y ocp version as a string (e.g. "4.6")
    @param brew_arch  architecture we are interested in (e.g. "s390x")
    @param private  boolean, true for private stream, false for public (currently, no effect)
    @return e.g. "https://releases-rhcos-art...com/storage/releases/rhcos-4.6-s390x"
    """
    # TODO: create private rhcos builds and do something with "private" here
    return f"{RHCOS_BASE_URL}/rhcos-{version}{brew_suffix_for_arch(brew_arch)}"


@retry(reraise=True, stop=stop_after_attempt(10), wait=wait_fixed(3))
def latest_rhcos_build_id(version: str, brew_arch: str = "x86_64", private: bool = False) -> Optional[str]:
    """
    :param version: The version number of the RHCOS stream (e.g. '4.6' would translate to 'rhcos-4.6' stream).
    :param brew_arch: The CPU architecture for the build.
    :param private: Whether we are looking for a private or public build. NOT CURRENTLY SUPPORTED.
    :return: Returns the build id for the latest RHCOS build for the specific CPU arch. Return None if not found.
    """
    # this is hard to test with retries, so wrap testable method
    return _latest_rhcos_build_id(version, brew_arch, private)


def _latest_rhcos_build_id(version: str, brew_arch: str = "x86_64", private: bool = False) -> Optional[str]:
    """
    :param version: The version number of the RHCOS stream (e.g. '4.6' would translate to 'rhcos-4.6' stream).
    :param brew_arch: The CPU architecture for the build.
    :param private: Whether we are looking for a private or public build. NOT CURRENTLY SUPPORTED.
    :return: Returns the build id for the latest RHCOS build for the specific CPU arch from the RHCOS release browser.
    Return None if not found.
    """
    # returns the build id string or None (or raise exception)
    # (may want to return "schema-version" also if this ever gets more complex)
    with request.urlopen(f"{rhcos_release_url(version, brew_arch, private)}/builds.json") as req:
        data = json.loads(req.read().decode())
    if not data["builds"]:
        return None
    build = data["builds"][0]
    # old schema just had the id as a string; newer has it in a dict
    return build if isinstance(build, str) else build["id"]


@retry(reraise=True, stop=stop_after_attempt(10), wait=wait_fixed(3))
def rhcos_build_meta(build_id: str, version: str, brew_arch: str = "x86_64", private: bool = False, meta_type: str = "meta") -> Dict:
    """
    Queries the RHCOS release browser to return metadata about the specified RHCOS build.
    :param build_id: The RHCOS build_id to check (e.g. 410.81.20200520.0)
    :param version: The major.minor of the RHCOS stream the build is associated with (e.g. '4.6')
    :param brew_arch: The CPU architecture for the build (uses brew naming convention)
    :param private: Whether this is a private build (NOT CURRENTLY SUPPORTED)
    :param meta_type: The data to retrieve. "commitmeta" (aka OS Metadata - ostree content) or "meta" (aka Build Metadata / Build record).
    :return: Returns a Dict containing the parsed requested metadata. See the RHCOS release browser for examples: https://releases-rhcos-art.cloud.privileged.psi.redhat.com/

    Example 'meta.json':
     https://releases-rhcos-art.cloud.privileged.psi.redhat.com/storage/releases/rhcos-4.1/410.81.20200520.0/meta.json
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
    return _rhcos_build_meta(build_id, version, brew_arch, private, meta_type)


def _rhcos_build_meta(build_id: str, version: str, brew_arch: str = "x86_64", private: bool = False, meta_type: str = "meta") -> Dict:
    """
    See public API rhcos_build_meta for details.
    """
    url = f"{rhcos_release_url(version, brew_arch, private)}/{build_id}/"
    # before 4.3 the arch was not included in the path
    vtuple = tuple(int(f) for f in version.split("."))
    url += f"{meta_type}.json" if vtuple < (4, 3) else f"{brew_arch}/{meta_type}.json"
    with request.urlopen(url) as req:
        return json.loads(req.read().decode())


def latest_machine_os_content(version: str, brew_arch: str = "x86_64", private: bool = False) -> Tuple[Optional[str], Optional[str]]:
    """
    :param version: The major.minor of the RHCOS stream the build is associated with (e.g. '4.6')
    :param brew_arch: The CPU architecture for the build (uses brew naming convention)
    :param private: Whether this is a private build (NOT CURRENTLY SUPPORTED)
    :return: Returns (rhcos build id, image pullspec) or (None, None) if not found.
    """
    build_id = latest_rhcos_build_id(version, brew_arch, private)
    if build_id is None:
        return None, None
    m_os_c = rhcos_build_meta(build_id, version, brew_arch, private)['oscontainer']
    return build_id, m_os_c['image'] + "@" + m_os_c['digest']


def rhcos_content_tag(runtime) -> str:
    """
    :return: Return the tag for packages we expect RHCOS to be built from.
    """
    base = runtime.group_config.branch.replace("-rhel-7", "-rhel-8")
    return f"{base}-candidate"


class RHCOSBuildInspector:

    def __init__(self, runtime, pullspec_or_build_id: str, brew_arch: str):
        self.runtime = runtime
        self.brew_arch = brew_arch

        if pullspec_or_build_id[0].isdigit():
            self.build_id = pullspec_or_build_id
        else:
            pullspec = pullspec_or_build_id
            image_info_str, _ = exectools.cmd_assert(f'oc image info -o json {pullspec}', retries=3)
            image_info = Model(dict_to_model=json.loads(image_info_str))
            self.build_id = image_info.config.config.Labels.version
            if not self.build_id:
                raise Exception(f'Unable to determine MOSC build_id from: {pullspec}. Retrieved image info: {image_info_str}')

        # The first two digits of the RHCOS build are the major.minor of the rhcos stream name
        self.stream_version = self.build_id[0] + '.' + self.build_id[1]  # e.g. 43.82.202102081639.0 -> "4.3"

        self._build_meta = rhcos_build_meta(self.build_id, self.stream_version, self.brew_arch, meta_type='meta')
        self._os_commitmeta = rhcos_build_meta(self.build_id, self.stream_version, self.brew_arch, meta_type='commitmeta')

    def __str__(self):
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
        """
        entries = self.get_os_metadata()['rpmostree.rpmdb.pkglist']
        if not entries:
            raise Exception(f"no pkglist in OS Metadata for build {self.build_id}")
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
                rpm_def = koji_api.getRPM(nvra, strict=True)
                package_build = koji_api.getBuild(rpm_def['build_id'], brew.KojiWrapperOpts(caching=True), strict=True)
                package_name = package_build['package_name']
                aggregate[package_name] = package_build

        return aggregate

    def get_image_pullspec(self) -> str:
        build_meta = self.get_build_metadata()
        m_os_c = build_meta['oscontainer']
        return m_os_c['image'] + "@" + m_os_c['digest']

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

    def get_machine_os_content_digest(self) -> str:
        """
        Returns the image digest for the oscontainer image associated with this build.
        This is the sha of the machine-os-content which should be published out on quay.
        """
        return self._build_meta['oscontainer']['digest']

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
