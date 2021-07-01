import logging
from collections import OrderedDict
from logging import Logger
from typing import Dict, Iterable, List, Optional, Union

from kobo.rpmlib import parse_nvr
from koji import ClientSession

from doozerlib.assembly import assembly_metadata_config, assembly_rhcos_config
from doozerlib.brew import get_build_objects, list_archives_by_builds
from doozerlib.image import ImageMetadata
from doozerlib.model import Model
from doozerlib.rpmcfg import RPMMetadata
from doozerlib.util import find_latest_builds, strip_epoch, to_nvre


class PlashetBuilder:
    def __init__(self, koji_api: ClientSession, logger: Optional[Logger] = None) -> None:
        self._koji_api = koji_api
        self._logger = logger or logging.getLogger(__name__)
        self._build_cache: Dict[str, Optional[Dict]] = {}  # Cache build_id/nvre -> build_dict to prevent unnecessary queries.

    def _get_builds(self, ids_or_nvrs: Iterable[Union[int, str]]) -> List[Dict]:
        """ Get build dicts from Brew. This method uses an internal cache to avoid unnecessary queries.
        :params ids_or_nvrs: list of build IDs or NVRs
        :return: a list of Brew build dicts
        """
        cache_miss = set(ids_or_nvrs) - self._build_cache.keys()
        if cache_miss:
            cache_miss = [strip_epoch(item) if isinstance(item, str) else item for item in cache_miss]
            builds = get_build_objects(cache_miss, self._koji_api)
            for id_or_nvre, build in zip(cache_miss, builds):
                if build:
                    self._cache_build(build)
                else:
                    self._build_cache[id_or_nvre] = None  # None indicates the build ID or NVRE doesn't exist
        return [self._build_cache[id] for id in ids_or_nvrs]

    def _cache_build(self, build: Dict):
        """ Save build dict to cache """
        self._build_cache[build["build_id"]] = build
        self._build_cache[build["nvr"]] = build
        if "epoch" in build:
            self._build_cache[to_nvre(build)] = build

    def from_tag(self, tag: str, inherit: bool, assembly: Optional[str], event: Optional[int] = None) -> Dict[str, Dict]:
        """ Returns RPM builds from the specified brew tag
        :param tag: Brew tag name
        :param inherit: Descend into brew tag inheritance
        :param assembly: Assembly name to query. If None, this method will return true latest builds.
        :param event: Brew event ID
        :return: a dict; keys are component names, values are Brew build dicts
        """
        if not assembly:
            # Assemblies are disabled. We need the true latest tagged builds in the brew tag
            self._logger.info("Finding latest RPM builds in Brew tag %s...", tag)
            builds = self._koji_api.listTagged(tag, latest=True, inherit=inherit, event=event, type='rpm')
        else:
            # Assemblies are enabled. We need all tagged builds in the brew tag then find the latest ones for the assembly.
            self._logger.info("Finding RPM builds specific to assembly %s in Brew tag %s...", assembly, tag)
            tagged_builds = self._koji_api.listTagged(tag, latest=False, inherit=inherit, event=event, type='rpm')
            builds = find_latest_builds(tagged_builds, assembly)
        component_builds = {build["name"]: build for build in builds}
        self._logger.info("Found %s RPM builds.", len(component_builds))
        for build in component_builds.values():  # Save to cache
            self._cache_build(build)
        return component_builds

    def from_pinned_by_is(self, el_version: int, assembly: str, releases_config: Model, rpm_map: Dict[str, RPMMetadata]) -> Dict[str, Dict]:
        """ Returns RPM builds pinned by "is" in assembly config
        :param el_version: RHEL version
        :param assembly: Assembly name to query. If None, this method will return true latest builds.
        :param releases_config: a Model for releases.yaml
        :param rpm_map: Map of rpm_distgit_key -> RPMMetadata
        :return: a dict; keys are component names, values are Brew build dicts
        """
        pinned_nvrs: Dict[str, str] = {}  # rpms pinned to the runtime assembly; keys are rpm component names, values are nvrs
        component_builds: Dict[str, Dict] = {}  # rpms pinned to the runtime assembly; keys are rpm component names, values are brew build dicts

        # Honor pinned rpm nvrs pinned by "is"
        for distgit_key, rpm_meta in rpm_map.items():
            meta_config = assembly_metadata_config(releases_config, assembly, 'rpm', distgit_key, rpm_meta.config)
            nvr = meta_config["is"][f"el{el_version}"]
            if not nvr:
                continue
            nvre_obj = parse_nvr(str(nvr))
            if nvre_obj["name"] != rpm_meta.rpm_name:
                raise ValueError(f"RPM {nvr} is pinned to assembly {assembly} for distgit key {distgit_key}, but its package name is not {rpm_meta.rpm_name}.")
            pinned_nvrs[nvre_obj["name"]] = nvr
        if pinned_nvrs:
            pinned_nvr_list = list(pinned_nvrs.values())
            self._logger.info("Found %s NVRs pinned to the runtime assembly %s. Fetching build infos from Brew...", len(pinned_nvr_list), assembly)
            pinned_builds = self._get_builds(pinned_nvr_list)
            missing_nvrs = [nvr for nvr, build in zip(pinned_nvr_list, pinned_builds) if not build]
            if missing_nvrs:
                raise IOError(f"The following NVRs pinned by 'is' don't exist: {missing_nvrs}")
            for pinned_build in pinned_builds:
                component_builds[pinned_build["name"]] = pinned_build
        return component_builds

    def from_group_deps(self, el_version: int, group_config: Model, rpm_map: Dict[str, RPMMetadata]) -> Dict[str, Dict]:
        """ Returns RPM builds defined in group config dependencies
        :param el_version: RHEL version
        :param group_config: a Model for group config
        :param rpm_map: Map of rpm_distgit_key -> RPMMetadata
        :return: a dict; keys are component names, values are Brew build dicts
        """
        component_builds: Dict[str, Dict] = {}  # rpms pinned to the runtime assembly; keys are rpm component names, values are brew build dicts
        # honor group dependencies
        dep_nvrs = {parse_nvr(dep[f"el{el_version}"])["name"]: dep[f"el{el_version}"] for dep in group_config.dependencies.rpms if dep[f"el{el_version}"]}  # rpms for this rhel version listed in group dependencies; keys are rpm component names, values are nvrs
        if dep_nvrs:
            dep_nvr_list = list(dep_nvrs.values())
            self._logger.info("Found %s NVRs defined in group dependencies. Fetching build infos from Brew...", len(dep_nvr_list))
            dep_builds = self._get_builds(dep_nvr_list)
            missing_nvrs = [nvr for nvr, build in zip(dep_nvr_list, dep_builds) if not build]
            if missing_nvrs:
                raise IOError(f"The following group dependency NVRs don't exist: {missing_nvrs}")
            # Make sure group dependencies have no ART managed rpms.
            art_rpms_in_group_deps = {dep_build["name"] for dep_build in dep_builds} & {meta.rpm_name for meta in rpm_map.values()}
            if art_rpms_in_group_deps:
                raise ValueError(f"Unable to build plashet. Group dependencies cannot have ART managed RPMs: {art_rpms_in_group_deps}")
            for dep_build in dep_builds:
                component_builds[dep_build["name"]] = dep_build
        return component_builds

    def from_images(self, image_map: Dict[str, ImageMetadata]) -> Dict[str, List[Dict]]:
        """ Returns RPM builds used in images
        :param image_map: Map of image_distgit_key -> ImageMetadata
        :return: a dict; keys are image distgit keys, values are lists of RPM build dicts
        """
        image_builds: Dict[str, Dict] = OrderedDict()  # keys are image distgit keys, values are brew build dicts
        image_rpm_builds: Dict[str, List[Dict]] = OrderedDict()  # rpms in images; keys are image distgit keys, values are rpm build dicts used in that image

        self._logger.info("Finding image builds...")
        for distgit_key, image_meta in image_map.items():
            build = image_meta.get_latest_build(default=None, honor_is=False)
            if build:  # Ignore None in case we build for an basis event that is prior to the first build of an image
                image_builds[distgit_key] = build

        self._logger.info("Finding RPMs used in %s image builds...", len(image_builds))
        archive_lists = list_archives_by_builds([b["build_id"] for b in image_builds.values()], "image", self._koji_api)

        rpm_build_ids = {rpm["build_id"] for archives in archive_lists for ar in archives for rpm in ar["rpms"]}
        self._logger.info("Querying Brew build infos for %s RPM builds...", len(rpm_build_ids))
        rpm_builds = self._get_builds(rpm_build_ids)
        build_map = {b["build_id"]: b for b in rpm_builds}  # Maps rpm_build_id to build object from brew

        for distgit_key, archives in zip(image_builds.keys(), archive_lists):
            rpm_build_ids = {rpm["build_id"] for ar in archives for rpm in ar["rpms"]}
            image_rpm_builds[distgit_key] = [build_map[build_id] for build_id in rpm_build_ids]
        return image_rpm_builds

    def from_image_member_deps(self, el_version: int, assembly: str, releases_config: Model, image_meta: ImageMetadata, rpm_map: Dict[str, RPMMetadata]) -> Dict[str, Dict]:
        """ Returns RPM builds defined in image member dependencies
        :param el_version: RHEL version
        :param assembly: Assembly name to query. If None, this method will return true latest builds.
        :param releases_config: a Model for releases.yaml
        :param image_meta: An instance of ImageMetadata
        :param rpm_map: Map of rpm_distgit_key -> RPMMetadata
        :return: a dict; keys are component names, values are Brew build dicts
        """
        component_builds: Dict[str, Dict] = {}  # rpms pinned to the runtime assembly; keys are rpm component names, values are brew build dicts

        meta_config = assembly_metadata_config(releases_config, assembly, 'image', image_meta.distgit_key, image_meta.config)
        # honor image member dependencies
        dep_nvrs = {parse_nvr(dep[f"el{el_version}"])["name"]: dep[f"el{el_version}"] for dep in meta_config.dependencies.rpms if dep[f"el{el_version}"]}  # rpms for this rhel version listed in member dependencies; keys are rpm component names, values are nvrs
        if dep_nvrs:
            dep_nvr_list = list(dep_nvrs.values())
            self._logger.info("Found %s NVRs defined in image member '%s' dependencies. Fetching build infos from Brew...", len(dep_nvr_list), image_meta.distgit_key)
            dep_builds = self._get_builds(dep_nvr_list)
            missing_nvrs = [nvr for nvr, build in zip(dep_nvr_list, dep_builds) if not build]
            if missing_nvrs:
                raise IOError(f"The following image member dependency NVRs don't exist: {missing_nvrs}")
            # Make sure image member dependencies have no ART managed rpms.
            art_rpms_in_deps = {dep_build["name"] for dep_build in dep_builds} & {meta.rpm_name for meta in rpm_map.values()}
            if art_rpms_in_deps:
                raise ValueError(f"Unable to build plashet. Image member dependencies cannot have ART managed RPMs: {art_rpms_in_deps}")
            for dep_build in dep_builds:
                component_builds[dep_build["name"]] = dep_build
        return component_builds

    def from_rhcos_deps(self, el_version: int, assembly: str, releases_config: Model, rpm_map: Dict[str, Dict]):
        """ Returns RPM builds defined in RHCOS config dependencies
        :param el_version: RHEL version
        :param assembly: Assembly name to query. If None, this method will return true latest builds.
        :param releases_config: a Model for releases.yaml
        :param rpm_map: Map of rpm_distgit_key -> RPMMetadata
        :return: a dict; keys are component names, values are Brew build dicts
        """
        component_builds: Dict[str, Dict] = {}  # keys are rpm component names, values are brew build dicts
        rhcos_config = assembly_rhcos_config(releases_config, assembly)
        # honor RHCOS dependencies
        # rpms for this rhel version listed in RHCOS dependencies; keys are rpm component names, values are nvrs
        dep_nvrs = {parse_nvr(dep[f"el{el_version}"])["name"]: dep[f"el{el_version}"] for dep in rhcos_config.dependencies.rpms if dep[f"el{el_version}"]}
        if dep_nvrs:
            dep_nvr_list = list(dep_nvrs.values())
            self._logger.info("Found %s NVRs defined in RHCOS dependencies. Fetching build infos from Brew...", len(dep_nvr_list))
            dep_builds = self._get_builds(dep_nvr_list)
            missing_nvrs = [nvr for nvr, build in zip(dep_nvr_list, dep_builds) if not build]
            if missing_nvrs:
                raise IOError(f"The following RHCOS dependency NVRs don't exist: {missing_nvrs}")
            # Make sure RHCOS dependencies have no ART managed rpms.
            art_rpms_in_rhcos_deps = {dep_build["name"] for dep_build in dep_builds} & {meta.rpm_name for meta in rpm_map.values()}
            if art_rpms_in_rhcos_deps:
                raise ValueError(f"Unable to build plashet. Group dependencies cannot have ART managed RPMs: {art_rpms_in_rhcos_deps}")
            for dep_build in dep_builds:
                component_builds[dep_build["name"]] = dep_build
        return component_builds
