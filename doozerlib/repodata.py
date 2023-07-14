import asyncio
import gzip
import io
import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from logging import Logger
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib import parse

import aiohttp
from ruamel.yaml import YAML
from tenacity import before_sleep_log, retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from doozerlib import rpm_utils

LOGGER = logging.getLogger(__name__)
NAMESPACES = {
    "repo": "http://linux.duke.edu/metadata/repo",
    "rpm": "http://linux.duke.edu/metadata/rpm",
    'common': 'http://linux.duke.edu/metadata/common',
}


@dataclass
class Rpm:
    name: str
    epoch: int
    version: str
    release: str
    arch: str

    @property
    def nevra(self):
        return f"{self.name}-{self.epoch}:{self.version}-{self.release}.{self.arch}"

    @property
    def nvr(self):
        return f"{self.name}-{self.version}-{self.release}"

    def compare(self, another: "Rpm"):
        evr1 = (str(self.epoch), self.version, self.release)
        evr2 = (str(another.epoch), another.version, another.release)
        return rpm_utils.labelCompare(evr1, evr2)

    def __repr__(self) -> str:
        return self.nevra

    def to_dict(self):
        return {
            "name": self.name,
            "epoch": str(self.epoch),
            "version": self.version,
            "release": self.release,
            "arch": self.arch,
            "nvr": self.nvr,
            "nevra": self.nevra,
        }

    @staticmethod
    def from_nevra(nevra: str):
        nevr, arch = nevra.rsplit(".", maxsplit=1)  # foo-0:1.2.3-1.x86_64 => (foo-0:1.2.3-1, x86_64)
        nvrea_dict = rpm_utils.parse_nvr(nevr)
        nvrea_dict["arch"] = arch
        return Rpm.from_dict(nvrea_dict)

    @staticmethod
    def from_dict(nvrea_dict: Dict):
        epoch = nvrea_dict.get("epoch") or "0"
        return Rpm(
            name=nvrea_dict["name"],
            epoch=int(epoch),
            version=nvrea_dict["version"],
            release=nvrea_dict["release"],
            arch=nvrea_dict["arch"],
        )

    @staticmethod
    def from_metadata(metadata: ET.Element):
        name = metadata.find("common:name", NAMESPACES)
        if name is None or not name.text:
            raise ValueError("name is not set")
        version = metadata.find("common:version", NAMESPACES)
        if version is None:
            raise ValueError("version is not set")
        arch = metadata.find("common:arch", NAMESPACES)
        if arch is None or not arch.text:
            raise ValueError("arch is not set")
        return Rpm(
            name=name.text,
            epoch=int(version.attrib["epoch"]),
            version=version.attrib["ver"],
            release=version.attrib["rel"],
            arch=arch.text,
        )


@dataclass
class RpmModule:
    name: str
    stream: str
    version: int
    context: str
    arch: str
    rpms: Set[str] = field(default_factory=set)

    @property
    def name_stream(self):
        return f"{self.name}:{self.stream}"

    @property
    def name_stream_version(self):
        return f"{self.name}:{self.stream}:{self.version}"

    @property
    def nsvca(self) -> str:
        nsvca = f"{self.name}:{self.stream}:{self.version}:{self.context}:{self.arch}"
        return nsvca

    def __repr__(self) -> str:
        return self.nsvca

    @staticmethod
    def from_metadata(metadata: Dict[str, Any]):
        rpms = metadata["data"].get("artifacts", {}).get("rpms", [])
        return RpmModule(
            name=metadata["data"]["name"],
            stream=metadata["data"]["stream"],
            version=metadata["data"]["version"],
            context=metadata["data"]["context"],
            arch=metadata["data"]["arch"],
            rpms=set(rpms),
        )


@dataclass
class Repodata:
    name: str
    primary_rpms: List[Rpm] = field(default_factory=list)
    modules: List[RpmModule] = field(default_factory=list)

    @staticmethod
    def from_metadatas(name: str, primary: ET.Element, modules_yaml: List[Dict]):
        primary_rpms = [Rpm.from_metadata(metadata) for metadata in primary.findall("common:package[@type='rpm']", NAMESPACES)]
        modules = [RpmModule.from_metadata(metadata) for metadata in modules_yaml if metadata['document'] == 'modulemd']
        repodata = Repodata(
            name=name,
            primary_rpms=primary_rpms,
            modules=modules,
        )
        return repodata


class RepodataLoader:
    @staticmethod
    async def _fetch_remote_gzip(session: aiohttp.ClientSession, url: Optional[str]):
        if not url:
            return b''
        data = io.BytesIO()
        async with session.get(url) as resp:
            resp.raise_for_status()
            data = io.BytesIO(await resp.read())
        data.seek(0)
        with gzip.GzipFile(fileobj=data) as uncompressed:
            return uncompressed.read()

    async def load(self, repo_name: str, repo_url: str):
        if not repo_url.endswith("/"):
            repo_url += "/"
        repomd_url = parse.urljoin(repo_url, "repodata/repomd.xml")

        timeout = aiohttp.ClientTimeout(total=60 * 10)
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=32, force_close=True), timeout=timeout) as session:
            async with session.get(repomd_url) as resp:
                resp.raise_for_status()
                repomd_xml = ET.fromstring(await resp.text())

            primary_data_element = repomd_xml.find('repo:data[@type="primary"]', NAMESPACES)
            if primary_data_element is None:
                raise ValueError("Couldn't find primary data in repodata")
            primary_location = primary_data_element.find('repo:location', NAMESPACES)
            if primary_location is None:
                raise ValueError("Couldn't find primary location in repodata")
            primary_url = parse.urljoin(repo_url, primary_location.attrib['href'])

            modules_url = None
            modules_data_element = repomd_xml.find('repo:data[@type="modules"]', NAMESPACES)
            if modules_data_element is not None:
                modules_location = modules_data_element.find('repo:location', NAMESPACES)
                if modules_location is None:
                    raise ValueError("Couldn't find modules location in repodata")
                modules_url = parse.urljoin(repo_url, modules_location.attrib['href'])

            @retry(reraise=True, stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10),
                   retry=(retry_if_exception_type((aiohttp.ServerDisconnectedError, aiohttp.ClientResponseError, aiohttp.ClientPayloadError))),
                   before_sleep=before_sleep_log(LOGGER, logging.WARNING))
            async def fetch_remote_gzip(url: Optional[str]):
                return await self._fetch_remote_gzip(session, url)

            primary_bytes, modules_bytes = await asyncio.gather(
                fetch_remote_gzip(primary_url),
                fetch_remote_gzip(modules_url),
            )

        yaml = YAML(typ='safe')
        repodata = Repodata.from_metadatas(
            repo_name,
            ET.fromstring(primary_bytes),
            list(yaml.load_all(modules_bytes) if modules_bytes else [])
        )
        return repodata


class OutdatedRPMFinder:

    @staticmethod
    def _find_candidate_modular_rpms(all_modules, enabled_streams):
        """ Finds all candidate modular rpms in enabled module streams
        """
        # Find the latest module versions for each enabled streams
        latest_modules: Dict[str, Dict[str, Tuple[str, RpmModule]]] = {}  # module_stream => context => (repo_name, RpmModule)
        for module_stream, allowed_contexts in enabled_streams.items():
            module_versions = sorted(all_modules[module_stream].keys(), reverse=True)  # module versions are sorted from newest to oldest
            latest_modules[module_stream] = {}
            for version in module_versions:
                for update_repo, update_module in all_modules[module_stream][version]:
                    if update_module.context not in allowed_contexts:
                        continue  # This module has a different "context"; ignoring
                    if update_module.context in latest_modules[module_stream]:
                        continue  # a newer version has been found
                    latest_modules[module_stream][update_module.context] = (update_repo, update_module)
        # Finally populate candidate_modular_rpms
        candidate_modular_rpms: Dict[str, Tuple[str, Rpm]] = {}  # package_name => (repo_name, rpm)
        for _, context_modules in latest_modules.items():
            for _, repo_module in context_modules.items():
                repo, module = repo_module
                for nevra in module.rpms:
                    rpm = Rpm.from_nevra(nevra)
                    _, candidate = candidate_modular_rpms.get(rpm.name, (None, None))
                    if not candidate or rpm.compare(candidate) > 0:
                        candidate_modular_rpms[rpm.name] = (repo, rpm)
        return candidate_modular_rpms

    @staticmethod
    def _find_candidate_non_modular_rpms(all_non_modular_rpms, candidate_modular_rpms, logger):
        """ Finds all candidate non-modular rpms.
        For each non-modular rpm, if there is another candidate modular rpm with the same package name,
        the non-modular rpm will be exempted.
        """
        candidate_non_modulear_rpms: Dict[str, Tuple[str, Rpm]] = {}  # package_name => (repo_name, rpm)
        for nevra, repo in all_non_modular_rpms.items():
            rpm = Rpm.from_nevra(nevra)
            _, candidate = candidate_non_modulear_rpms.get(rpm.name, (None, None))
            if not candidate or rpm.compare(candidate) > 0:
                if rpm.name in candidate_modular_rpms:
                    modular_repo, modular_rpm = candidate_modular_rpms[rpm.name]
                    logger.debug("Non-modular RPM %s from %s is shadowed by modular RPM %s from %s", nevra, repo, modular_rpm.nevra, modular_repo)
                    continue  # This non-modular rpm is shadowed by a modular rpm
                candidate_non_modulear_rpms[rpm.name] = (repo, rpm)
        return candidate_non_modulear_rpms

    def find_non_latest_rpms(self, rpms_to_check: List[Dict], repodatas: List[Repodata], logger: Optional[Logger] = None) -> List[Tuple[str, str, str]]:
        """
        Finds non-latest rpms.

        :param rpms_to_check: a list of RPMs to check
        :param repodata: a list of YUM repos.
        :return: Returns a list of outdated rpms in the form of (installed_rpm, latest_rpm, repo_name)
        """
        # Determine which repos are enabled for the image
        logger = logger or logging.getLogger(__name__)

        # archive_rpms holds all rpms to examine
        archive_rpms = {rpm['name']: rpm for rpm in rpms_to_check}  # rpm_name => rpm dict

        # To correctly detect outdated rpms coming from modular repos, we need to know which modules are enabled during image build.
        # However, this is no Brew API or any other easy way to know that.
        # To work around this limitation, the following approach is used:
        # 1. List all module streams and their modular rpms in enabled repos.
        # 2. For each installed rpm, check if the rpm is contained by a module stream.
        # 3. If yes, we will consider that module stream is "enabled" for this image.
        # This approach is not perfect, but it should be good enough for our use cases.

        logger.info("Determining which module streams are enabled")
        # Populate dicts to hold all modules and all modular rpms
        all_modules: Dict[str, Dict[int, List[Tuple[str, RpmModule]]]] = {}  # module_name_stream => version => [(repo_name, module_object)]
        all_modular_rpms: Dict[str, Dict[str, Dict[str, RpmModule]]] = {}  # rpm_nvera => repo_name => module_nsvca => module_object
        for repodata in repodatas:
            for module in repodata.modules:
                all_modules.setdefault(module.name_stream, {}).setdefault(module.version, []).append((repodata.name, module))
                for nevra in module.rpms:
                    all_modular_rpms.setdefault(nevra, {}).setdefault(repodata.name, {})[module.nsvca] = module

        # Populate a dict to hold enabled module streams
        enabled_streams: Dict[str, Set[str]] = {}  # module_stream => {context}
        for name, archive_rpm in archive_rpms.items():
            rpm = Rpm.from_dict(archive_rpm)
            if rpm.nevra in all_modular_rpms:
                for repo_name, modules in all_modular_rpms[rpm.nevra].items():
                    for _, module in modules.items():
                        enabled_streams.setdefault(module.name_stream, set()).add(module.context)

        # Populate candidate_modular_rpms, which will hold visible modular rpms that are latest among all configured repos
        candidate_modular_rpms: Dict[str, Tuple[str, Rpm]] = {}  # package_name => (repo_name, rpm)
        if not enabled_streams:
            logger.info("Looks like no module streams are enabled")
        else:
            candidate_modular_rpms = self._find_candidate_modular_rpms(all_modules, enabled_streams)

        # Populate a dict to hold all non-modular rpms
        all_non_modular_rpms: Dict[str, str] = {}  # rpm_nvera => repo_name
        for repodata in repodatas:
            for rpm in repodata.primary_rpms:
                if rpm.nevra in all_modular_rpms:
                    continue  # It is a modular rpm
                all_non_modular_rpms[rpm.nevra] = repodata.name

        # Populate candidate_non_modulear_rpms, which will hold all visible non-modular rpms that are latest among all configured repos
        candidate_non_modulear_rpms = self._find_candidate_non_modular_rpms(all_non_modular_rpms, candidate_modular_rpms, logger)

        # Compare archive rpms to all candidate rpms
        results: List[Tuple[str, str, str]] = []
        for name, archive_rpm in archive_rpms.items():
            archive_rpm = Rpm.from_dict(archive_rpm)
            repo, candidate_rpm = None, None
            if archive_rpm.nevra in all_modular_rpms:  # Archive rpm is a modular rpm
                repo, candidate_rpm = candidate_modular_rpms.get(name, (None, None))
            else:  # Archive rpm is a non-modular rpm
                repo, candidate_rpm = candidate_non_modulear_rpms.get(name, (None, None))
            if not repo or not candidate_rpm:
                continue  # Archive rpm rpm is not available in any configured repos
            if archive_rpm.compare(candidate_rpm) < 0:  # Archive rpm is older than candidate rpm
                results.append((archive_rpm.nevra, candidate_rpm.nevra, repo))
        return results
