import json
from typing import Dict
from urllib import request
from urllib.parse import quote

import aiohttp
from doozerlib.exectools import cmd_assert_async
from tenacity import retry, stop_after_attempt, wait_fixed

RHCOS_BASE_URL = "https://releases-rhcos-art.cloud.privileged.psi.redhat.com/storage/releases"


def rhcos_release_url(version, arch="x86_64", private=False):
    """
    base url for a release stream in the release browser (AWS bucket).

    @param version  The 4.y ocp version as a string (e.g. "4.6")
    @param arch  architecture we are interested in (e.g. "s390x")
    @param private  boolean, true for private stream, false for public (currently, no effect)
    @return e.g. "https://releases-rhcos-art...com/storage/releases/rhcos-4.6-s390x"
    """
    arch_suffix = "" if arch in ["x86_64", "amd64"] else f"-{arch}"
    # TODO: create private rhcos builds and do something with "private" here
    return f"{RHCOS_BASE_URL}/rhcos-{version}{arch_suffix}"


# this is hard to test with retries, so wrap testable method
@retry(reraise=True, stop=stop_after_attempt(10), wait=wait_fixed(3))
def latest_rhcos_build_id(version, arch="x86_64", private=False):
    return _latest_rhcos_build_id(version, arch, private)


def _latest_rhcos_build_id(version, arch="x86_64", private=False):
    # returns the build id string or None (or raise exception)
    # (may want to return "schema-version" also if this ever gets more complex)
    with request.urlopen(f"{rhcos_release_url(version, arch, private)}/builds.json") as req:
        data = json.loads(req.read().decode())
    if not data["builds"]:
        return None
    build = data["builds"][0]
    # old schema just had the id as a string; newer has it in a dict
    return build if isinstance(build, str) else build["id"]


# this is hard to test with retries, so wrap testable method
@retry(reraise=True, stop=stop_after_attempt(10), wait=wait_fixed(3))
def rhcos_build_meta(build_id, version, arch="x86_64", private=False):
    return _rhcos_build_meta(build_id, version, arch, private)


def _rhcos_build_meta(build_id, version, arch="x86_64", private=False):
    """
    rhcos build record for an id in the given stream in the release browser

    @return  a "meta" build record e.g.:
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
    url = f"{rhcos_release_url(version, arch, private)}/{build_id}/"
    # before 4.3 the arch was not included in the path
    vtuple = tuple(int(f) for f in version.split("."))
    url += "meta.json" if vtuple < (4, 3) else f"{arch}/meta.json"
    with request.urlopen(url) as req:
        return json.loads(req.read().decode())


def latest_machine_os_content(version, arch="x86_64", private=False):
    # returns the id and machine-os-content pullspec for the latest rhcos build in a stream
    build_id = latest_rhcos_build_id(version, arch, private)
    if build_id is None:
        return (None, None)
    m_os_c = rhcos_build_meta(build_id, version, arch, private)['oscontainer']
    return build_id, m_os_c['image'] + "@" + m_os_c['digest']


async def get_rhcos_pullspec_from_image_stream(is_name: str, is_namespace: str, rhcos_tag: str) -> str:
    """ Get RHCOS pullspec from an image stream
    :param is_name: image stream name
    :param is_namspace: image stream name
    :param rhcos_tag: RHCOS image tag in the image stream. e.g. machine-os-content
    :returns: RHCOS pullspec
    """
    cmd = ["oc", "-n", is_namespace, "get", "is", is_name, "-o", "json"]
    out, _ = await cmd_assert_async(cmd)
    image_stream = json.loads(out)
    tags = image_stream["status"].get("tags")
    if not tags:
        raise ValueError(f"No tags in imagestream {is_namespace}/{is_name}.")
    rhcos_entry = next(filter(lambda tag: tag["tag"] == rhcos_tag, tags), None)
    if not rhcos_entry:
        raise ValueError(f"No such tag named {rhcos_tag} in imagestream {is_namespace}/{is_name}.")
    pullspec = rhcos_entry["items"][0]["dockerImageReference"]
    return pullspec


async def get_rhcos_version_arch(pullspec: str) -> (str, str):
    """ Get RHCOS version and arch for a given RHCOS pullspec
    :param pullspec: RHCOS pullspec
    :returns: (version, arch)
    """
    cmd = ["oc", "image", "info", "-o", "json", pullspec]
    out, _ = await cmd_assert_async(cmd)
    image_manifest = json.loads(out)
    version = image_manifest["config"]["config"]["Labels"]["version"]
    arch = image_manifest["config"]["architecture"]
    if arch == "amd64":
        arch = "x86_64"  # convert golang arch name to gcc arch name
    return version, arch


async def get_rhcos_build_metadata(version: str, arch: str) -> Dict:
    """ Get RHCOS build metadata
    :version: RHCOS build version
    :arch: RHCOS architecture
    :returns: build metadata. e.g. https://releases-rhcos-art.cloud.privileged.psi.redhat.com/storage/releases/rhcos-4.6-ppc64le/46.82.202008271839-0/ppc64le/meta.json
    """
    # Assuming RHCOS versions like 46.82.202009222340-0 are in stream `releases/rhcos-4.6`
    rhcos_major_version = int(version.split(".", 1)[0])
    major = rhcos_major_version // 10
    minor = rhcos_major_version % 10
    arch_suffix = f"-{arch}" if arch != "x86_64" else ""
    stream = f"releases/rhcos-{major}.{minor}{arch_suffix}"
    url = f"https://releases-rhcos-art.cloud.privileged.psi.redhat.com/storage/{stream}/{quote(version)}/{arch}/meta.json"
    async with aiohttp.ClientSession() as session:
        response = await session.get(url)
        if response.status == 403:  # The URL returns 403 instread of 404 if the RHCOS build doesn't exist
            return None
        response.raise_for_status()
        metadata = await response.json()
    return metadata


async def get_rhcos_pullspec_from_release(release_pullspec: str, rhcos_tag: str) -> str:
    """ Get RHCOS pullspec from a given release
    :param release_pullspec: pullspec of the release
    :param rhcos_tag: RHCOS image tag in the image stream. e.g. machine-os-content
    :returns: RHCOS pullspec
    """
    cmd = ["oc", "adm", "release", "info", release_pullspec, "-o", "json"]
    out, _ = await cmd_assert_async(cmd)
    release_info = json.loads(out)
    tags = release_info["references"]["spec"]["tags"]
    rhcos_entry = next(filter(lambda tag: tag["name"] == rhcos_tag, tags), None)
    if not rhcos_entry:
        raise ValueError(f"No such tag named {rhcos_tag} in {release_pullspec}.")
    pullspec = rhcos_entry["from"]["name"]
    return pullspec
