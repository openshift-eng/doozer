from __future__ import absolute_import, print_function, unicode_literals
import json
from tenacity import retry, stop_after_attempt, wait_fixed
from urllib import request

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
def rhcos_build_meta(build_id, version, arch="x86_64", private=False, meta_type="meta"):
    return _rhcos_build_meta(build_id, version, arch, private, meta_type)


def _rhcos_build_meta(build_id, version, arch="x86_64", private=False, meta_type="meta"):
    """
    rhcos metadata for an id in the given stream from the release browser.
    meta_type is "meta" for the build record or "commitmeta" for its ostree content.

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
    url += f"{meta_type}.json" if vtuple < (4, 3) else f"{arch}/{meta_type}.json"
    with request.urlopen(url) as req:
        return json.loads(req.read().decode())


def latest_machine_os_content(version, arch="x86_64", private=False):
    # returns the id and machine-os-content pullspec for the latest rhcos build in a stream
    build_id = latest_rhcos_build_id(version, arch, private)
    if build_id is None:
        return (None, None)
    m_os_c = rhcos_build_meta(build_id, version, arch, private)['oscontainer']
    return build_id, m_os_c['image'] + "@" + m_os_c['digest']


def rhcos_content_tag(runtime):
    # return the tag for packages we expect RHCOS to be built from
    base = runtime.group_config.branch.replace("-rhel-7", "-rhel-8")
    return f"{base}-candidate"
