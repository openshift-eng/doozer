import click
import json
from datetime import datetime
from urllib import request
from doozerlib.cli import cli
from doozerlib import constants, util, exectools


@cli.command("get-nightlies", short_help="Get sets of Accepted nightlies. A set contains nightly for each arch, "
                                         "determined by closest timestamps")
@click.option("--limit", default=1, metavar='NUM', help="Number of sets of passing nightlies to print")
@click.option("--rhcos", is_flag=True, help="Print rhcos build id with nightlies")
@click.pass_obj
def get_nightlies(runtime, limit, rhcos):
    limit = int(limit)
    runtime.initialize(clone_distgits=False)
    major = runtime.group_config.vars.MAJOR
    minor = runtime.group_config.vars.MINOR

    not_arm = major == 4 and minor < 9
    nightlies = {}
    for arch in util.go_arches:
        if arch == 'arm64' and not_arm:
            continue
        nightlies[arch] = all_accepted_nightlies(major, minor, arch)

    i = 0
    for x64_nightly in nightlies["amd64"]:
        if i >= limit:
            break
        nightly_set = []
        rhcos_set = {}
        for arch in util.go_arches:
            if arch == 'arm64' and not_arm:
                continue
            nightly = get_closest_nightly(nightlies[arch], x64_nightly)
            nightly_set.append(nightly)
            if rhcos:
                rhcos_set[arch] = get_build_from_payload(get_nightly_pullspec(nightly, arch))
        print(",".join(nightly_set))
        if rhcos:
            print(rhcos_set)
        i += 1


def get_nightly_pullspec(release, arch):
    suffix = util.go_suffix_for_arch(arch)
    return f'registry.ci.openshift.org/ocp{suffix}/release{suffix}:{release}'


def get_build_from_payload(payload_pullspec):
    rhcos_tag = 'machine-os-content'
    out, err = exectools.cmd_assert(["oc", "adm", "release", "info", "--image-for", rhcos_tag, "--", payload_pullspec])
    if err:
        raise Exception(f"Error running oc adm: {err}")
    rhcos_pullspec = out.split('\n')[0]
    out, err = exectools.cmd_assert(["oc", "image", "info", "-o", "json", rhcos_pullspec])
    if err:
        raise Exception(f"Error running oc adm: {err}")
    image_info = json.loads(out)
    build_id = image_info["config"]["config"]["Labels"]["version"]
    return build_id


def get_closest_nightly(nightly_list, nightly):
    target_ts = util.get_release_tag_datetime(nightly)
    min, min_nightly = None, None
    for i in nightly_list:
        nightly_ts = util.get_release_tag_datetime(i)
        delta = target_ts - nightly_ts
        m = abs(delta.total_seconds())
        if min is None or m < min:
            min = m
            min_nightly = i
    return min_nightly


def rc_api_url(tag, arch):
    """
    base url for a release tag in release controller.

    @param tag  The RC release tag as a string (e.g. "4.9.0-0.nightly")
    @param arch  architecture we are interested in (e.g. "s390x")
    @return e.g. "https://s390x.ocp.releases.ci.openshift.org/api/v1/releasestream/4.9.0-0.nightly-s390x"
    """
    arch = util.go_arch_for_brew_arch(arch)
    arch_suffix = util.go_suffix_for_arch(arch)
    return f"{constants.RC_BASE_URL.format(arch=arch)}/api/v1/releasestream/{tag}{arch_suffix}"


def all_accepted_nightlies(major, minor, arch):
    # returns the build id string or None (or raise exception), one for each arch
    # (may want to return "schema-version" also if this ever gets more complex)
    tag = f'{major}.{minor}.0-0.nightly'
    with request.urlopen(f"{rc_api_url(tag, arch)}/tags?phase=Accepted") as req:
        data = json.loads(req.read().decode())
    if not data["tags"]:
        return None
    nightlies = [tag["name"] for tag in data["tags"]]
    return nightlies
