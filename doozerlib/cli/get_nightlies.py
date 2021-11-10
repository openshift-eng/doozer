import click
import json
from datetime import datetime
from urllib import request
from doozerlib.cli import cli
from doozerlib import constants, util


@cli.command("get-nightlies", short_help="Get sets of Accepted nightlies. A set contains nightly for each arch, "
                                         "determined by closest timestamps")
@click.option("--limit", help="Number of sets of passing nightlies to print", default=10)
@click.pass_obj
def get_nightlies(runtime, limit):
    limit = int(limit)
    runtime.initialize(clone_distgits=False)
    major = runtime.group_config.vars.MAJOR
    minor = runtime.group_config.vars.MINOR
    version = f'{major}.{minor}'

    nightlies = {}
    for arch in util.go_arches:
        if version < '4.9' and arch == 'arm64':
            continue
        nightlies[arch] = all_accepted_nightlies(major, minor, arch)

    i = 0
    for nightly in nightlies["amd64"]:
        if i >= limit:
            break
        n = []
        for arch in util.go_arches:
            if version < '4.9' and arch == 'arm64':
                continue
            n.append(get_closest_nightly(nightlies[arch], nightly))
        print(",".join(n))
        i += 1


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
