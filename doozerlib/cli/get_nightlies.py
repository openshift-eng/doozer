import click
import json
import sys
from urllib import request
from doozerlib.cli import cli
from doozerlib import constants, util, exectools

# See https://github.com/openshift/machine-config-operator/blob/master/docs/OSUpgrades.md
# But in the future this will be replaced, see https://github.com/coreos/enhancements/blob/main/os/coreos-layering.md
OLD_FORMAT_COREOS_TAG = 'machine-os-content'


@cli.command("get-nightlies", short_help="Get sets of Accepted nightlies. A set contains nightly for each arch, "
                                         "determined by closest timestamps")
@click.option("--limit", default=1, metavar='NUM', help="Number of sets of passing nightlies to print")
@click.option("--rhcos", is_flag=True, help="Print rhcos build id with nightlies")
@click.option("--latest", is_flag=True, help="Just get the latest nightlies for all arches (Accepted or not)")
@click.pass_obj
def get_nightlies(runtime, limit, rhcos, latest):
    if latest and limit > 1:
        print("Don't use --latest and --limit > 1", file=sys.stderr)
        exit(1)

    limit = int(limit)
    runtime.initialize(clone_distgits=False)
    major = runtime.group_config.vars.MAJOR
    minor = runtime.group_config.vars.MINOR

    not_arm = major == 4 and minor < 9
    nightlies_with_phase = {}

    def ignore_arch(arch):
        return (arch == 'arm64' and not_arm) or arch == 'multi'

    for arch in util.go_arches:
        if ignore_arch(arch):
            continue
        phase = 'Accepted'
        if latest:
            phase = ''
        nightlies_with_phase[arch] = all_nightlies_in_phase(major, minor, arch, phase)

    i = 0
    for x64_nightly, x64_phase in nightlies_with_phase["amd64"]:
        if i >= limit:
            break
        nightly_set = []
        for arch in util.go_arches:
            if ignore_arch(arch):
                continue
            if latest:
                nightly, phase = nightlies_with_phase[arch][i]
            else:
                nightly, phase = get_closest_nightly(nightlies_with_phase[arch], x64_nightly)
            nightly_set.append(nightly)
            nightly_str = f'{nightly} {phase}'
            if rhcos:
                if phase != 'Pending':
                    rhcos = get_coreos_build_from_payload(get_nightly_pullspec(nightly, arch))
                    nightly_str += f' {rhcos}'
            print(nightly_str)
        print(",".join(nightly_set))
        i += 1


def get_nightly_pullspec(release, arch):
    suffix = util.go_suffix_for_arch(arch)
    return f'registry.ci.openshift.org/ocp{suffix}/release{suffix}:{release}'


def get_coreos_build_from_payload(payload_pullspec):
    """Retrive the build version of machine-os-content (e.g. 411.86.202206131434-0)"""
    out, err = exectools.cmd_assert(["oc", "adm", "release", "info", "--image-for", OLD_FORMAT_COREOS_TAG, "--", payload_pullspec])
    if err:
        raise Exception(f"Error running oc adm: {err}")
    rhcos_pullspec = out.split('\n')[0]
    out, err = exectools.cmd_assert(["oc", "image", "info", "-o", "json", rhcos_pullspec])
    if err:
        raise Exception(f"Error running oc adm: {err}")
    image_info = json.loads(out)
    build_id = image_info["config"]["config"]["Labels"]["version"]
    return build_id


def get_closest_nightly(nightly_with_phase_list, nightly):
    target_ts = util.get_release_tag_datetime(nightly)
    min_delta, min_nightly = None, None
    for nightly, phase in nightly_with_phase_list:
        nightly_ts = util.get_release_tag_datetime(nightly)
        delta = target_ts - nightly_ts
        local_delta = abs(delta.total_seconds())
        if min_delta is None or local_delta < min_delta:
            min_delta = local_delta
            min_nightly = (nightly, phase)
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


def all_nightlies_in_phase(major, minor, arch, phase):
    # returns the build id string or None (or raise exception), one for each arch
    # (may want to return "schema-version" also if this ever gets more complex)
    tag = f'{major}.{minor}.0-0.nightly'
    rc_url = f"{rc_api_url(tag, arch)}/tags"
    if phase:
        rc_url += f'?phase={phase}'
    with request.urlopen(rc_url) as req:
        data = json.loads(req.read().decode())
    if not data["tags"]:
        return None
    nightlies = [(tag["name"], tag["phase"]) for tag in data["tags"]]
    return nightlies
