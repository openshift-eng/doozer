import click
from doozerlib.cli import cli
from doozerlib import util


@cli.command("release:calc-upgrade-tests",
             short_help="Returns a list of recommended upgrade tests for an OCP release version")
@click.option("--version", required=True, help="The release version to calculate upgrade tests for (e.g. 4.6.31)")
def release_calc_upgrade_tests(version):
    arch = 'x86_64'
    arch = util.go_arch_for_brew_arch(arch)

    major, minor = util.extract_version_fields(version, at_least=2)[:2]
    candidate_channel = util.get_cincinnati_channels(major, minor)[0]
    if minor == 0:
        return
    prev_candidate_channel = util.get_cincinnati_channels(major, minor - 1)[0]

    prev_versions, prev_edges = util.get_channel_versions(prev_candidate_channel, arch)
    curr_versions, current_edges = util.get_channel_versions(candidate_channel, arch)

    prev_prev = f'{major}.{minor - 2}'
    prev_versions = [x for x in prev_versions if prev_prev not in x]
    version_list = util.sort_semver(set(prev_versions + curr_versions))

    # if 'hotfix' in version:
    #     def valid(v): return all([x not in v for x in ['rc', 'fc']])
    #     version_list = list(filter(valid, version_list))
    if not (('rc' in version) or ('fc' in version)):
        def valid(v): return all([x not in v for x in ['rc', 'fc', 'hotfix']])
        version_list = list(filter(valid, version_list))

    print(','.join(version_list))
