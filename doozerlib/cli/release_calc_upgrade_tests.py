import click
from doozerlib.cli import cli
from doozerlib import util


@cli.command("release:calc-upgrade-tests",
             short_help="Returns a list of recommended upgrade tests for an OCP release version")
@click.option("--version", required=True, help="The release version to calculate upgrade tests for (e.g. 4.6.31)")
def release_calc_upgrade_tests(version):
    arch = 'x86_64'
    versions = util.get_release_calc_previous(version, arch)
    major, minor = util.extract_version_fields(version, at_least=2)[:2]
    curr_versions = [x for x in versions if f'{major}.{minor}' in x]
    prev_versions = [x for x in versions if f'{major}.{minor-1}' in x]

    def get_test_edges(version_list):
        test_edges = []
        sorted_list = util.sort_semver(version_list)
        test_edges += sorted_list[:5]  # add first 5
        test_edges += sorted_list[-5:]  # add last 5
        # 5 equally distributed between remaining
        remaining = sorted_list[5:-5]
        if len(remaining) < 5:
            test_edges += remaining
        else:
            step = len(remaining) // 5
            test_edges += remaining[step::step]
        return util.sort_semver(set(test_edges))

    edges = get_test_edges(curr_versions) + get_test_edges(prev_versions)
    print(','.join(edges))
