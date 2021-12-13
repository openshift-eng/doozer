import click

from doozerlib.rhcos import RHCOSBuildInspector, RHCOSBuildFinder
from doozerlib.cli import cli
from doozerlib.util import brew_arches
from doozerlib.cli.release_gen_payload import PayloadGenerator
from doozerlib.assembly import AssemblyIssueCode


@cli.command("inspect:stream", short_help="Inspect stream assembly for assembly issues")
@click.argument("code", type=click.Choice([code.name for code in AssemblyIssueCode]), required=True)
@click.pass_obj
def inspect_stream(runtime, code):
    runtime.initialize(clone_distgits=False)
    logger = runtime.logger
    if runtime.assembly != 'stream':
        logger.warning(f'Disregarding non-stream assembly: {runtime.assembly}. This command is only intended for '
                       f'stream')


    if code == AssemblyIssueCode.INCONSISTENT_RHCOS_RPMS.name:
        rhcos_builds, rhcos_inconsistencies = _check_inconsistent_rhcos_rpms(runtime)
        if rhcos_inconsistencies:
            print(f'Found RHCOS inconsistencies in builds {rhcos_builds}: {rhcos_inconsistencies}')
            exit(1)
        print(f'RHCOS builds consistent {rhcos_builds}')
        exit(0)
    else:
        print(f'AssemblyIssueCode {code} not supported at this time :(')
        exit(1)

def _check_inconsistent_rhcos_rpms(runtime):
    logger = runtime.logger
    major = runtime.group_config.vars.MAJOR
    minor = runtime.group_config.vars.MINOR
    version = f'{major}.{minor}'
    not_arm = major == 4 and minor < 9
    rhcos_builds = []
    for arch in brew_arches:
        if not_arm and arch == 'aarch64':
            continue
        build_id, pullspec = RHCOSBuildFinder(runtime, version, arch, False).latest_machine_os_content()
        if not pullspec:
            raise IOError(f"No RHCOS latest found for {version} / {arch}")
        rhcos_builds.append(RHCOSBuildInspector(runtime, pullspec, arch))
    logger.info(f"Checking following builds for inconsistency: {rhcos_builds}")
    rhcos_inconsistencies = PayloadGenerator.find_rhcos_build_rpm_inconsistencies(rhcos_builds)
    return rhcos_builds, rhcos_inconsistencies
