import click

from doozerlib.rhcos import RHCOSBuildInspector, RHCOSBuildFinder
from doozerlib.cli import cli
from doozerlib.util import brew_arches
from doozerlib.cli.release_gen_payload import PayloadGenerator
from doozerlib.assembly import AssemblyIssueCode
from doozerlib.assembly import AssemblyIssue
from doozerlib.assembly_inspector import AssemblyInspector


@cli.command("inspect:stream", short_help="Inspect stream assembly for assembly issues")
@click.argument("code", type=click.Choice([code.name for code in AssemblyIssueCode], case_sensitive=False),
                required=True)
@click.pass_obj
def inspect_stream(runtime, code):
    code = AssemblyIssueCode[code]
    if runtime.assembly != 'stream':
        print(f'Disregarding non-stream assembly: {runtime.assembly}. This command is only intended for stream')
    runtime.assembly = 'stream'
    runtime.initialize(clone_distgits=False)
    assembly_inspector = AssemblyInspector(runtime, lite=True)

    if code == AssemblyIssueCode.INCONSISTENT_RHCOS_RPMS:
        rhcos_builds, rhcos_inconsistencies = _check_inconsistent_rhcos_rpms(runtime, assembly_inspector)
        if rhcos_inconsistencies:
            msg = f'Found RHCOS inconsistencies in builds {rhcos_builds}: {rhcos_inconsistencies}'
            print(msg)
            assembly_issue = AssemblyIssue(msg, component='rhcos', code=code)
            if assembly_inspector.does_permit(assembly_issue):
                print(f'Assembly permits code {code}.')
                exit(0)
            exit(1)
        print(f'RHCOS builds consistent {rhcos_builds}')
        exit(0)
    else:
        print(f'AssemblyIssueCode {code} not supported at this time :(')
        exit(1)


def _check_inconsistent_rhcos_rpms(runtime, assembly_inspector):
    logger = runtime.logger
    rhcos_builds = []
    for arch in runtime.group_config.arches:
        build_inspector = assembly_inspector.get_rhcos_build(arch)
        rhcos_builds.append(build_inspector)
    logger.info(f"Checking following builds for inconsistency: {rhcos_builds}")
    rhcos_inconsistencies = PayloadGenerator.find_rhcos_build_rpm_inconsistencies(rhcos_builds)
    return rhcos_builds, rhcos_inconsistencies
