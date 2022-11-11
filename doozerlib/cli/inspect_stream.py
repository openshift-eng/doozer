import click
from pprint import pprint

from doozerlib.cli import cli
from doozerlib.cli.release_gen_payload import PayloadGenerator
from doozerlib.assembly import AssemblyIssueCode
from doozerlib.assembly import AssemblyIssue
from doozerlib.assembly_inspector import AssemblyInspector


@cli.command("inspect:stream", short_help="Inspect stream assembly for assembly issues")
@click.argument("code", type=click.Choice([code.name for code in AssemblyIssueCode], case_sensitive=False),
                required=True)
@click.option("--strict", default=False, type=bool, is_flag=True, help='Fail even if permitted')
@click.pass_obj
def inspect_stream(runtime, code, strict):
    code = AssemblyIssueCode[code]
    if runtime.assembly != 'stream':
        print(f'Disregarding non-stream assembly: {runtime.assembly}. This command is only intended for stream')
        runtime.assembly = 'stream'
    runtime.initialize(clone_distgits=False)

    if code == AssemblyIssueCode.INCONSISTENT_RHCOS_RPMS:
        assembly_inspector = AssemblyInspector(runtime, lookup_mode=None)
        rhcos_builds, rhcos_inconsistencies = _check_inconsistent_rhcos_rpms(runtime, assembly_inspector)
        if rhcos_inconsistencies:
            msg = f'Found RHCOS inconsistencies in builds {rhcos_builds}'
            print(msg)
            pprint(rhcos_inconsistencies)
            assembly_issue = AssemblyIssue(msg, component='rhcos', code=code)
            if assembly_inspector.does_permit(assembly_issue):
                print(f'Assembly permits code {code}.')
                if not strict:
                    exit(0)
                print('Running in strict mode')
            exit(1)
        print(f'RHCOS builds consistent {rhcos_builds}')
    elif code == AssemblyIssueCode.FAILED_CONSISTENCY_REQUIREMENT:
        requirements = runtime.group_config.rhcos.require_consistency
        if not requirements:
            print("No cross-payload consistency requirements defined in group.yml")
            exit(0)

        runtime.logger.info("Checking cross-payload consistency requirements defined in group.yml")
        assembly_inspector = AssemblyInspector(runtime, lookup_mode="images")
        issues = _check_cross_payload_consistency_requirements(runtime, assembly_inspector, requirements)
        if issues:
            print('Payload contents consistency requirements not satisfied')
            pprint(issues)
            not_permitted = [issue for issue in issues if not assembly_inspector.does_permit(issue)]
            if not_permitted:
                print(f'Assembly does not permit: {not_permitted}')
                exit(1)
            elif strict:
                print('Running in strict mode; saw: {issues}')
                exit(1)
        print('Payload contents consistency requirements satisfied')
    else:
        print(f'AssemblyIssueCode {code} not supported at this time :(')
        exit(1)


def _check_inconsistent_rhcos_rpms(runtime, assembly_inspector):
    rhcos_builds = []
    for arch in runtime.group_config.arches:
        build_inspector = assembly_inspector.get_rhcos_build(arch)
        rhcos_builds.append(build_inspector)
    runtime.logger.info(f"Checking following builds for inconsistency: {rhcos_builds}")
    rhcos_inconsistencies = PayloadGenerator.find_rhcos_build_rpm_inconsistencies(rhcos_builds)
    return rhcos_builds, rhcos_inconsistencies


def _check_cross_payload_consistency_requirements(runtime, assembly_inspector, requirements):
    issues = []
    for arch in runtime.group_config.arches:
        issues.extend(PayloadGenerator.find_rhcos_payload_rpm_inconsistencies(
            assembly_inspector.get_rhcos_build(arch),
            assembly_inspector.get_group_release_images(),
            requirements))
    return issues
