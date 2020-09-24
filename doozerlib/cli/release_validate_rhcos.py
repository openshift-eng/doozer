import asyncio
import click
import json

from doozerlib.cli import cli, pass_runtime, click_coroutine
from doozerlib.exceptions import DoozerFatalError
from doozerlib.rhcos import get_rhcos_pullspec_from_image_stream, get_rhcos_version_arch, get_rhcos_build_metadata, get_rhcos_pullspec_from_release
from doozerlib.util import red_print, green_print


@cli.command("release:validate-rhcos", short_help="validate if RHCOS in the latest nightly is viable to promote")
@click.option("--is-name", metavar='NAME',
              help="ImageStream .metadata.name value. Something like '4.2-art-latest'")
@click.option("--is-namespace", metavar='NAMESPACE', required=False, default='ocp',
              help="ImageStream .metadata.namespace value.\ndefault=ocp")
@click.option("--rhcos-tag", metavar="TAG", default="machine-os-content",
              help="Tag of RHCOS image in the release ImageStream. default=machine-os-content")
@click.argument("release_pullspecs", metavar="PULLSPECS...", nargs=-1)
@pass_runtime
@click_coroutine
async def release_validate_rhcos(runtime, is_name, is_namespace, rhcos_tag, release_pullspecs):
    """Validates RHCOS is generally OK to promote.

    Example 1: Validate the latest nightly in an image stream
        doozer release:validate-rhcos --is-name=4.6-art-latest --is-namespace=ocp
    Example 2: Validate given release images
        doozer release:validate-rhcos registry.svc.ci.openshift.org/ocp/release:4.6.0-0.nightly-2020-09-24-074159 registry.svc.ci.openshift.org/ocp-s390x/release-s390x:4.6.0-0.nightly-s390x-2020-09-24-065929
    """
    runtime.initialize(no_group=True)
    if is_name and release_pullspecs:
        raise click.BadParameter("Use one of --is-name or PULLSPECS.")

    VALIDATION_ERROR = 2  # 2 donates validation errors rather than common runtime errors

    if is_name:
        runtime.logger.info("Getting latest nightly...")
        try:
            pullspec = await get_rhcos_pullspec_from_image_stream(is_name, is_namespace, rhcos_tag)
        except Exception as ex:
            raise DoozerFatalError(f"Couldn't get RHCOS pullspec: {ex}. See debug.log for more info. Did you log in to api.ci.openshift.org?")
        ok = await validate_pullspec(runtime, pullspec)
        if not ok:
            red_print(f"Latest RHCOS in ImageStream {is_namespace}/{is_name} is not in the AWS bucket.")
            exit(VALIDATION_ERROR)

    if release_pullspecs:
        runtime.logger.info(f"Validating RHCOS images in {len(release_pullspecs)} release pullspecs...")
        results = await asyncio.gather(*[validate_rhcos_in_release(runtime, release, rhcos_tag) for release in release_pullspecs])
        bad_releases = [release for release, ok in zip(release_pullspecs, results) if not ok]
        for bad_release in bad_releases:
            red_print(f"RHCOS in {bad_release} is not in the AWS bucket.")
        if bad_releases:
            exit(VALIDATION_ERROR)

    green_print("Passed all validations.")


async def validate_rhcos_in_release(runtime, release_pullspec, rhcos_tag):
    runtime.logger.info(f"Validating release {release_pullspec}...")
    pullspec = await get_rhcos_pullspec_from_release(release_pullspec, rhcos_tag)
    return await validate_pullspec(runtime, pullspec)


async def validate_pullspec(runtime, pullspec):
    runtime.logger.info(f"Determining build ID for RHCOS pullspec {pullspec}...")
    try:
        version, arch = await get_rhcos_version_arch(pullspec)
    except Exception as ex:
        raise DoozerFatalError(f"Couldn't determine RHCOS build ID: {ex}. See debug.log for more info. Did you log in to quay.io?")
    runtime.logger.info(f"Pullspec {pullspec} is version {version}, arch {arch}. Fetching metadata from the AWS bucket...")
    metadata = await get_rhcos_build_metadata(version, arch)
    return metadata is not None
