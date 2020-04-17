from typing import List, Dict, Optional
from pathlib import Path
import click
import yaml
import semver
import sys
import threading

from doozerlib import exectools
from doozerlib.runtime import Runtime
from doozerlib.image import ImageMetadata
from doozerlib.releaseapi import create_source_revision_for_image, sanitize_for_serialization


@click.group("beta:releaseapi", short_help="(Beta) Release APIServer objects")
@click.pass_context
def releaseapi_cli(ctx):
    pass


@releaseapi_cli.command(short_help="Generate source revisions (currently only support images)")
@click.option("--kind", "-k", metavar='KIND', type=click.Choice(['all', 'image']), default="all", required=True)
@click.option("--out", "-o", metavar='FILE', type=Path, default="-", required=True)
@click.option("--release-name", "-r", metavar="RELEASE_NAME", type=str, required=True)
@click.pass_context
def gen_source_revisions(ctx, kind: str, out: Path, release_name: str):
    """ Record the latest upstream commit hashes for a release branch and generate source revison objects.

    Example 1: Generate source revisions for OpenShift 4.4
        doozer -g openshift-4.4 beta:releaseapi gen-source-revisions -r openshift-4.4.1 -k image -o openshift-4.4.1.source-revisions.yaml

    Example 2: Generate source revisions for specified components
        doozer -g openshift-4.4 -i ose-installer-artifacts,openshift-enterprise-cli beta:releaseapi gen-source-revisions -r openshift-4.4.1 -k image -o openshift-4.4.1.source-revisions.yaml

    """
    # version = version.lstrip("v")
    # semver.parse_version_info(version)  # assert semver
    # release_name = f"openshift-{version}"
    runtime: Runtime = ctx.obj
    runtime.initialize(clone_distgits=False)
    release_stream_name = runtime.group

    images: List[ImageMetadata] = [image for image in runtime.image_metas()]

    def _create_source_revision(image: ImageMetadata):
        runtime.logger.info(f"Processing {image.name}...")
        url, ref, commit = image.get_source()
        sr = create_source_revision_for_image(release_stream_name, release_name, image, commit)
        return sr

    results = runtime._parallel_exec(_create_source_revision, images, 64)
    results = sanitize_for_serialization(results)
    if str(out) == "-":
        yaml.safe_dump_all(results, sys.stdout)
    else:
        with open(out, "w") as f:
            yaml.safe_dump_all(results, f)
