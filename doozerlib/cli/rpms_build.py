import asyncio
import traceback
from typing import List

import click

from doozerlib import exectools
from doozerlib.cli import (cli, click_coroutine, pass_runtime,
                           validate_semver_major_minor_patch)
from doozerlib.exceptions import DoozerFatalError
from doozerlib.rpm_builder import RPMBuilder
from doozerlib.rpmcfg import RPMMetadata
from doozerlib.runtime import Runtime


# This command reimplements `rpms:build` without tito. Rename it to `rpms:build` when getting to prod.
@cli.command("beta:rpms:rebase-and-build", help="Build rpms in the group or given by --rpms.")
@click.option("--version", metavar='VERSION', default=None, callback=validate_semver_major_minor_patch,
              help="Version string to populate in specfile.", required=True)
@click.option("--release", metavar='RELEASE', default=None,
              help="Release label to populate in specfile.", required=True)
@click.option("--embargoed", default=False, is_flag=True, help="Add .p1 to the release string for all rpms, which indicates those rpms have embargoed fixes")
@click.option('--scratch', default=False, is_flag=True, help='Perform a scratch build.')
@click.option('--dry-run', default=False, is_flag=True, help='Do not build anything, but only print build operations.')
@pass_runtime
@click_coroutine
async def rpms_rebase_and_build(runtime: Runtime, version: str, release: str, embargoed: bool, scratch: bool, dry_run: bool):
    """
    Attempts to rebase and build rpms for all of the defined rpms
    in a group. If an rpm has already been built, it will be treated as
    a successful operation.
    """
    exit_code = await _rpms_rebase_and_build(runtime, version=version, release=release, embargoed=embargoed, scratch=scratch, dry_run=dry_run)
    exit(exit_code)


async def _rpms_rebase_and_build(runtime: Runtime, version: str, release: str, embargoed: bool, scratch: bool, dry_run: bool):
    if version.startswith('v'):
        version = version[1:]

    runtime.initialize(mode='rpms', clone_source=False, clone_distgits=False)  # We will clone distgits later.
    if runtime.local:
        raise DoozerFatalError("Local RPM build is not currently supported.")
    if runtime.group_config.public_upstreams and (release is None or not release.endswith(".p?")):
        raise click.BadParameter("You must explicitly specify a `release` ending with `.p?` when there is a public upstream mapping in ocp-build-data.")

    runtime.assert_mutation_is_permitted()

    rpms: List[RPMMetadata] = runtime.rpm_metas()
    if not rpms:
        runtime.logger.error("No RPMs found. Check the arguments.")
        exit(0)

    if embargoed:
        for rpm in rpms:
            rpm.private_fix = True

    builder = RPMBuilder(runtime, dry_run=dry_run, scratch=scratch)

    async def _rebase_and_build(rpm: RPMMetadata):
        status = await _rebase_rpm(runtime, builder, rpm, version, release)
        if status != 0:
            return status
        status = await _build_rpm(runtime, builder, rpm)
        return status
    tasks = [asyncio.ensure_future(_rebase_and_build(rpm)) for rpm in rpms]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    failed = [rpms[i].distgit_key for i, r in enumerate(results) if r != 0]
    if failed:
        runtime.logger.error("\n".join(["Build failures:"] + sorted(failed)))
        return 1
    return 0


@cli.command("beta:rpms:rebase", help="Build rpms in the group or given by --rpms.")
@click.option("--version", metavar='VERSION', default=None, callback=validate_semver_major_minor_patch,
              help="Version string to populate in specfile.", required=True)
@click.option("--release", metavar='RELEASE', default=None,
              help="Release label to populate in specfile.", required=True)
@click.option("--embargoed", default=False, is_flag=True, help="Add .p1 to the release string for all rpms, which indicates those rpms have embargoed fixes")
@click.option('--dry-run', default=False, is_flag=True, help='Do not build anything, but only print build operations.')
@pass_runtime
@click_coroutine
async def rpms_build(runtime: Runtime, version: str, release: str, embargoed: bool, dry_run: bool):
    """
    Attempts to rebase rpms for all of the defined rpms in a group.

    For each rpm, uploads the source tarball to distgit lookaside cache and pulls the current source rpm spec file (and potentially other supporting
    files) into distgit with transformations defined in the config yaml applied.

    This operation will also set the version and release in the file according to the
    command line arguments provided.
    """
    exit_code = await _rpms_rebase(runtime, version=version, release=release, embargoed=embargoed, dry_run=dry_run)
    exit(exit_code)


async def _rpms_rebase(runtime: Runtime, version: str, release: str, embargoed: bool, dry_run: bool):
    if version.startswith('v'):
        version = version[1:]

    runtime.initialize(mode='rpms', clone_source=False, clone_distgits=False)  # We will clone distgits later.
    if runtime.local:
        raise DoozerFatalError("Local RPM build is not currently supported.")
    if runtime.group_config.public_upstreams and (release is None or not release.endswith(".p?")):
        raise click.BadParameter("You must explicitly specify a `release` ending with `.p?` when there is a public upstream mapping in ocp-build-data.")

    runtime.assert_mutation_is_permitted()

    rpms: List[RPMMetadata] = runtime.rpm_metas()
    if not rpms:
        runtime.logger.error("No RPMs found. Check the arguments.")
        exit(0)

    if embargoed:
        for rpm in rpms:
            rpm.private_fix = True

    builder = RPMBuilder(runtime, dry_run=dry_run)
    tasks = [asyncio.ensure_future(_rebase_rpm(runtime, builder, rpm, version, release)) for rpm in rpms]

    results = await asyncio.gather(*tasks, return_exceptions=True)
    failed = [rpms[i].distgit_key for i, r in enumerate(results) if r != 0]
    if failed:
        runtime.logger.error("\n".join(["Build failures:"] + sorted(failed)))
        return 1
    return 0


async def _rebase_rpm(runtime: Runtime, builder: RPMBuilder, rpm: RPMMetadata, version, release):
    logger = rpm.logger
    action = "rebase_rpm"
    record = {
        "distgit_key": rpm.distgit_key,
        "rpm": rpm.rpm_name,
        "version": version,
        "release": release,
        "message": "Unknown failure",
        "status": -1,
        # Status defaults to failure until explicitly set by succcess. This handles raised exceptions.
    }
    try:
        await builder.rebase(rpm, version, release)
        record["version"] = rpm.version
        record["release"] = rpm.release
        record["specfile"] = rpm.specfile
        record["private_fix"] = rpm.private_fix
        record["source_head"] = rpm.source_head
        record["source_commit"] = rpm.pre_init_sha or ""
        record["dg_branch"] = rpm.distgit_repo().branch
        record["status"] = 0
        record["message"] = "Success"
        logger.info("Successfully rebased rpm: %s", rpm.distgit_key)
    except Exception:
        tb = traceback.format_exc()
        record["message"] = "Exception occurred:\n{}".format(tb)
        logger.error("Exception occurred when rebasing %s:\n%s", rpm.distgit_key, tb)
    finally:
        runtime.add_record(action, **record)
    return record["status"]


# This command reimplements `rpms:build` without tito. Rename it to `rpms:build` when getting to prod.
@cli.command("beta:rpms:build", help="Build rpms in the group or given by --rpms.")
@click.option('--scratch', default=False, is_flag=True, help='Perform a scratch build.')
@click.option('--dry-run', default=False, is_flag=True, help='Do not build anything, but only print build operations.')
@pass_runtime
@click_coroutine
async def rpms_build(runtime: Runtime, scratch: bool, dry_run: bool):
    """
    Attempts to build rpms for all of the defined rpms
    in a group. If an rpm has already been built, it will be treated as
    a successful operation.
    """
    exit_code = await _rpms_build(runtime, scratch=scratch, dry_run=dry_run)
    exit(exit_code)


async def _rpms_build(runtime: Runtime, scratch: bool, dry_run: bool):
    runtime.initialize(mode='rpms', clone_source=False, clone_distgits=False)  # We will clone distgits later.
    if runtime.local:
        raise DoozerFatalError("Local RPM build is not currently supported.")

    runtime.assert_mutation_is_permitted()

    rpms: List[RPMMetadata] = runtime.rpm_metas()
    if not rpms:
        runtime.logger.error("No RPMs found. Check the arguments.")
        exit(0)

    builder = RPMBuilder(runtime, dry_run=dry_run, scratch=scratch)
    tasks = [asyncio.ensure_future(_build_rpm(runtime, builder, rpm)) for rpm in rpms]

    results = await asyncio.gather(*tasks, return_exceptions=True)
    failed = [rpms[i].distgit_key for i, r in enumerate(results) if r != 0]
    if failed:
        runtime.logger.error("\n".join(["Build failures:"] + sorted(failed)))
        return 1
    return 0


async def _build_rpm(runtime: Runtime, builder: RPMBuilder, rpm: RPMMetadata):
    logger = rpm.logger
    action = "build_rpm"
    record = {
        "distgit_key": rpm.distgit_key,
        "rpm": rpm.rpm_name,
        "message": "Unknown failure",
        "targets": rpm.targets,
        "status": -1,
        # Status defaults to failure until explicitly set by succcess. This handles raised exceptions.
    }
    task_ids = []
    task_urls = []
    try:
        task_ids, task_urls = await builder.build(rpm)
        record["version"] = rpm.version
        record["release"] = rpm.release
        record["specfile"] = rpm.specfile
        record["status"] = 0
        record["message"] = "Success"
        logger.info("Successfully built rpm: %s ; Task URLs: %s", rpm.distgit_key, [url for url in task_urls])
    except (Exception, KeyboardInterrupt) as e:
        tb = traceback.format_exc()
        record["message"] = "Exception occurred:\n{}".format(tb)
        logger.error("Exception occurred when building %s:\n%s", rpm.distgit_key, tb)
        if isinstance(e, exectools.RetryException):
            task_ids, task_urls = e.args[1]
    finally:
        if task_ids:
            record["task_ids"] = task_ids
            record["task_urls"] = task_urls
            record["task_id"] = task_ids[0]
            record["task_url"] = task_urls[0]
        runtime.add_record(action, **record)
    return record["status"]
