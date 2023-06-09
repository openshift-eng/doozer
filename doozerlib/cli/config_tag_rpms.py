import json
import logging
from typing import Dict, Iterable, List, Optional, Set, Tuple, Union, cast

import click
import koji

from doozerlib import brew, exectools
from doozerlib.assembly import AssemblyTypes
from doozerlib.cli import cli, click_coroutine, pass_runtime
from doozerlib.exceptions import DoozerFatalError
from doozerlib.rpm_delivery import RPMDeliveries
from doozerlib.runtime import Runtime


class TagRPMsCli:
    def __init__(self, runtime: Runtime, dry_run: bool, as_json: bool) -> None:
        self._runtime = runtime
        self.dry_run = dry_run
        self.as_json = as_json

    @staticmethod
    async def get_tagged_builds(session: koji.ClientSession,
                                tag_component_tuples: Iterable[Tuple[str, Optional[str]]],
                                build_type: Optional[str],
                                event: Optional[int] = None,
                                latest: int = 0,
                                inherit: bool = False) -> List[List[Dict]]:
        """ Get tagged builds as of the given event

        In each list for a component, builds are ordered from newest tagged to oldest tagged:
        https://pagure.io/koji/blob/3fed02c8adb93cde614af9f61abd12bbccdd6682/f/hub/kojihub.py#_1392

        :param session: instance of Brew session
        :param tag_component_tuples: List of (tag, component_name) tuples
        :param build_type: if given, only retrieve specified build type (rpm, image)
        :param event: Brew event ID, or None for now.
        :param latest: 0 to get all tagged builds, N to get N latest builds per package
        :param inherit: True to include builds inherited from parent tags
        :return: a list of lists of Koji/Brew build dicts
        """
        def _func():
            tasks = []
            with session.multicall(strict=True) as m:
                for tag, component_name in tag_component_tuples:
                    tasks.append(m.listTagged(tag, package=component_name, event=event, type=build_type, latest=latest, inherit=inherit))
            return [task.result for task in tasks]
        return cast(List[List[Dict]], await exectools.to_thread(_func))

    @staticmethod
    async def untag_builds(session: koji.ClientSession, tag_build_tuples: Iterable[Tuple[str, Union[str, int]]]):
        def _func():
            tasks = []
            with session.multicall(strict=True) as m:
                for tag, build in tag_build_tuples:
                    tasks.append(m.untagBuild(tag, build, strict=False))  # strict=False: Don't raise TagError when the build is not in the tag.
            return [task.result for task in tasks]
        return await exectools.to_thread(_func)

    @staticmethod
    async def tag_builds(session: koji.ClientSession, tag_build_tuples: Iterable[Tuple[str, Union[str, int]]], logger: logging.Logger):
        def _func():
            tasks = []
            with session.multicall(strict=True) as m:
                for tag, build in tag_build_tuples:
                    tasks.append(m.tagBuild(tag, build))
            return [task.result for task in tasks]
        task_ids = cast(List[int], await exectools.to_thread(_func))
        if task_ids:
            TASK_URL = "https://brewweb.engineering.redhat.com/brew/taskinfo?taskID="
            logger.info("Waiting for task(s) to complete: %s", ", ".join(map(lambda t: f"{TASK_URL}{t}", task_ids)))
            errors = await brew.watch_tasks_async(session, logger.info, task_ids)
            # we will treat "already tagged" error as a success
            failed_tasks = {task_id for task_id, error in errors.items() if error and "already tagged" not in error}
            if failed_tasks:
                # if "already tagged" in errors[task_id]:
                message = "; ".join(
                    f"Task {TASK_URL}{task_id} failed: {errors[task_id]}"
                    for task_id in failed_tasks
                )
                raise DoozerFatalError(message)

    async def run(self):
        if self._runtime.assembly_type is not AssemblyTypes.STREAM:
            raise DoozerFatalError("This command can only be run for stream assembly.")
        logger = self._runtime.logger
        report = {
            "untagged": {},
            "tagged": {},
        }
        # Load and verify rpm_deliveries config
        group_config = self._runtime.group_config.primitive()
        rpm_deliveries = RPMDeliveries.parse_obj(group_config.get("rpm_deliveries", []))  # will raise ValidationError if invalid
        if not rpm_deliveries:
            logger.warning("rpm_deliveries is not defined for this group.")
            if self.as_json:
                print(json.dumps(report))
            return
        # Scan for builds
        logger.info("Logging into Brew...")
        koji_api = self._runtime.build_retrying_koji_client()
        koji_api.gssapi_login()
        MAX_BUILDS = 10  # getting 10 latest builds per package should be more than enough
        builds_to_tag: Dict[str, Set[str]] = {}  # target_tag_name -> set of NVRs
        builds_to_untag: Dict[str, Set[str]] = {}  # target_tag_name -> set of NVRs
        for entry in rpm_deliveries:
            # For each package, look at builds in stop-ship tag and integration tag,
            # then find an acceptable build in integration tag.
            if not entry.target_tag:
                logger.warning("RPM delivery config for package(s) %s doesn't define a target tag. Skipping...",
                               ", ".join(entry.packages))
                continue
            builds_to_tag.setdefault(entry.target_tag, set())
            builds_to_untag.setdefault(entry.target_tag, set())
            # Get all builds in stop-ship tag
            logger.info("Getting tagged builds in stop-ship tag %s...", entry.stop_ship_tag)
            builds_in_stop_ship_tag = await self.get_tagged_builds(
                koji_api,
                [(entry.stop_ship_tag, pkg) for pkg in entry.packages],
                build_type="rpm")
            # Get at most 10 builds in integration tag
            logger.info("Getting %s latest tagged builds in integration tag %s...", MAX_BUILDS, entry.integration_tag)
            builds_in_integration_tag = await self.get_tagged_builds(
                koji_api,
                [(entry.integration_tag, pkg) for pkg in entry.packages],
                build_type="rpm",
                latest=MAX_BUILDS)
            for package, candidate_builds, stop_ship_builds in zip(entry.packages, builds_in_integration_tag, builds_in_stop_ship_tag):
                stop_ship_nvrs = {b["nvr"] for b in stop_ship_builds}
                logger.info("Found %s build(s) of package %s in stop-ship tag %s", len(stop_ship_nvrs), package, entry.stop_ship_tag)
                if stop_ship_nvrs:
                    # Check if those stop-ship builds are also in target tag
                    nvr_list = sorted(stop_ship_nvrs)
                    logger.info("Check if the following stop-ship builds are in target tag %s: %s", entry.target_tag, ", ".join(nvr_list))
                    for nvr, tags in zip(nvr_list, brew.get_builds_tags(nvr_list, koji_api)):
                        if next(filter(lambda tag: tag["name"] == entry.target_tag, tags), None):
                            builds_to_untag[entry.target_tag].add(nvr)
                for build in candidate_builds:
                    # check if the build is already tagged into the stop-ship tag
                    if build["nvr"] in stop_ship_nvrs:
                        logger.warning("Build %s is tagged into the stop-ship tag: %s. Skipping...", build["nvr"], entry.stop_ship_tag)
                        continue
                    # check if the build is already (or historically) tagged into the target tag
                    logger.info("Checking if build %s is already tagged into target tag %s...", build["nvr"], entry.target_tag)
                    history = koji_api.queryHistory(tables=["tag_listing"], build=build["nvr"], tag=entry.target_tag)["tag_listing"]
                    if history:  # already or historically tagged
                        if not history[-1]["active"]:  # the build was historically tagged but untagged afterwards
                            logger.warning("Build %s was untagged from %s after being tagged. Skipping...", build["nvr"], entry.target_tag)
                            continue  # look at the next build
                        logger.warning("Build %s is already tagged into %s", build["nvr"], entry.target_tag)
                        break
                    logger.info("Build %s is acceptable for %s", build["nvr"], entry.target_tag)
                    builds_to_tag[entry.target_tag].add(build["nvr"])
                    break

        # untag builds from target tags
        tag_build_tuples = []
        for tag, nvrs in builds_to_untag.items():
            if not nvrs:
                continue
            nvrs = sorted(nvrs)
            logger.info("About to untag the following build(s) from tag %s: %s", tag, ", ".join(nvrs))
            tag_build_tuples += [(tag, nvr) for nvr in nvrs]
            report["untagged"].setdefault(tag, []).extend(nvrs)
        if tag_build_tuples:
            if self.dry_run:
                logger.warning("[DRY RUN] Builds should have been untagged")
            else:
                await self.untag_builds(koji_api, tag_build_tuples)
                logger.info("Builds have been untagged")
        else:
            logger.info("Nothing to untag")

        # tag builds into target tags
        tag_build_tuples = []
        for tag, nvrs in builds_to_tag.items():
            if not nvrs:
                continue
            nvrs = sorted(nvrs)
            logger.info("About to tag the following build(s) into tag %s: %s", tag, ", ".join(nvrs))
            tag_build_tuples += [(tag, nvr) for nvr in nvrs]
            report["tagged"].setdefault(tag, []).extend(nvrs)
        if tag_build_tuples:
            if self.dry_run:
                logger.warning("[DRY RUN] Builds should have been tagged")
            else:
                await self.tag_builds(koji_api, tag_build_tuples, logger)
                logger.info("Builds have been tagged")
        else:
            logger.info("Nothing to tag")

        # Print out the result as JSON format
        if self.as_json:
            print(json.dumps(report))


@cli.command("config:tag-rpms", short_help="Tag or untag RPMs for RPM delivery")
@click.option('--dry-run', is_flag=True, help='Do not tag anything, but only print which builds will be tagged or untagged')
@click.option("--json", "as_json", is_flag=True, help="Print out the result as JSON format")
@pass_runtime
@click_coroutine
async def config_tag_rpms(runtime: Runtime, dry_run: bool, as_json: bool):
    """
    This command scans RPMs (usually kernel and kernel-rt) in the integration Brew tag defined in
    group config, then tags acceptable builds into the target Brew tag. "acceptable" here means the
    build is not tagged into stop_ship_tag or historically tagged into the target tag.

    e.g. With the following config:

        \b
        rpm_deliveries:
            - packages:
                - kernel
                - kernel-rt
              integration_tag: early-kernel-integration-8.6
              stop_ship_tag: early-kernel-stop-ship
              ship_ok_tag: early-kernel-ship-ok
              target_tag: rhaos-4.11-rhel-8-candidate

    Doozer will try to find latest acceptable builds of kernel and kernel-rt from Brew tag
    early-kernel-integration-8.6, then tag them into Brew tag rhaos-4.11-rhel-8-candidate.
    Additionally, all builds in tag early-kernel-stop-ship will be untagged from rhaos-4.11-rhel-8-candidate.
    """
    runtime.initialize(config_only=True)
    await TagRPMsCli(runtime=runtime, dry_run=dry_run, as_json=as_json).run()
