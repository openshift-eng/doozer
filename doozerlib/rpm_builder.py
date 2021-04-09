import asyncio
import logging
import re
import shutil
import threading
from os import PathLike
from pathlib import Path
from typing import Dict, List, Optional

import aiofiles
import aiofiles.os

from doozerlib import brew, exectools
from doozerlib.distgit import RPMDistGitRepo
from doozerlib.model import Missing
from doozerlib.rpmcfg import RPMMetadata
from doozerlib.runtime import Runtime
from doozerlib.util import is_in_directory


class RPMBuilder:
    """ It builds RPMs!
    """

    def __init__(
        self, runtime: Runtime, *, scratch: bool = False, dry_run: bool = False
    ) -> None:
        """ Create a RPMBuilder instance.
        :param runtime: Doozer runtime
        :param scratch: Whether to create a scratch build
        :param dry_run: Don't build anything but just exercise the code
        """
        self._runtime = runtime
        self._scratch = scratch
        self._dry_run = dry_run

    async def rebase(self, rpm: RPMMetadata, version: str, release: str) -> str:
        """ Rebases and pushes the distgit repo for an rpm
        :param rpm: Metadata of the rpm
        :param version: Set rpm version
        :param release: Set rpm release
        :return: Hash of the new distgit commit
        """
        logger = rpm.logger
        # clone source and distgit
        logger.info("Cloning source and distgit repos...")
        dg: RPMDistGitRepo
        _, dg = await asyncio.gather(
            exectools.to_thread(rpm.clone_source),
            exectools.to_thread(rpm.distgit_repo, autoclone=True),
        )
        # cleanup distgit dir
        logger.info("Cleaning up distgit repo...")
        await exectools.cmd_assert_async(
            ["git", "reset", "--hard", "origin/" + dg.branch], cwd=dg.distgit_dir
        )
        await exectools.cmd_assert_async(
            ["git", "rm", "--ignore-unmatch", "-rf", "."], cwd=dg.distgit_dir
        )

        # set .p0/.p1 flag
        if self._runtime.group_config.public_upstreams:
            if not release.endswith(".p?"):
                raise ValueError(
                    f"'release' must end with '.p?' for an rpm with a public upstream but its actual value is {release}"
                )
            if rpm.private_fix is None:
                raise AssertionError("rpm.private_fix flag should already be set")
            if rpm.private_fix:
                logger.warning("Source contains embargoed fixes.")
                pval = ".p1"
            else:
                pval = ".p0"
            release = release[:-3] + pval

        # include commit hash in release field
        release += ".git." + rpm.pre_init_sha[:7]
        rpm.set_nvr(version, release)

        # generate new specfile
        tarball_name = f"{rpm.config.name}-{rpm.version}-{rpm.release}.tar.gz"
        logger.info("Creating rpm spec file...")
        specfile = await self._populate_specfile_async(rpm, tarball_name)
        dg_specfile_path = dg.dg_path / Path(rpm.specfile).name
        async with aiofiles.open(dg_specfile_path, "w") as f:
            await f.writelines(specfile)
        rpm.specfile = str(dg_specfile_path)

        # create tarball source as Source0
        logger.info("Creating tarball source...")
        tarball_path = dg.dg_path / tarball_name
        await exectools.cmd_assert_async(
            [
                "tar",
                "-czf",
                tarball_path,
                "--exclude=.git",
                fr"--transform=s,^\./,{rpm.config.name}-{rpm.version}/,",
                ".",
            ],
            cwd=rpm.source_path,
        )
        logger.info(
            "Done creating tarball source. Uploading to distgit lookaside cache..."
        )
        if not self._dry_run:
            await exectools.cmd_assert_async(
                ["rhpkg", "new-sources", tarball_name], cwd=dg.dg_path, retries=3
            )
        else:
            async with aiofiles.open(dg.dg_path / "sources", "w") as f:
                f.write("SHA512 ({}) = {}\n".format(tarball_name, "0" * 128))
            logger.warning("DRY RUN - Would have uploaded %s", tarball_name)

        # copy Source1, Source2,... and Patch0, Patch1,...
        logger.info("Determining additional sources and patches...")
        out, _ = await exectools.cmd_assert_async(
            ["spectool", "--", dg_specfile_path], cwd=dg.dg_path
        )
        for line in out.splitlines():
            line_split = line.split(": ")
            if len(line_split) < 2 or line_split[0] == "Source0":
                continue
            filename = line_split[1].strip()
            src = Path(rpm.source_path, filename)
            # For security, ensure the source file referenced by the specfile is contained in both source and distgit directories.
            if not is_in_directory(src, rpm.source_path):
                raise ValueError(
                    "STOP! Source file {} referenced in Specfile {} lives outside of the source directory {}".format(
                        filename, dg_specfile_path, rpm.source_path
                    )
                )
            dest = dg.dg_path / filename
            if not is_in_directory(dest, dg.dg_path):
                raise ValueError(
                    "STOP! Source file {} referenced in Specfile {} would be copied to a directory outside of distgit directory {}".format(
                        filename, dg_specfile_path, dg.dg_path
                    )
                )
            dest.parent.mkdir(parents=True, exist_ok=True)
            logging.debug("Copying %s", filename)

            shutil.copy(src, dest, follow_symlinks=False)

        # run modifications
        if rpm.config.content.source.modifications is not Missing:
            logging.info("Running custom modifications...")
            await exectools.to_thread(
                rpm._run_modifications, dg_specfile_path, dg.dg_path
            )

        # commit changes
        logging.info("Committing distgit changes...")
        await aiofiles.os.remove(tarball_path)
        commit_hash = await exectools.to_thread(dg.commit,
                                                f"Automatic commit of package [{rpm.config.name}] release [{rpm.version}-{rpm.release}]."
                                                )

        # push
        if not self._dry_run:
            await dg.push_async()
        else:
            logger.warning("Would have pushed %s", dg.name)
        return commit_hash

    async def build(self, rpm: RPMMetadata, retries: int = 3):
        """ Builds rpm with the latest distgit commit
        :param rpm: Metadata of the RPM
        :param retries: The number of times to retry
        """
        logger = rpm.logger
        dg = rpm.distgit_repo()
        if rpm.specfile is None:
            rpm.specfile, nvr, rpm.pre_init_sha = await dg.resolve_specfile_async()
            rpm.set_nvr(nvr[1], nvr[2])
        if rpm.private_fix is None:
            rpm.private_fix = ".p1" in rpm.release
        if rpm.private_fix:
            logger.warning("This rpm build contains embargoed fixes.")

        if len(rpm.targets) > 1:  # for a multi target build, we need to ensure all buildroots have valid versions of golang compilers
            logger.info("Checking whether this is a golang package...")
            if await self._golang_required(rpm.specfile):
                # assert buildroots contain the correct versions of golang
                logger.info(
                    "This is a golang package. Checking whether buildroots contain consistent versions of golang compilers..."
                )
                await exectools.to_thread(rpm.assert_golang_versions)

        # Submit build tasks
        message = "Unknown error"
        for attempt in range(retries):
            # errors, task_ids, task_urls = await self._create_build_tasks(dg)
            task_ids = []
            task_urls = []
            logger.info("Creating Brew tasks...")
            for task_id, task_url in await asyncio.gather(
                *[self._build_target_async(rpm, target) for target in rpm.targets]
            ):
                task_ids.append(task_id)
                task_urls.append(task_url)

            # Wait for all tasks to complete
            logger.info("Waiting for all tasks to complete")
            errors = await self._watch_tasks_async(task_ids, logger)

            # Gather brew-logs
            logger.info("Gathering brew-logs")
            for target, task_id in zip(rpm.targets, task_ids):
                logs_dir = (
                    Path(self._runtime.brew_logs_dir) / rpm.name / f"{target}-{task_id}"
                )
                cmd = ["brew", "download-logs", "--recurse", "-d", logs_dir, task_id]
                if not self._dry_run:
                    logs_rc, _, logs_err = await exectools.cmd_gather_async(cmd)
                    if logs_rc != exectools.SUCCESS:
                        logger.warning(
                            "Error downloading build logs from brew for task %s: %s"
                            % (task_id, logs_err)
                        )
                else:
                    logger.warning("DRY RUN - Would have downloaded Brew logs with %s", cmd)
            failed_tasks = {task_id for task_id, error in errors.items() if error is not None}
            if not failed_tasks:
                logger.info("Successfully built rpm: %s", rpm.rpm_name)
                rpm.build_status = True
                break
            # An error occurred. We don't have a viable build.
            message = ", ".join(
                f"Task {task_id} failed: {errors[task_id]}"
                for task_id in failed_tasks
            )
            logger.warning(
                "Error building rpm %s [attempt #%s] in Brew: %s",
                rpm.qualified_name,
                attempt + 1,
                message,
            )
            if attempt < retries - 1:
                # Brew does not handle an immediate retry correctly, wait before trying another build
                await asyncio.sleep(5 * 60)
        if not rpm.build_status:
            raise exectools.RetryException(
                f"Giving up after {retries} failed attempt(s): {message}",
                (task_ids, task_urls),
            )
        return task_ids, task_urls

    async def _golang_required(self, specfile: PathLike):
        """Returns True if this RPM requires a golang compiler"""
        out, _ = await exectools.cmd_assert_async(
            ["rpmspec", "-q", "--buildrequires", "--", specfile]
        )
        return any(dep.strip().startswith("golang") for dep in out.splitlines())

    @staticmethod
    async def _populate_specfile_async(
        rpm: RPMMetadata, source_filename: str
    ) -> List[str]:
        """Populates spec file
        :param source_filename: Path to the template spec file
        """
        if not rpm.source_path:
            raise ValueError("Source is not cloned.")
        maintainer = await exectools.to_thread(rpm.get_maintainer_info)
        maintainer_string = ""
        if maintainer:
            flattened_info = ", ".join(f"{k}: {v}" for (k, v) in maintainer.items())
            # The space before the [Maintainer] information is actual rpm spec formatting
            # clue to preserve the line instead of assuming it is part of a paragraph.
            maintainer_string = f"[Maintainer] {flattened_info}"

        # otherwise, make changes similar to tito tagging
        rpm_spec_tags = {
            "Name:": "Name:           {}\n".format(rpm.config.name),
            "Version:": "Version:        {}\n".format(rpm.version),
            "Release:": "Release:        {}%{{?dist}}\n".format(rpm.release),
            "Source0:": f"Source0:        {source_filename}\n",
        }

        # rpm.version example: 3.9.0
        # Extract the major, minor, patch
        major, minor, patch = rpm.version.split(".")
        full = "v{}".format(rpm.version)

        # If this is a pre-release RPM, the include the release field in
        # the full version.
        # pre-release full version: v3.9.0-0.20.1
        # release full version: v3.9.0
        if rpm.release.startswith("0."):
            full += "-{}".format(rpm.release)

        commit_sha = rpm.pre_init_sha

        # Update with NVR, env vars, and descriptions
        described = False
        async with aiofiles.open(rpm.specfile, "r") as sf:
            lines = await sf.readlines()
        for i in range(len(lines)):
            line = lines[i].strip().lower()
            # If an RPM has sub packages, there can be multiple %description directives
            if line.startswith("%description"):
                lines[i] = f"{lines[i].strip()}\n{maintainer_string}\n"
                described = True
            elif line.startswith("%global os_git_vars "):
                lines[
                    i
                ] = f"%global os_git_vars OS_GIT_VERSION={major}.{minor}.{patch}-{rpm.release}-{commit_sha[0:7]} OS_GIT_MAJOR={major} OS_GIT_MINOR={minor} OS_GIT_PATCH={patch} OS_GIT_COMMIT={commit_sha} OS_GIT_TREE_STATE=clean"
                for k, v in rpm.extra_os_git_vars.items():
                    lines[i] += f" {k}={v}"
                lines[i] += "\n"
            elif line.startswith("%global commit"):
                lines[i] = re.sub(
                    r"commit\s+\w+", "commit {}".format(commit_sha), lines[i]
                )
            elif line.startswith("%setup"):
                lines[i] = f"%setup -q -n {rpm.config.name}-{rpm.version}\n"
            elif line.startswith("%autosetup"):
                lines[i] = f"%autosetup -S git -n {rpm.config.name}-{rpm.version} -p1\n"

            elif rpm_spec_tags:  # If there are keys left to replace
                for k in list(rpm_spec_tags.keys()):
                    v = rpm_spec_tags[k]
                    # Note that tags are not case sensitive
                    if lines[i].lower().startswith(k.lower()):
                        lines[i] = v
                        del rpm_spec_tags[k]
                        break

        # If there are still rpm tags to inject, do so
        for v in rpm_spec_tags.values():
            lines.insert(0, v)

        # If we didn't find a description, create one
        if not described:
            lines.insert(0, f"%description\n{maintainer_string}\n")

        return lines

    async def _build_target_async(self, rpm: RPMMetadata, target: str):
        """ Creates a Brew task to build the rpm against specific target
        :param rpm: Metadata of the rpm
        :param target: The target to build against
        """
        dg: RPMDistGitRepo = rpm.distgit_repo()
        logger = rpm.logger
        logger.info("Building %s against target %s", rpm.name, target)
        cmd = ["rhpkg", "build", "--nowait", "--target", target]
        if self._scratch:
            cmd.append("--skip-tag")
        if not self._dry_run:
            out, _ = await exectools.cmd_assert_async(cmd, cwd=dg.dg_path)
        else:
            logger.warning("DRY RUN - Would have created Brew task with %s", cmd)
            out = "Created task: 0\nTask info: https://brewweb.engineering.redhat.com/brew/taskinfo?taskID=0\n"
        # we should have a brew task we can monitor listed in the stdout.
        out_lines = out.splitlines()
        # Look for a line like: "Created task: 13949050" . Extract the identifier.
        task_id = int(
            next(
                (line.split(":")[1]).strip()
                for line in out_lines
                if line.startswith("Created task:")
            )
        )
        # Look for a line like: "Task info: https://brewweb.engineering.redhat.com/brew/taskinfo?taskID=13948942"
        task_url = next(
            (line.split(":", 1)[1]).strip()
            for line in out_lines
            if line.startswith("Task info:")
        )
        logger.info("Build running: %s - %s - %s", rpm.rpm_name, target, task_url)
        return task_id, task_url

    async def _watch_tasks_async(
        self, task_ids: List[int], logger: logging.Logger
    ) -> Dict[int, Optional[str]]:
        """ Asynchronously watches Brew Tasks for completion
        :param task_ids: List of Brew task IDs
        :param logger: A logger for logging
        :return: a dict of task ID and error message mappings
        """
        if self._dry_run:
            return {task_id: None for task_id in task_ids}
        brew_session = self._runtime.build_retrying_koji_client()
        terminate_event = threading.Event()
        try:
            errors = await exectools.to_thread(
                brew.watch_tasks, brew_session, logger.info, task_ids, terminate_event
            )
        except (asyncio.CancelledError, KeyboardInterrupt):
            terminate_event.set()
            raise
        return errors
