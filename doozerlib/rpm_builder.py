import asyncio
import logging
import pathlib
import re
import shutil
import time
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
from doozerlib.util import is_in_directory, isolate_assembly_in_release


class RPMBuilder:
    """ It builds RPMs!
    """

    def __init__(
        self, runtime: Runtime, *, push: bool = True, scratch: bool = False, dry_run: bool = False
    ) -> None:
        """ Create a RPMBuilder instance.
        :param runtime: Doozer runtime
        :param scratch: Whether to push commits and upload sources to distgit
        :param scratch: Whether to create a scratch build
        :param dry_run: Don't build anything but just exercise the code
        """
        self._runtime = runtime
        self._push = push
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
        release += ".g" + rpm.pre_init_sha[:7]

        if self._runtime.assembly:
            release += f'.assembly.{self._runtime.assembly}'

        rpm.set_nvr(version, release)

        # run modifications
        if rpm.config.content.source.modifications is not Missing:
            logger.info("Running custom modifications...")
            await exectools.to_thread(
                rpm._run_modifications, rpm.specfile, rpm.source_path
            )

        # generate new specfile
        tarball_name = f"{rpm.config.name}-{rpm.version}-{rpm.release}.tar.gz"
        logger.info("Creating rpm spec file...")
        source_commit_url = '{}/commit/{}'.format(rpm.public_upstream_url, rpm.pre_init_sha)
        go_compliance_shim = self._runtime.group_config.compliance.rpm_shim.enabled  # Missing is Falsey
        specfile = await self._populate_specfile_async(rpm, tarball_name, source_commit_url, go_compliance_shim=go_compliance_shim)
        dg_specfile_path = dg.dg_path / Path(rpm.specfile).name
        async with aiofiles.open(dg_specfile_path, "w") as f:
            await f.writelines(specfile)

        if rpm.get_package_name_from_spec() != rpm.get_package_name():
            raise IOError(f'RPM package name in .spec file ({rpm.get_package_name_from_spec()}) does not match doozer metadata name {rpm.get_package_name()}')

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

        if self._push:
            if not self._dry_run:
                await exectools.cmd_assert_async(
                    ["rhpkg", "new-sources", tarball_name], cwd=dg.dg_path, retries=3
                )
            else:
                async with aiofiles.open(dg.dg_path / "sources", "w") as f:
                    f.write("SHA512 ({}) = {}\n".format(tarball_name, "0" * 128))
                if self._push:
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
            logger.debug("Copying %s", filename)

            shutil.copy(src, dest, follow_symlinks=False)

        # commit changes
        logger.info("Committing distgit changes...")
        await aiofiles.os.remove(tarball_path)
        commit_hash = await exectools.to_thread(dg.commit,
                                                f"Automatic commit of package [{rpm.config.name}] release [{rpm.version}-{rpm.release}].",
                                                commit_attributes={
                                                    'version': rpm.version,
                                                    'release': rpm.release,
                                                    'io.openshift.build.commit.id': rpm.pre_init_sha,
                                                    'io.openshift.build.source-location': rpm.public_upstream_url,
                                                }
                                                )

        if self._push:
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
        if self._runtime.assembly and isolate_assembly_in_release(rpm.release) != self._runtime.assembly:
            # Assemblies should follow its naming convention
            raise ValueError(f"RPM {rpm.name} is not rebased with assembly '{self._runtime.assembly}'.")
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
            nvrs = []
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
                # All tasks complete.
                with self._runtime.shared_koji_client_session() as koji_api:
                    if not koji_api.logged_in:
                        koji_api.gssapi_login()
                    with koji_api.multicall(strict=True) as m:
                        multicall_tasks = [m.listBuilds(taskID=task_id, completeBefore=None) for task_id in task_ids]    # this call should not be constrained by brew event
                    nvrs = [task.result[0]["nvr"] for task in multicall_tasks]
                    if self._runtime.hotfix:
                        # Tag rpms so they don't get garbage collected.
                        hotfix_tags = rpm.hotfix_brew_tags()
                        self._runtime.logger.info(f'Tagging build(s) {nvrs} info {hotfix_tags} to prevent garbage collection')
                        with koji_api.multicall(strict=True) as m:
                            for nvr, hotfix_tag in zip(nvrs, hotfix_tags):
                                m.tagBuild(hotfix_tag, nvr)

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
        return task_ids, task_urls, nvrs

    async def _golang_required(self, specfile: PathLike):
        """Returns True if this RPM requires a golang compiler"""
        out, _ = await exectools.cmd_assert_async(
            ["rpmspec", "-q", "--buildrequires", "--", specfile]
        )
        return any(dep.strip().startswith("golang") for dep in out.splitlines())

    @staticmethod
    async def _populate_specfile_async(
        rpm: RPMMetadata, source_filename: str, source_commit_url: str, go_compliance_shim=False
    ) -> List[str]:
        """Populates spec file
        :param source_filename: Path to the template spec file
        """
        if not rpm.source_path:
            raise ValueError("Source is not cloned.")
        project, component = await exectools.to_thread(rpm.get_jira_info)
        maintainer_string = f'[Maintainer] project: {project}, component: {component}'

        # otherwise, make changes similar to tito tagging
        rpm_spec_tags = {
            "Name:": "Name:           {}\n".format(rpm.config.name),
            "Version:": "Version:        {}\n".format(rpm.version),
            "Release:": "Release:        {}%{{?dist}}\n".format(rpm.release),
            "Source0:": f"Source0:        {source_filename}\n",
        }

        # rpm.version example: 4.12.0 or 4.12.0~rc.4 if pre-release
        # Extract the major, minor, patch
        version_split = rpm.version.split("~", 1)
        major, minor, patch = version_split[0].split(".", 2)
        # If rpm.version contains `~`, it is a pre-release
        pre_release = version_split[1] if len(version_split) > 1 else ""
        commit_sha = rpm.pre_init_sha
        if pre_release:
            full = f"{major}.{minor}.{patch}-{pre_release}-{rpm.release}-{commit_sha[0:7]}"
        else:
            full = f"{major}.{minor}.{patch}-{rpm.release}-{commit_sha[0:7]}"

        current_time = time.strftime('%a %b %d %Y', time.localtime(time.time()))
        changelog_title = f"* {current_time} AOS Automation Release Team <noreply@redhat.com> - {rpm.version}-{rpm.release}"

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
            elif "%global os_git_vars " in line:
                lines[
                    i
                ] = f"%global os_git_vars OS_GIT_VERSION={full} OS_GIT_MAJOR={major} OS_GIT_MINOR={minor} OS_GIT_PATCH={patch} OS_GIT_COMMIT={commit_sha} OS_GIT_TREE_STATE=clean"
                for k, v in rpm.extra_os_git_vars.items():
                    lines[i] += f" {k}={v}"
                lines[i] += "\n"
            elif "%global commit" in line:
                lines[i] = re.sub(
                    r"commit\s+\w+", "commit {}".format(commit_sha), lines[i]
                )
            elif line.startswith("%setup"):
                lines[i] = f"%setup -q -n {rpm.config.name}-{rpm.version}\n"
            elif line.startswith("%autosetup"):
                lines[i] = f"%autosetup -S git -n {rpm.config.name}-{rpm.version} -p1\n"
            elif line.startswith("%changelog"):
                lines[i] = f"{lines[i].strip()}\n{changelog_title}\n- Update to source commit {source_commit_url}\n"
            elif line.startswith("%build"):
                if go_compliance_shim:
                    rpm_builder_go_wrapper_sh = pathlib.Path(pathlib.Path(__file__).parent, 'rpm_builder_go_wrapper.sh').read_text()
                    lines[i] = f'''{line}
export REAL_GO_PATH=$(which go || true)
if [[ -n "$REAL_GO_PATH" ]]; then
    GOSHIM_DIR=/tmp/goshim
    mkdir -p $GOSHIM_DIR
    ln -s $REAL_GO_PATH $GOSHIM_DIR/go.real
    export PATH=$GOSHIM_DIR:$PATH
    # Use single quotes 'EOF' to avoid variable expansion.
cat > $GOSHIM_DIR/go << 'EOF'
{rpm_builder_go_wrapper_sh}
EOF
    chmod +x $GOSHIM_DIR/go
fi
\n'''
                else:
                    # If we are not enforcing compliance, do nothing to the line.
                    pass
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
        return await brew.watch_tasks_async(brew_session, logger.info, task_ids)
