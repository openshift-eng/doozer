import re
import traceback
from time import sleep
from typing import Dict, Optional, Tuple
from urllib.parse import quote

import koji

from doozerlib import brew, distgit, exectools, image, runtime
from doozerlib.constants import BREWWEB_URL, DISTGIT_GIT_URL
from doozerlib.exceptions import DoozerFatalError


class OSBS2BuildError(Exception):
    def __init__(self, message: str, task_id: int, task_url: Optional[str]) -> None:
        super().__init__(message)
        self.task_id = task_id
        self.task_url = task_url


class OSBS2Builder:
    """ Builds container images with OSBS 2
    """

    def __init__(
        self, runtime: "runtime.Runtime", *, scratch: bool = False, dry_run: bool = False
    ) -> None:
        """ Create a OSBS2Builder instance.
        :param runtime: Doozer runtime
        :param scratch: Whether to create a scratch build
        :param dry_run: Don't build anything but just exercise the code
        """
        self._runtime = runtime
        self.scratch = scratch
        self.dry_run = dry_run

    async def build(self, image: "image.ImageMetadata", profile: Dict, retries: int = 3) -> Tuple[int, Optional[str], Optional[Dict]]:
        """ Build an image
        :param image: Image metadata
        :param profile: Build profile
        :param retries: The number of times to retry
        :return: (task_id, task_url, build_info)
        """
        dg: "distgit.ImageDistGitRepo" = image.distgit_repo()
        logger = dg.logger
        if len(image.targets) > 1:
            # Currently we don't really support building images against multiple targets,
            # or we would overwrite the image tag when pushing to the registry.
            # `targets` is defined as an array just because we want to keep consistency with RPM build.
            raise DoozerFatalError("Building images against multiple targets is not currently supported.")
        target = image.targets[0]
        task_id = 0
        task_url = None
        build_info = None
        build_url = None
        logger.info("OSBS 2: Building image %s...", image.name)
        koji_api = self._runtime.build_retrying_koji_client()
        if not koji_api.logged_in:
            await exectools.to_thread(koji_api.gssapi_login)

        error = None
        message = None
        nvr = f"{image.get_component_name()}-{dg.org_version}-{dg.org_release}"
        for attempt in range(retries):
            logger.info("Build attempt %s/%s", attempt + 1, retries)
            try:
                # Submit build task
                task_id, task_url = await exectools.to_thread(self._start_build, dg, target, profile, koji_api)
                logger.info("Waiting for build task %s to complete...", task_id)
                if self.dry_run:
                    logger.warning("[DRY RUN] Build task %s would have completed", task_id)
                    error = None
                else:
                    error = await brew.watch_task_async(koji_api, logger.info, task_id)

                # Gather brew-logs
                logger.info("Gathering brew-logs")
                cmd = ["brew", "download-logs", "--recurse", "-d", dg._logs_dir(), task_id]
                if self.dry_run:
                    logger.warning("[DRY RUN] Would have downloaded Brew logs with %s", cmd)
                else:
                    logs_rc, _, logs_err = exectools.cmd_gather(cmd)
                    if logs_rc != 0:
                        logger.warning("Error downloading build logs from brew for task %s: %s", task_id, logs_err)

                if error:
                    # Error in task does not necessarily mean error in build. Look if the build is successful
                    build_info = koji_api.getBuild(nvr)
                    if build_info and build_info.get('state') == 1:  # State 1 means complete.
                        build_url = f"{BREWWEB_URL}/buildinfo?buildID={build_info['id']}"
                        logger.info("Image %s already built against this dist-git commit (or version-release tag): %s", build_info["nvr"], build_url)
                        error = None  # Treat as a success

            except Exception as err:
                error = f"Error building image {image.name}: {str(err)}: {traceback.format_exc()}"

            if not error:  # Build succeeded
                # Get build_id and build_info
                if self.dry_run:
                    build_id = 0
                    build_info = {
                        "id": build_id,
                        "name": image.get_component_name(),
                        "version": dg.org_version,
                        "release": dg.org_release,
                        "nvr": nvr,
                    }
                    build_url = f"{BREWWEB_URL}/buildinfo?buildID={build_info['id']}"
                elif not build_info and not self.scratch:
                    # Unlike rpm build, koji_api.listBuilds(taskID=...) doesn't support image build. For now, let's use a different approach.
                    taskResult = koji_api.getTaskResult(task_id)
                    build_id = int(taskResult["koji_builds"][0])
                    build_info = koji_api.getBuild(build_id)
                    build_url = f"{BREWWEB_URL}/buildinfo?buildID={build_info['id']}"
                break

            # An error occurred. We don't have a viable build.
            message = f"Build failed: {error}"
            logger.warning(
                "Error building image %s [attempt #%s] in Brew: %s",
                image.name,
                attempt + 1,
                message,
            )
            if attempt < retries - 1:
                # Brew does not handle an immediate retry correctly, wait before trying another build
                logger.info("Will retry in 5 minutes")
                sleep(5 * 60)

        if error:
            raise OSBS2BuildError(
                f"Giving up after {retries} failed attempt(s): {message}",
                task_id, task_url
            )

        if build_info:
            logger.info("Successfully built image %s; task: %s ; nvr: %s ; build record: %s ", image.name, task_url, build_info["nvr"], build_url)
        else:
            logger.info("Successfully built image %s without a build record; task: %s", image.name, task_url)

        if not self.scratch and self._runtime.hotfix:
            # Tag the image so it won't get garbage collected.
            logger.info(f'Tagging {image.get_component_name()} build {build_info["nvr"]} into {image.hotfix_brew_tag()}'
                        f' to prevent garbage collection')
            if self.dry_run:
                logger.warning("[DRY RUN] Build %s would have been tagged into %s", build_info["nvr"], image.hotfix_brew_tag())
            else:
                koji_api.tagBuild(image.hotfix_brew_tag(), build_info["nvr"])
                logger.warning("Build %s has been tagged into %s", build_info["nvr"], image.hotfix_brew_tag())

        return task_id, task_url, build_info

    def _start_build(self, dg: "distgit.ImageDistGitRepo", target: str, profile: Dict, koji_api: koji.ClientSession):
        logger = dg.logger
        src = self._construct_build_source_url(dg)
        signing_intent = profile["signing_intent"]
        repo_type = profile["repo_type"]
        repo_list = profile["repo_list"]
        if repo_type and not repo_list:  # If --repo was not specified on the command line
            repo_file = f".oit/{repo_type}.repo"
            if not self.dry_run:
                existence, repo_url = dg.cgit_file_available(repo_file)
            else:
                logger.warning("[DRY RUN] Would have checked if cgit repo file is present.")
                existence, repo_url = True, f"https://cgit.example.com/{repo_file}"
            if not existence:
                raise FileNotFoundError(f"Repo file {repo_file} is not available on cgit; cgit cache may not be"
                                        f" reflecting distgit in a timely manner.")
            repo_list = [repo_url]

        opts = {
            'scratch': self.scratch,
            'signing_intent': signing_intent,
            'yum_repourls': repo_list,
            'git_branch': dg.branch,
        }

        task_id = 0
        logger.info("Starting OSBS 2 build with source %s and target %s...", src, target)
        if self.dry_run:
            logger.warning("[DRY RUN] Would have started container build")
        else:
            if not koji_api.logged_in:
                koji_api.gssapi_login()
            task_id: int = koji_api.buildContainer(src, target, opts=opts, channel="container-binary")

        task_url = f"{BREWWEB_URL}/taskinfo?taskID={task_id}"
        logger.info("OSBS 2 build started. Task ID: %s, url: %s", task_id, task_url)
        return task_id, task_url

    @staticmethod
    def _construct_build_source_url(dg: "distgit.ImageDistGitRepo"):
        if not dg.sha:
            raise ValueError(f"Image {dg.name} Distgit commit sha is unknown")
        return f"{DISTGIT_GIT_URL}/{quote(dg.metadata.namespace)}/{quote(dg.name)}#{quote(dg.sha)}"
