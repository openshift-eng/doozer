import re
import threading
import traceback
from time import sleep
from typing import Dict
from urllib.parse import quote

import koji

from doozerlib import brew, distgit, exectools, image, runtime
from doozerlib.constants import BREWWEB_URL, DISTGIT_GIT_URL
from doozerlib.exceptions import DoozerFatalError


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
        self.task_id: int = 0
        self.task_url: str = ""
        self.nvr: str = ""

    def build(self, image: "image.ImageMetadata", profile: Dict, retries: int = 3):
        dg: "distgit.ImageDistGitRepo" = image.distgit_repo()
        logger = dg.logger

        if len(image.targets) > 1:
            # Currently we don't really support building images against multiple targets,
            # or we would overwrite the image tag when pushing to the registry.
            # `targets` is defined as an array just because we want to keep consistency with RPM build.
            raise DoozerFatalError("Building images against multiple targets is not currently supported.")
        target = image.targets[0]
        build_info = None
        build_url = None
        logger.info("OSBS 2: Building image %s...", image.name)
        koji_api = self._runtime.build_retrying_koji_client()
        if not koji_api.logged_in:
            koji_api.gssapi_login()

        error = None
        message = None
        for attempt in range(retries):
            logger.info("Build attempt %s/%s", attempt + 1, retries)
            try:
                # Submit build task
                self._start_build(dg, target, profile, koji_api)
                logger.info("Waiting for build task %s to complete...", self.task_id)
                if self.dry_run:
                    logger.warning("[DRY RUN] Build task %s would have completed", self.task_id)
                    error = None
                else:
                    error = brew.watch_task(koji_api, logger.info, self.task_id, terminate_event=threading.Event())

                # Gather brew-logs
                logger.info("Gathering brew-logs")
                cmd = ["brew", "download-logs", "--recurse", "-d", dg._logs_dir(), self.task_id]
                if self.dry_run:
                    logger.warning("[DRY RUN] Would have downloaded Brew logs with %s", cmd)
                else:
                    logs_rc, _, logs_err = exectools.cmd_gather(cmd)
                    if logs_rc != 0:
                        logger.warning("Error downloading build logs from brew for task %s: %s", self.task_id, logs_err)

                if error:
                    # Looking for error message like the following to conclude the image has already been built:
                    # BuildError: Build for openshift-enterprise-base-v3.7.0-0.117.0.0 already exists, id 588961
                    # Note it is possible that a Brew task fails
                    # with a build record left (https://issues.redhat.com/browse/ART-1723).
                    # Didn't find a variable in the context to get the Brew NVR or ID.
                    # Extracting the build ID from the error message.
                    # Hope the error message format will not change.
                    match = re.search(r"already exists, id (\d+)", error)
                    if match:
                        build_id = int(match[1])
                        builds = brew.get_build_objects([build_id], koji_api)
                        if builds and builds[0] and builds[0].get('state') == 1:  # State 1 means complete.
                            build_info = builds[0]
                            build_url = f"{BREWWEB_URL}/buildinfo?buildID={build_info['id']}"
                            logger.info(
                                "Image %s already built against this dist-git commit (or version-release tag): %s",
                                build_info["nvr"],
                                build_url
                            )
                            error = None  # Treat as a success

            except Exception as err:
                error = f"Error building image {image.name}: {str(err)}: {traceback.format_exc()}"

            if not error:  # Build succeeded
                # Get build_id and build_info
                if self.dry_run:
                    build_id = 0
                    build_info = {"id": build_id, "nvr": f"{dg.name}-container-{dg.org_version}-{dg.org_release}"}
                elif not build_info:
                    # Unlike rpm build, koji_api.listBuilds(taskID=...) doesn't support image build.
                    # For now, let's use a different approach.
                    task_result = koji_api.getTaskResult(self.task_id)
                    build_id = int(task_result["koji_builds"][0])
                    build_info = koji_api.getBuild(build_id)
                build_url = f"{BREWWEB_URL}/buildinfo?buildID={build_info['id']}"
                self.nvr = build_info["nvr"]
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
            raise exectools.RetryException(
                f"Giving up after {retries} failed attempt(s): {message}",
                (self.task_url, self.task_url),
            )

        logger.info("Successfully built image %s; task: %s; build record: %s", self.nvr, self.task_url, build_url)

        if self._runtime.hotfix:
            # Tag the image so it won't get garbage collected.
            logger.info(f'Tagging {image.get_component_name()} build {build_info["nvr"]} into {image.hotfix_brew_tag()}'
                        f' to prevent garbage collection')
            if self.dry_run:
                logger.warning("[DRY RUN] Build %s would have been tagged into %s", self.nvr, image.hotfix_brew_tag())
            else:
                koji_api.tagBuild(image.hotfix_brew_tag(), build_info["nvr"])
                logger.warning("Build %s has been tagged into %s", self.nvr, image.hotfix_brew_tag())

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

        logger.info("Starting OSBS 2 build with source %s and target %s...", src, target)
        if self.dry_run:
            logger.warning("[DRY RUN] Would have started container build")
        else:
            if not koji_api.logged_in:
                koji_api.gssapi_login()
            self.task_id = koji_api.buildContainer(src, target, opts=opts, channel="container-binary")

        self.task_url = f"{BREWWEB_URL}/taskinfo?taskID={self.task_id}"
        logger.info(f"OSBS2 build started. Task ID: {self.task_id}, url: {self.task_url}")

    @staticmethod
    def _construct_build_source_url(dg: "distgit.ImageDistGitRepo"):
        if not dg.sha:
            raise ValueError(f"Image {dg.name} Distgit commit sha is unknown")
        return f"{DISTGIT_GIT_URL}/containers/{quote(dg.name)}#{quote(dg.sha)}"
