import pprint
import sys
import requests
import re
from doozerlib.logutil import getLogger

ART_DASH_API_ENDPOINT = "http://art-dash-server-aos-art-web.apps.ocp4.prod.psi.redhat.com/api/v1"
GITHUB_API_URL_REPOS = "https://api.github.com/repos"


class CommentOnPr:
    """
    Comment on origin PR about the status of build, nightly, releases.
    """

    def __init__(self, job_id: int, logger=None):
        self.job_id = job_id
        self.logger = logger if logger else getLogger(__name__)

    def _pr_of_builds_from_job(self):
        """
        Get the PR urls of the build details of an images that are successfully built.

        job_no: The jenkins job number
        """
        url = f"{ART_DASH_API_ENDPOINT}/builds?jenkins_build_url__icontains=ocp4/{self.job_id}"
        self.logger.debug(f"Querying ART Dash server with url: {url}")

        response = requests.get(url)
        if response.status_code != 200:
            self.logger.error(f"ART DASH Server error. Status code: {response.status_code}")
            sys.exit(1)

        data = response.json()
        self.logger.debug(f"Response: {pprint.pformat(data)}")

        if data["count"] > 0:
            pull_request_urls = []
            results = data["results"]

            for build in results:
                if build["brew_task_state"] != "success":
                    # Do not check for unsuccessful builds
                    continue

                build_commit_url = build["label_io_openshift_build_commit_url"]
                build_id = build["build_0_id"]
                distgit_name = build["dg_name"]
                nvr = build["build_0_nvr"]

                regex = r"^https:\/\/github\.com\/(?P<org>[a-z-]+)\/(?P<repo_name>[a-z-]+)\/commit\/(?P<sha>\w+)$"
                match = re.match(regex, build_commit_url)
                self.logger.debug(f"regex matches: {pprint.pformat(match.groupdict())}")

                org, repo_name, sha = match.groupdict().values()

                github_api_url = f"{GITHUB_API_URL_REPOS}/{org}/{repo_name}/commits/{sha}/pulls"
                self.logger.info(f"Querying GitHub API with url: {github_api_url}")

                pulls = requests.get(github_api_url)
                if pulls.status_code != 200:
                    self.logger.error(f"GitHub API error. Status code: {pulls.status_code}")
                    sys.exit(1)

                pulls = pulls.json()
                self.logger.info(f"Pull requests: {pprint.pformat(pulls)}")

                if len(pulls) == 1:
                    pull_url = pulls[0]["html_url"]
                    pull_request_urls.append({"org": org, "repo_name": repo_name, "pr": pull_url, "build_id": build_id,
                                              "distgit_name": distgit_name, "nvr": nvr})
                else:
                    self.logger.error("More than one PRs were found for a merge commit")
            return pull_request_urls
        else:
            self.logger.info(f"No builds were found for job no: {self.job_id} ")

    def _get_comments_from_pr(self, org: str, repo_name: str, pr_no: int):
        """
        Get the comments from a given pull request
        """
        github_api_url = f"{GITHUB_API_URL_REPOS}/{org}/{repo_name}/issues/{pr_no}/comments"
        self.logger.info(f"Querying GitHub API with url: {github_api_url}")

        response = requests.get(github_api_url)
        if response.status_code != 200:
            self.logger.error(f"GitHub API error. Status code: {response.status_code}")
            sys.exit(1)

        comments = response.json()
        self.logger.debug(f"Comments from PR: {pprint.pformat(comments)}")

        return comments

    def _check_if_pr_needs_reporting(self, org: str, repo_name: str, pr_no: int, type_: str):
        """
        Check the comments to see if the bot has already reported so as not to prevent spamming

        repo_name: Name of the repository. Eg: console
        pr_no: The Pull request ID/No
        type_: The type of notification. PR | NIGHTLY | RELEASE
        """

        comments = self._get_comments_from_pr(org, repo_name, pr_no)

        for comment in comments:
            body = comment["body"]
            self.logger.debug(f"Comment body: {body}")

            if f"[ART PR {type_} NOTIFIER]" in body:
                return False
        return True

    def _check_builds(self):
        """
        Returns a list of repo PRs that needs reporting to.
        """
        data = self._pr_of_builds_from_job()
        results = []
        for dict_ in data:
            org, repo_name, pull_request, build_id, distgit_name, nvr = dict_.values()
            pr_no = pull_request.split("/")[-1]
            self.logger.info(f"repo_name: {repo_name}\n"
                             f"org: {org}\n"
                             f"pull_request: {pull_request}\n"
                             f"build_id: {build_id}\n"
                             f"pr_no: {pr_no}\n"
                             f"distgit_name: {distgit_name}\n"
                             f"nvr: {nvr}")

            if self._check_if_pr_needs_reporting(org, repo_name, pr_no, "BUILD"):
                results.append({"org": org, "repo_name": repo_name, "pr_no": pr_no, "pr_url": pull_request,
                                "build_id": build_id, "distgit_name": distgit_name, "nvr": nvr})
        return results

    def run(self):
        return {"from_builds": self._check_builds()}
