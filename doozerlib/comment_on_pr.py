import pprint
import sys
import requests
import re
import logging


class CommentOnPr:
    """
    Comment on origin PR about the status of build, nightly, releases.
    """

    def __init__(self, job_id: int, logger=None):
        self.art_dash_api_endpoint = "http://art-dash-server-aos-art-web.apps.ocp4.prod.psi.redhat.com/api/v1"
        self.github_api_base_url_openshift = "https://api.github.com/repos/openshift"
        self.job_id = job_id

        self.logger = logger
        if logger is None:
            logging.basicConfig(level=logging.INFO)
            self.logger = logging.getLogger()

    def _pr_of_builds_from_job(self):
        """
        Get the PR urls of the build details of an images that are successfully built.

        job_no: The jenkins job number
        """
        url = f"{self.art_dash_api_endpoint}/builds?jenkins_build_url__icontains=ocp4/{self.job_id}"
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

                regex = r"^https:\/\/github\.com\/(?P<org>[a-z]+)\/(?P<repo_name>[a-z-]+)\/commit\/(?P<sha>\w+)$"
                match = re.match(regex, build_commit_url)
                self.logger.debug(f"regex matches: {pprint.pformat(match.groupdict())}")

                _, self.repo_name, sha = match.groupdict().values()

                github_api_url = f"{self.github_api_base_url_openshift}/{self.repo_name}/commits/{sha}/pulls"
                self.logger.debug(f"Querying GitHub API with url: {github_api_url}")

                pulls = requests.get(github_api_url)
                if pulls.status_code != 200:
                    self.logger.error(f"GitHub API error. Status code: {pulls.status_code}")
                    sys.exit(1)

                pulls = pulls.json()
                self.logger.debug(f"Pull requests: {pprint.pformat(pulls)}")

                if len(pulls) == 1:
                    pull_url = pulls[0]["html_url"]
                    pull_request_urls.append({"repo_name": self.repo_name, "pr": pull_url, "build_id": build_id,
                                              "distgit_name": distgit_name})
                else:
                    self.logger.error("More than one PRs were found for a merge commit")
            return pull_request_urls
        else:
            self.logger.info(f"No builds were found for job no: {self.job_id} ")

    def _get_comments_from_pr(self, repo_name: str, pr_no: int):
        """
        Get the comments from a given pull request
        """
        github_api_url = f"{self.github_api_base_url_openshift}/{repo_name}/issues/{pr_no}/comments"
        self.logger.debug(f"Querying GitHub API with url: {github_api_url}")

        response = requests.get(github_api_url)
        if response.status_code != 200:
            self.logger.error(f"GitHub API error. Status code: {response.status_code}")
            sys.exit(1)

        comments = response.json()
        self.logger.debug(f"Comments from PR: {pprint.pformat(comments)}")

        return comments

    def _check_if_pr_needs_reporting(self, repo_name: str, pr_no: int, type_: str):
        """
        Check the comments to see if the bot has already reported so as not to prevent spamming

        repo_name: Name of the repository. Eg: console
        pr_no: The Pull request ID/No
        type_: The type of notification. PR | NIGHTLY | RELEASE
        """

        comments = self._get_comments_from_pr(repo_name, pr_no)

        for comment in comments:
            body = comment["body"]
            self.logger.debug(f"Comment body: {body}")

            if f"[ART PR {type_} NOTIFIER]" not in body:
                return True
        return False

    def _check_builds(self):
        """
        Returns a list of repo PRs that needs reporting to.
        """
        data = self._pr_of_builds_from_job()
        results = []
        for dict_ in data:
            repo_name, pull_request, build_id, distgit_name = dict_.values()
            pr_no = pull_request.split("/")[-1]
            self.logger.debug(f"repo_name: {repo_name}\n"
                              f"pull_request: {pull_request}\n"
                              f"build_id: {build_id}\n"
                              f"pr_no: {pr_no}")

            if self._check_if_pr_needs_reporting(repo_name, pr_no, "BUILD"):
                results.append({"repo_name": repo_name, "pr_no": pr_no, "build_id": build_id,
                                "distgit_name": distgit_name})
        return results

    def run(self):
        return {"from_builds": self._check_builds()}
