from ghapi.all import GhApi
from doozerlib.pushd import Dir
from dockerfile_parse import DockerfileParser


class CommentOnPr:
    def __init__(self, distgit_dir: str, token: str):
        self.distgit_dir = distgit_dir
        self.token = token
        self.owner = None
        self.repo = None
        self.commit = None
        self.gh_client = None  # GhApi client
        self.pr_url = None
        self.pr_no = None

    def list_comments(self):
        """
        List the comments in a PR
        """
        # https://docs.github.com/rest/reference/issues#list-issue-comments
        return self.gh_client.issues.list_comments(issue_number=self.pr_no, per_page=100)

    def check_if_comment_exist(self, comment):
        """
        Check if the same comment already exists in the PR
        """
        issue_comments = self.list_comments()
        for issue_comment in issue_comments:
            if issue_comment["body"] == comment:
                return True
        return False

    def post_comment(self, comment):
        """
        Post the comment in the PR if the comment doesn't exist already
        """
        # https://docs.github.com/rest/reference/issues#create-an-issue-comment
        if not self.check_if_comment_exist(comment):
            self.gh_client.issues.create_comment(issue_number=self.pr_no, body=comment)
            return True
        return False

    def set_pr_from_commit(self):
        """
        Get the PR from the merge commit
        """
        # https://docs.github.com/rest/commits/commits#list-pull-requests-associated-with-a-commit
        prs = self.gh_client.repos.list_pull_requests_associated_with_commit(self.commit)
        if len(prs) == 1:
            # self._logger.info(f"PR from merge commit {sha}: {pull_url}")
            self.pr_url = prs[0]["html_url"]
            self.pr_no = prs[0]["number"]
            return
        raise Exception(f"Multiple PRs found for merge commit {self.commit}")

    def set_repo_details(self):
        """
        Get the owner, commit and repo from the dfp label
        """
        with Dir(self.distgit_dir):
            dfp = DockerfileParser(str(Dir.getpath().joinpath('Dockerfile')))

            # eg: "https://github.com/openshift/origin/commit/660e0c785a2c9b1fd5fad33cbcffd77a6d84ccb5"
            source_commit_url = dfp.labels["io.openshift.build.commit.url"]
            url_split = source_commit_url.split("/")
            commit = url_split[-1]  # eg: 660e0c785a2c9b1fd5fad33cbcffd77a6d84ccb5
            repo = url_split[-3]  # eg: origin
            owner = url_split[-4]  # eg: openshift

            self.owner = owner
            self.commit = commit
            self.repo = repo

    def set_github_client(self):
        """
        Set the gh client after the get_source_details function is run
        """
        self.gh_client = GhApi(owner=self.owner, repo=self.repo, token=self.token)
