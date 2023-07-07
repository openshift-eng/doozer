from unittest import TestCase
from flexmock import flexmock

from doozerlib.cli import images_streams


class TestScanSourcesCli(TestCase):
    def test_connect_issue_with_pr(self):
        github_client = flexmock()
        source_repo = flexmock()
        pr = flexmock(title="test pr")
        github_client.should_receive("get_repo").and_return(source_repo)
        source_repo.should_receive("get_pull").and_return(pr)
        pr.should_receive("edit").once()
        images_streams.connect_issue_with_pr(github_client, "https://github.com/openshift/OCBUGS/pulls/1234", "OCPBUGS-1234")

    def test_connect_issue_with_pr_bug_in_title(self):
        github_client = flexmock()
        source_repo = flexmock()
        pr = flexmock(title="OCPBUGS-1111: test pr")
        github_client.should_receive("get_repo").and_return(source_repo)
        source_repo.should_receive("get_pull").and_return(pr)
        pr.should_receive("get_issue_comments").and_return([])
        pr.should_receive("create_issue_comment").once()
        images_streams.connect_issue_with_pr(github_client, "https://github.com/openshift/OCBUGS/pulls/1234", "OCPBUGS-1234")
