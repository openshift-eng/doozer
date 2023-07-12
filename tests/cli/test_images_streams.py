from unittest import TestCase
from flexmock import flexmock

from doozerlib.cli import images_streams


class TestScanSourcesCli(TestCase):
    def test_connect_issue_with_pr(self):
        pr = flexmock(title="test pr", html_url="https://github.com/openshift/OCBUGS/pulls/1234")
        pr.should_receive("edit").once()
        images_streams.connect_issue_with_pr(pr, "OCPBUGS-1234")

    def test_connect_issue_with_pr_bug_in_title(self):
        pr = flexmock(title="OCPBUGS-1111: test pr", html_url="https://github.com/openshift/OCBUGS/pulls/1234")
        pr.should_receive("get_issue_comments").and_return([])
        pr.should_receive("create_issue_comment").once()
        images_streams.connect_issue_with_pr(pr, "OCPBUGS-1234")
