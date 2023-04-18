import unittest
from unittest.mock import MagicMock, patch
from doozerlib.comment_on_pr import CommentOnPr


class TestCommentOnPr(unittest.TestCase):
    def setUp(self):
        self.distgit_dir = "distgit_dir"
        self.token = "token"
        self.commit = "commit_sha"
        self.comment = "comment"

    def test_list_comments(self):
        pr_no = 1
        api_mock = MagicMock()
        api_mock.issues.list_comments.return_value = [{"body": "test comment"}]
        comment_on_pr = CommentOnPr(self.distgit_dir, self.token)
        comment_on_pr.pr_no = pr_no
        comment_on_pr.gh_client = api_mock
        result = comment_on_pr.list_comments()
        api_mock.issues.list_comments.assert_called_once_with(issue_number=pr_no, per_page=100)
        self.assertEqual(result, [{"body": "test comment"}])

    @patch.object(CommentOnPr, "list_comments")
    def test_check_if_comment_exist(self, mock_list_comments):
        api_mock = MagicMock()
        api_mock.issues.list_comments.return_value = [{"body": self.comment}]
        mock_list_comments.return_value = api_mock.issues.list_comments()
        comment_on_pr = CommentOnPr(self.distgit_dir, self.token)
        result = comment_on_pr.check_if_comment_exist(self.comment)
        self.assertTrue(result)

    @patch.object(CommentOnPr, "list_comments")
    def test_check_if_comment_exist_when_comment_does_not_exist(self, mock_list_comments):
        api_mock = MagicMock()
        api_mock.issues.list_comments.return_value = [{"body": "test comment"}]
        mock_list_comments.return_value = api_mock.issues.list_comments()
        comment_on_pr = CommentOnPr(self.distgit_dir, self.token)
        result = comment_on_pr.check_if_comment_exist(self.comment)
        self.assertFalse(result)

    @patch.object(CommentOnPr, "check_if_comment_exist")
    def test_post_comment(self, mock_check_if_comment_exist):
        pr_no = 1
        api_mock = MagicMock()
        mock_check_if_comment_exist.return_value = False
        comment_on_pr = CommentOnPr(self.distgit_dir, self.token)
        comment_on_pr.pr_no = pr_no
        comment_on_pr.gh_client = api_mock
        result = comment_on_pr.post_comment(self.comment)
        api_mock.issues.create_comment.assert_called_once_with(issue_number=pr_no, body=self.comment)
        self.assertTrue(result)

    @patch.object(CommentOnPr, "check_if_comment_exist")
    def test_post_comment_when_comment_already_exists(self, mock_check_if_comment_exist):
        pr_no = 1
        api_mock = MagicMock()
        mock_check_if_comment_exist.return_value = True
        comment_on_pr = CommentOnPr(self.distgit_dir, self.token)
        comment_on_pr.pr_no = pr_no
        comment_on_pr.gh_client = api_mock
        result = comment_on_pr.post_comment(self.comment)
        api_mock.issues.create_comment.assert_not_called()
        self.assertFalse(result)

    def test_get_pr_from_commit(self):
        api_mock = MagicMock()
        comment_on_pr = CommentOnPr(self.distgit_dir, self.token)
        comment_on_pr.gh_client = api_mock
        api_mock.repos.list_pull_requests_associated_with_commit.return_value = [{"html_url": "test_url", "number": 1}]
        comment_on_pr.set_pr_from_commit()
        self.assertEqual(comment_on_pr.pr_url, 'test_url')
        self.assertEqual(comment_on_pr.pr_no, 1)

    def test_multiple_prs_for_merge_commit(self):
        api_mock = MagicMock()
        comment_on_pr = CommentOnPr(self.distgit_dir, self.token)
        comment_on_pr.gh_client = api_mock
        api_mock.repos.list_pull_requests_associated_with_commit.return_value = [{"html_url": "test_url", "number": 1},
                                                                                 {"html_url": "test_url_2",
                                                                                  "number": 2}]
        with self.assertRaises(Exception):
            comment_on_pr.set_pr_from_commit()

    def test_set_github_client(self):
        comment_on_pr = CommentOnPr(self.distgit_dir, self.token)
        comment_on_pr.owner = "owner"
        comment_on_pr.repo = "repo"
        comment_on_pr.token = "token"
        comment_on_pr.set_github_client()
        self.assertIsNotNone(comment_on_pr.gh_client)

    @patch('doozerlib.comment_on_pr.DockerfileParser')
    def test_get_source_details(self, mock_parser):
        comment_on_pr = CommentOnPr(self.distgit_dir, self.token)
        # Mocking the labels dictionary of the DockerfileParser object
        mock_parser.return_value.labels = {
            "io.openshift.build.commit.url": "https://github.com/openshift/origin/commit/660e0c785a2c9b1fd5fad33cbcffd77a6d84ccb5"
        }

        # Calling the get_source_details method
        comment_on_pr.set_repo_details()

        # Asserting that the owner, commit, and repo attributes are set correctly
        self.assertEqual(comment_on_pr.owner, 'openshift')
        self.assertEqual(comment_on_pr.commit, '660e0c785a2c9b1fd5fad33cbcffd77a6d84ccb5')
        self.assertEqual(comment_on_pr.repo, 'origin')
