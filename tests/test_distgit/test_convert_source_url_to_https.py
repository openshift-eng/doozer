import unittest
from doozerlib import util


class TestDistgitConvertSourceURLToHTTPS(unittest.TestCase):

    def test_conversion_from_ssh_source(self):
        source = "git@github.com:myorg/myproject.git"

        actual = util.convert_remote_git_to_https(source)
        expected = "https://github.com/myorg/myproject"

        self.assertEqual(actual, expected)

    def test_conversion_from_already_https_source(self):
        source = "https://github.com/myorg/myproject"

        actual = util.convert_remote_git_to_https(source)
        expected = "https://github.com/myorg/myproject"

        self.assertEqual(actual, expected)

    def test_conversion_from_https_source_with_dotgit_suffix(self):
        source = "https://github.com/myorg/myproject.git"

        actual = util.convert_remote_git_to_https(source)
        expected = "https://github.com/myorg/myproject"

        self.assertEqual(actual, expected)

    def test_conversion_from_https_source_with_dotgit_elsewhere(self):
        source = "https://foo.gitlab.com/myorg/myproject"

        actual = util.convert_remote_git_to_https(source)
        expected = "https://foo.gitlab.com/myorg/myproject"

        self.assertEqual(actual, expected)

    def test_conversion_from_ssh_org_source(self):
        expected = "https://github.com/myorg"
        for source in ["git@github.com:myorg/", "git@github.com/myorg/", 'ssh://someone@github.com/myorg', 'ssh://someone@github.com:myorg']:
            actual = util.convert_remote_git_to_https(source)
            self.assertEqual(actual, expected)

    def test_conversion_from_https_org_source(self):
        source = "https://github.com/myorg/"

        actual = util.convert_remote_git_to_https(source)
        expected = "https://github.com/myorg"

        self.assertEqual(actual, expected)


if __name__ == '__main__':
    unittest.main()
