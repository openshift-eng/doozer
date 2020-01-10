from __future__ import absolute_import, print_function, unicode_literals
import unittest
from doozerlib import distgit


class TestDistgitConvertSourceURLToHTTPS(unittest.TestCase):

    def test_conversion_from_ssh_source(self):
        source = "git@github.com:myorg/myproject.git"

        actual = distgit.convert_source_url_to_https(source)
        expected = "https://github.com/myorg/myproject"

        self.assertEqual(actual, expected)

    def test_conversion_from_already_https_source(self):
        source = "https://github.com/myorg/myproject"

        actual = distgit.convert_source_url_to_https(source)
        expected = "https://github.com/myorg/myproject"

        self.assertEqual(actual, expected)

    def test_conversion_from_https_source_with_dotgit_suffix(self):
        source = "https://github.com/myorg/myproject.git"

        actual = distgit.convert_source_url_to_https(source)
        expected = "https://github.com/myorg/myproject"

        self.assertEqual(actual, expected)

    def test_conversion_from_https_source_with_dotgit_elsewhere(self):
        source = "https://foo.gitlab.com/myorg/myproject"

        actual = distgit.convert_source_url_to_https(source)
        expected = "https://foo.gitlab.com/myorg/myproject"

        self.assertEqual(actual, expected)


if __name__ == '__main__':
    unittest.main()
