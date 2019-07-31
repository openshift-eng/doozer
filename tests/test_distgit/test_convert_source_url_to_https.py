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
