from unittest import TestCase

from mock import MagicMock

from doozerlib.metadata import Metadata


class TestMetadata(TestCase):

    def test_cgit_url(self):
        data_obj = MagicMock(key="foo", filename="foo.yml", data={"name": "foo"})
        runtime = MagicMock()
        runtime.group_config.urls.cgit = "http://distgit.example.com/cgit"
        meta = Metadata("image", runtime, data_obj)
        url = meta.cgit_file_url("some_path/some_file.txt", "abcdefg", "some-branch")
        self.assertEqual(url, "http://distgit.example.com/cgit/containers/foo/plain/some_path/some_file.txt?h=some-branch&id=abcdefg")
