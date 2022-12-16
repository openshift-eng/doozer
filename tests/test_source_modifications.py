
import os
import pathlib
import shutil
import tempfile
import unittest

from unittest import mock
import yaml.scanner

from doozerlib.source_modifications import (AddModifier, RemoveModifier,
                                            SourceModifierFactory)


class SourceModifierFactoryTestCase(unittest.TestCase):
    def setUp(self):
        self.factory = SourceModifierFactory()

    def test_supports(self):
        self.assertTrue(self.factory.supports("add"))
        self.assertFalse(self.factory.supports("nonexistent"))

    def test_create(self):
        add_modifier = self.factory.create(action="add", source="https://example.com/gating_yaml", path="gating.yaml")
        self.assertIsInstance(add_modifier, AddModifier)
        with self.assertRaises(KeyError):
            self.factory.create(action="nonexistent")


class AddModifierTestCase(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_act_success(self):
        params = {
            "source": "http://example.com/gating_yaml",
            "path": os.path.join(self.temp_dir, "gating.yaml"),
            "overwriting": True,
        }
        expected_content = b"some\ncontent"
        modifier = AddModifier(**params)
        with mock.patch("requests.Session") as MockSession:
            session = MockSession()
            response = session.get.return_value
            response.content = expected_content
            context = {"distgit_path": pathlib.Path(self.temp_dir)}
            modifier.act(ceiling_dir=self.temp_dir, session=session, context=context)
        with open(params["path"], "rb") as f:
            actual = f.read()
        self.assertEqual(actual, expected_content)

    def test_act_failed_with_file_exists(self):
        params = {
            "source": "http://example.com/gating_yaml",
            "path": os.path.join(self.temp_dir, "gating.yaml"),
            "overwriting": False,
        }
        expected_content = b"some\ncontent"
        modifier = AddModifier(**params)
        with open(params["path"], "w") as f:
            f.write("some existing file")
        with mock.patch("requests.Session") as MockSession:
            session = MockSession()
            response = session.get.return_value
            response.content = expected_content
            with self.assertRaises(IOError) as cm:
                context = {"distgit_path": pathlib.Path(self.temp_dir)}
                modifier.act(ceiling_dir=self.temp_dir, session=session, context=context)
            self.assertIn("overwrite", repr(cm.exception))

    def test_act_failed_with_invalid_yaml(self):
        params = {
            "source": "http://example.com/gating_yaml",
            "path": os.path.join(self.temp_dir, "gating.yaml"),
            "overwriting": True,
            "validate": "yaml"
        }
        expected_content = b"@!abc123"
        modifier = AddModifier(**params)
        with open(params["path"], "w") as f:
            f.write("some existing file")
        with mock.patch("requests.Session") as MockSession:
            session = MockSession()
            response = session.get.return_value
            response.content = expected_content
            with self.assertRaises(yaml.scanner.ScannerError):
                context = {"distgit_path": pathlib.Path(self.temp_dir)}
                modifier.act(ceiling_dir=self.temp_dir, session=session, context=context)


class TestRemoveModifier(unittest.TestCase):
    def test_remove_file(self):
        distgit_path = pathlib.Path("/path/to/distgit")
        modifier = RemoveModifier(glob="**/*.txt", distgit_path=distgit_path)
        context = {
            "component_name": "foo",
            "kind": "Dockerfile",
            "content": "whatever",
            "distgit_path": distgit_path,
        }
        with mock.patch.object(pathlib.Path, "rglob") as rglob, mock.patch.object(pathlib.Path, "unlink") as unlink:
            rglob.return_value = map(lambda path: distgit_path.joinpath(path), [
                "1.txt",
                "a/2.txt",
                "b/c/d/e/3.txt",
            ])
            modifier.act(context=context, ceiling_dir=str(distgit_path))
            unlink.assert_called()

    def test_remove_file_outside_of_distgit_dir(self):
        distgit_path = pathlib.Path("/path/to/distgit")
        modifier = RemoveModifier(glob="**/*.txt", distgit_path=distgit_path)
        context = {
            "component_name": "foo",
            "kind": "Dockerfile",
            "content": "whatever",
            "distgit_path": distgit_path,
        }
        with mock.patch.object(pathlib.Path, "rglob") as rglob:
            rglob.return_value = map(lambda path: pathlib.Path("/some/other/path").joinpath(path), [
                "1.txt",
                "a/2.txt",
                "b/c/d/e/3.txt",
            ])
            with self.assertRaises(PermissionError):
                modifier.act(context=context, ceiling_dir=str(distgit_path))


if __name__ == "__main__":
    unittest.main()
