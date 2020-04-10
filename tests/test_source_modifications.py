from __future__ import absolute_import, print_function, unicode_literals

import unittest
import tempfile
import os
import shutil
import mock
import pathlib

from doozerlib.source_modifications import SourceModifierFactory, AddModifier


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
        expected_content = "some\ncontent"
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


if __name__ == "__main__":
    unittest.main()
