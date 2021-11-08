import unittest
from . import run_doozer, get_working_dir
import os


class TestCleanup(unittest.TestCase):
    def test_cleanup(self):
        # to initialize working directory
        working_dir = get_working_dir()
        _, out, _ = run_doozer([
            "--group=openshift-3.11",
            "config:get"
        ])
        # doing cleanup
        _, out, _ = run_doozer([
            "--group=openshift-3.11",
            "cleanup"
        ])
        # ensure everyting except settings.yaml is cleaned up
        for f in os.listdir(working_dir):
            self.assertFalse(f != "setting.yaml", "{} is not deleted.".format(f))
