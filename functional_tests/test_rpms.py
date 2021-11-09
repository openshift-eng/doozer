import unittest
import os
import pygit2
from . import run_doozer, get_working_dir


class TestRPMs(unittest.TestCase):
    def test_rpms_clone_sources(self):
        _, out, _ = run_doozer([
            "--group=openshift-3.11",
            "--rpms=openshift-ansible",
            "rpms:clone-sources"
        ])
        working_dir = get_working_dir()
        repo_path = os.path.join(working_dir, "sources", "rpm_openshift-ansible_openshift-ansible")
        repo = pygit2.Repository(repo_path)
        ref = repo.references["HEAD"].resolve()  # type: pygit2.Reference
        self.assertEqual("release-3.11", ref.shorthand)
        self.assertTrue(os.path.isfile(os.path.join(repo_path, "openshift-ansible.spec")))

    def test_rpms_clone(self):
        _, out, _ = run_doozer([
            "--group=openshift-3.11",
            "--rpms=openshift-ansible",
            "rpms:clone"
        ])
        working_dir = get_working_dir()
        repo_path = os.path.join(working_dir, "distgits", "rpms", "openshift-ansible")
        repo = pygit2.Repository(repo_path)
        ref = repo.references["HEAD"].resolve()  # type: pygit2.Reference
        self.assertEqual("rhaos-3.11-rhel-7", ref.shorthand)
        self.assertTrue(os.path.isfile(os.path.join(repo_path, "openshift-ansible.spec")))

    def test_rpms_build(self):
        _, out, _ = run_doozer([
            "--group=openshift-3.11",
            "--rpms=openshift-ansible",
            "rpms:build",
            "--version=0.0.0",
            "--release=1",
            "--dry-run",
        ])
