import unittest
import os
import pygit2
import yaml
from . import run_doozer, get_working_dir


class TestImages(unittest.TestCase):
    def test_images_build(self):
        _, out, _ = run_doozer([
            "--group=openshift-3.11",
            "--images=aos3-installation",
            "images:build",
            "--dry-run",
        ])

    def test_images_print(self):
        _, out, _ = run_doozer([
            "--group=openshift-3.11",
            "--images=aos3-installation",
            "images:print",
        ])
        self.assertIn("aos3-installation-container-", out)

    def test_images_foreach(self):
        _, out, _ = run_doozer([
            "--group=openshift-3.11",
            "--images=aos3-installation",
            "images:foreach",
            "echo $doozer_image_name",
        ])
        self.assertIn("openshift3/ose-ansible", out)

    def test_images_list(self):
        _, out, _ = run_doozer([
            "--group=openshift-3.11",
            "--images=aos3-installation",
            "images:list",
            "echo $doozer_image_name",
        ])
        self.assertIn("containers/aos3-installation", out)

    def test_images_clone(self):
        _, out, _ = run_doozer([
            "--group=openshift-3.11",
            "--images=aos3-installation",
            "images:clone",
        ])
        working_dir = get_working_dir()
        repo_path = os.path.join(working_dir, "distgits", "containers", "aos3-installation")
        repo = pygit2.Repository(repo_path)
        ref = repo.references["HEAD"].resolve()  # type: pygit2.Reference
        self.assertEqual("rhaos-3.11-rhel-7", ref.shorthand)
        self.assertTrue(os.path.isfile(os.path.join(repo_path, "Dockerfile")))

    def test_images_pull(self):
        _, out, _ = run_doozer([
            "--group=openshift-3.11",
            "--images=aos3-installation",
            "images:pull",
        ])

    def test_images_show_tree(self):
        _, out, _ = run_doozer([
            "--group=openshift-3.11",
            "--images=grafana,openshift-enterprise-cli,openshift-enterprise-base",
            "images:show-tree",
            "--yml"
        ])
        tree = yaml.safe_load(out)
        self.assertIn("grafana", tree["openshift-enterprise-base"])
        self.assertIn("openshift-enterprise-cli", tree["openshift-enterprise-base"])

    def test_images_push(self):
        _, _, err = run_doozer([
            "--group=openshift-3.11",
            "--images=aos3-installation",
            "images:push",
            "--to-defaults",
            "--dry-run",
        ])
        self.assertIn("Would have tagged", err)

    def test_images_rebase(self):
        _, out, _ = run_doozer([
            "--group=openshift-3.11",
            "--images=aos3-installation",
            "images:rebase",
            "--message=test rebase"
        ])
        working_dir = get_working_dir()
        repo_path = os.path.join(working_dir, "distgits", "containers", "aos3-installation")
        repo = pygit2.Repository(repo_path)
        ref = repo.references["HEAD"].resolve()  # type: pygit2.Reference
        git_commit = ref.peel(pygit2.Commit)  # type: pygit2.Commit
        self.assertIn("test rebase", git_commit.message)

    def test_images_revert(self):
        _, out, _ = run_doozer([
            "--group=openshift-3.11",
            "--images=aos3-installation",
            "images:revert",
            "--message=test revert",
            "1",
        ])
        working_dir = get_working_dir()
        repo_path = os.path.join(working_dir, "distgits", "containers", "aos3-installation")
        repo = pygit2.Repository(repo_path)
        ref = repo.references["HEAD"].resolve()  # type: pygit2.Reference
        git_commit = ref.peel(pygit2.Commit)  # type: pygit2.Commit
        self.assertIn("test revert", git_commit.message)

    def test_images_push_distgit(self):
        # FIXME: images:push-distgit is not tested because we don't have an option to dry run
        pass

    def test_images_print_config_template(self):
        # FIXME: images:print-config-template is not tested because it is broken
        pass

    def test_images_query_rpm_version(self):
        # FIXME: images:query-rpm-version is not tested because it is broken
        pass
