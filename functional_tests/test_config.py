import unittest
import yaml
from . import run_doozer, get_working_dir
import tempfile
import os
import pygit2


class TestConfig(unittest.TestCase):
    def test_config_get(self):
        _, out, _ = run_doozer([
            "--group=openshift-3.11",
            "config:get"
        ])

    def test_config_print(self):
        _, out, _ = run_doozer([
            "--group=openshift-3.11",
            "--images=aos3-installation",
            "config:print",
            "--yaml"
        ])
        self.assertIn("aos3-installation", yaml.safe_load(out)["images"])

    def test_config_gen_csv(self):
        _, out, _ = run_doozer([
            "--group=openshift-3.11",
            "--images=aos3-installation",
            "config:gen-csv",
            "--type=image",
            "--keys=key,name"
        ])
        self.assertIn("aos3-installation.yml,openshift3/ose-ansible", out)

    def test_config_read_group(self):
        _, out, _ = run_doozer([
            "--group=openshift-3.11",
            "config:read-group",
            "--default=[]",
            "--yaml",
            "non_release.images"
        ])
        self.assertIn("openshift-enterprise-base", yaml.safe_load(out))

    def test_config_scan_sources(self):
        _, out, _ = run_doozer([
            "--group=openshift-3.11",
            "--images=aos3-installation",
            "config:scan-sources",
            "--yaml",
        ])
        self.assertEqual("aos3-installation", yaml.safe_load(out)['images'][0]["name"])

    def test_config_update_required(self):
        with tempfile.NamedTemporaryFile(prefix="doozertest-imagelist-") as f:
            f.write(b"ose-ansible\n")
            f.flush()
            _, out, _ = run_doozer([
                "--group=openshift-3.11",
                "config:update-required",
                "--image-list=" + f.name,
            ])
        self.assertIn("Updating aos3-installation to be required", out)

    def test_config_commit(self):
        # ensure ocp-build-data is cloned
        _, out, _ = run_doozer([
            "--group=openshift-3.11",
            "config:get"
        ])
        # configure user.name and user.email for ocp-build-data commit
        ocp_build_data_dir = os.path.join(get_working_dir(), "ocp-build-data")
        repo = pygit2.Repository(ocp_build_data_dir)
        pygit2.Config()
        repo_conf = repo.config  # type: pygit2.Config
        repo_conf["user.name"] = "Doozer Test User"
        repo_conf["user.email"] = "doozer-test@example.com"

        _, out, _ = run_doozer([
            "--group=openshift-3.11",
            "config:commit",
            "--message=Doozer test commit message",
            "--no-push",
        ])

        # Assert commit message
        commit = repo.head.peel(pygit2.Commit)  # type: pygit2.Commit
        self.assertEqual(commit.message.rstrip(), "Doozer test commit message")

    def test_config_update_mode(self):
        _, _, err = run_doozer([
            "--group=openshift-3.11",
            "--images=aos3-installation",
            "config:update-mode",
            "disabled",
        ])
        self.assertIn("aos3-installation.yml: [mode] -> disabled", err)
