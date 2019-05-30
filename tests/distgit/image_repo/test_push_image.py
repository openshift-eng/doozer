from unittest import TestCase
from mock import patch, Mock, call, mock_open
from distgit import ImageDistGitRepo


class TestDistGitImageRepoPushImage(TestCase):

    @patch("distgit.Dir")
    @patch("distgit.os.mkdir")
    @patch("distgit.exectools.cmd_gather", return_value=(0, "stdout", "stderr"))
    def test_generation_of_oc_mirror_commands(self, mocked_cmd_gather, *_):
        repo = ImageDistGitRepo(Mock(**{
            "config.push.late": False,
            "runtime.group_config.insecure_source": False,

            "get_latest_build_info.return_value": ("name", "version", "release"),
            "get_default_push_names.return_value": [
                "registry.reg-aws.openshift.com:443/openshift/logging-elasticsearch5",
                "registry.reg-aws.openshift.com:443/openshift/ose-logging-elasticsearch5",
            ],
            "get_additional_push_names.return_value": [],
            "runtime.group_config.urls.brew_image_host": "brew-pulp-docker01.web.prod.ext.phx2.redhat.com:8888",
            "get_default_push_tags.return_value": [
                "v4.1.0-201905191700",
                "v4.1.0",
            ],
            "runtime.working_dir": "/my/working/dir",
            "distgit_key": "logging-elasticsearch5", # is it correct ?
            "config.name": "origin-aggregated-logging",
        }), autoclone=False)

        mocked_bultin_open = mock_open()
        with patch("distgit.open", mocked_bultin_open):
            expected_return = ("logging-elasticsearch5", True)
            actual_return = repo.push_image([], True)

        self.assertEqual(expected_return, actual_return)

        # oc image mirror command should be called twice (why?)
        expected_cmd_calls = [
            call("oc image mirror   --filename=/my/working/dir/push/logging-elasticsearch5"),
            call("oc image mirror   --filename=/my/working/dir/push/logging-elasticsearch5"),
        ]
        self.assertEqual(expected_cmd_calls, mocked_cmd_gather.mock_calls)

        # why?
        expected_push_config_contents_first_run = """
brew-pulp-docker01.web.prod.ext.phx2.redhat.com:8888/origin-aggregated-logging:version-release=registry.reg-aws.openshift.com:443/openshift/logging-elasticsearch5:v4.1.0-201905191700
brew-pulp-docker01.web.prod.ext.phx2.redhat.com:8888/origin-aggregated-logging:version-release=registry.reg-aws.openshift.com:443/openshift/logging-elasticsearch5:v4.1.0
"""
        expected_push_config_contents_second_run = """
brew-pulp-docker01.web.prod.ext.phx2.redhat.com:8888/origin-aggregated-logging:version-release=registry.reg-aws.openshift.com:443/openshift/logging-elasticsearch5:v4.1.0-201905191700
brew-pulp-docker01.web.prod.ext.phx2.redhat.com:8888/origin-aggregated-logging:version-release=registry.reg-aws.openshift.com:443/openshift/logging-elasticsearch5:v4.1.0
brew-pulp-docker01.web.prod.ext.phx2.redhat.com:8888/origin-aggregated-logging:version-release=registry.reg-aws.openshift.com:443/openshift/ose-logging-elasticsearch5:v4.1.0-201905191700
brew-pulp-docker01.web.prod.ext.phx2.redhat.com:8888/origin-aggregated-logging:version-release=registry.reg-aws.openshift.com:443/openshift/ose-logging-elasticsearch5:v4.1.0
"""
        expected_write_commands = [
            call(expected_push_config_contents_first_run.strip()),
            call(expected_push_config_contents_second_run.strip()),
        ]

        self.assertEqual(expected_write_commands, mocked_bultin_open().write.mock_calls)

    @patch("distgit.Dir")
    @patch("distgit.os.mkdir")
    @patch("distgit.open")
    @patch("distgit.exectools.cmd_gather", return_value=(0, "stdout", "stderr"))
    def test_insecure_oc_mirror_command(self, mocked_cmd_gather, *_):
        repo = ImageDistGitRepo(Mock(**{
            "config.push.late": False,
            "runtime.group_config.insecure_source": True,

            "get_latest_build_info.return_value": ("name", "version", "release"),
            "get_default_push_names.return_value": [
                "registry.reg-aws.openshift.com:443/openshift/logging-elasticsearch5",
                "registry.reg-aws.openshift.com:443/openshift/ose-logging-elasticsearch5",
            ],
            "get_additional_push_names.return_value": [],
            "runtime.group_config.urls.brew_image_host": "brew-pulp-docker01.web.prod.ext.phx2.redhat.com:8888",
            "get_default_push_tags.return_value": [
                "v4.1.0-201905191700",
                "v4.1.0",
            ],
            "runtime.working_dir": "/my/working/dir",
            "distgit_key": "logging-elasticsearch5", # is it correct ?
            "config.name": "origin-aggregated-logging",
        }), autoclone=False)

        repo.push_image([], True)

        expected_cmd = "oc image mirror  --insecure=true --filename=/my/working/dir/push/logging-elasticsearch5"
        mocked_cmd_gather.assert_called_with(expected_cmd)

    @patch("distgit.Dir")
    @patch("distgit.os.mkdir")
    @patch("distgit.open")
    @patch("distgit.exectools.cmd_gather", return_value=(0, "stdout", "stderr"))
    def test_dry_run_oc_mirror_command(self, mocked_cmd_gather, *_):
        repo = ImageDistGitRepo(Mock(**{
            "config.push.late": False,
            "runtime.group_config.insecure_source": False,

            "get_latest_build_info.return_value": ("name", "version", "release"),
            "get_default_push_names.return_value": [
                "registry.reg-aws.openshift.com:443/openshift/logging-elasticsearch5",
                "registry.reg-aws.openshift.com:443/openshift/ose-logging-elasticsearch5",
            ],
            "get_additional_push_names.return_value": [],
            "runtime.group_config.urls.brew_image_host": "brew-pulp-docker01.web.prod.ext.phx2.redhat.com:8888",
            "get_default_push_tags.return_value": [
                "v4.1.0-201905191700",
                "v4.1.0",
            ],
            "runtime.working_dir": "/my/working/dir",
            "distgit_key": "logging-elasticsearch5", # is it correct ?
            "config.name": "origin-aggregated-logging",
        }), autoclone=False)

        repo.push_image([], True, dry_run=True)
        mocked_cmd_gather.assert_not_called()
