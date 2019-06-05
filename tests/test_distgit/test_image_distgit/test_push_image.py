import errno
import os
import unittest

import mock

import distgit


class TestImageDistGitRepoPushImage(unittest.TestCase):

    def test_push_image_is_late_push(self):
        metadata = mock.Mock()
        metadata.config.push.late = True
        metadata.distgit_key = "distgit_key"

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)

        expected = ("distgit_key", True)
        actual = repo.push_image([], "push_to_defaults")
        self.assertEqual(expected, actual)

    def test_push_image_nothing_to_push(self):
        metadata = mock.Mock()
        metadata.config.push.late = distgit.Missing
        metadata.distgit_key = "distgit_key"
        metadata.get_default_push_names.return_value = []
        metadata.get_additional_push_names.return_value = []

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        push_to_defaults = False

        expected = ("distgit_key", True)
        actual = repo.push_image([], push_to_defaults)
        self.assertEqual(expected, actual)

    @mock.patch("distgit.Dir")
    @mock.patch("distgit.os.mkdir", return_value=None)
    @mock.patch("distgit.os.path.isdir", return_value=True)
    @mock.patch("distgit.os.path.isfile", return_value=True)
    @mock.patch("distgit.os.remove", return_value=None)
    @mock.patch("distgit.exectools.cmd_gather", return_value=(0, "", ""))
    @mock.patch("__builtin__.open")
    def test_push_image_to_defaults(self, open_mock, *_):
        open_mock.write.return_value = None
        metadata = mock.Mock()
        metadata.config.push.late = distgit.Missing
        metadata.get_default_push_names.return_value = ["my-default-name"]
        metadata.get_additional_push_names.return_value = []
        metadata.distgit_key = "my-distgit-key"
        metadata.config.name = "my-name"
        metadata.config.namespace = "my-namespace"
        metadata.runtime.group_config.urls.brew_image_host = "brew-img-host"
        metadata.runtime.group_config.insecure_source = False
        metadata.runtime.working_dir = "my-working-dir"

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        tag_list = ["tag-a", "tag-b"]
        push_to_defaults = True

        expected = ("my-distgit-key", True)
        actual = repo.push_image(tag_list,
                                 push_to_defaults,
                                 version_release_tuple=("version", "release"))
        self.assertEqual(expected, actual)

    @mock.patch("distgit.Dir")
    @mock.patch("distgit.os.mkdir", return_value=None)
    @mock.patch("distgit.os.path.isdir", return_value=True)
    @mock.patch("distgit.os.path.isfile", return_value=True)
    @mock.patch("distgit.os.remove", return_value=None)
    @mock.patch("distgit.exectools.cmd_gather", return_value=(0, "", ""))
    @mock.patch("__builtin__.open")
    def test_push_image_without_version_release_tuple(self, open_mock, *_):
        open_mock.write.return_value = None
        metadata = mock.Mock()
        metadata.config.push.late = distgit.Missing
        metadata.get_default_push_names.return_value = ["my-default-name"]
        metadata.get_additional_push_names.return_value = []
        metadata.distgit_key = "my-distgit-key"
        metadata.config.name = "my-name"
        metadata.config.namespace = "my-namespace"
        metadata.get_latest_build_info.return_value = ("_", "my-version", "my-release")
        metadata.runtime.group_config.urls.brew_image_host = "brew-img-host"
        metadata.runtime.group_config.insecure_source = False
        metadata.runtime.working_dir = "my-working-dir"

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        tag_list = ["tag-a", "tag-b"]
        push_to_defaults = True

        expected = ("my-distgit-key", True)
        actual = repo.push_image(tag_list,
                                 push_to_defaults)
        self.assertEqual(expected, actual)

    @mock.patch("distgit.Dir")
    @mock.patch("distgit.os.mkdir", return_value=None)
    @mock.patch("distgit.os.path.isdir", return_value=True)
    @mock.patch("distgit.os.path.isfile", return_value=True)
    @mock.patch("distgit.os.remove", return_value=None)
    @mock.patch("distgit.exectools.cmd_gather", return_value=(0, "", ""))
    @mock.patch("__builtin__.open")
    def test_push_image_no_push_tags(self, open_mock, *_):
        open_mock.write.return_value = None
        metadata = mock.Mock()
        metadata.config.push.late = distgit.Missing
        metadata.get_default_push_names.return_value = ["my/default-name"]
        metadata.get_default_push_tags.return_value = []
        metadata.get_additional_push_names.return_value = []
        metadata.distgit_key = "my-distgit-key"
        metadata.config.name = "my-name"
        metadata.config.namespace = "my-namespace"
        metadata.runtime.group_config.urls.brew_image_host = "brew-img-host"
        metadata.runtime.group_config.insecure_source = False
        metadata.runtime.working_dir = "my-working-dir"

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        tag_list = []
        push_to_defaults = True

        expected = ("my-distgit-key", True)
        actual = repo.push_image(tag_list,
                                 push_to_defaults,
                                 version_release_tuple=("version", "release"),
                                 dry_run=True)
        self.assertEqual(expected, actual)

    @mock.patch("distgit.Dir")
    @mock.patch("distgit.os.mkdir", return_value=None)
    @mock.patch("distgit.os.path.isdir", return_value=True)
    @mock.patch("distgit.os.path.isfile", return_value=True)
    @mock.patch("distgit.os.remove", return_value=None)
    @mock.patch("distgit.exectools.cmd_gather", return_value=(0, "", ""))
    @mock.patch("__builtin__.open")
    def test_push_image_dry_run(self, open_mock, *_):
        open_mock.write.return_value = None
        metadata = mock.Mock()
        metadata.config.push.late = distgit.Missing
        metadata.get_default_push_names.return_value = ["my-default-name"]
        metadata.get_additional_push_names.return_value = []
        metadata.distgit_key = "my-distgit-key"
        metadata.config.name = "my-name"
        metadata.config.namespace = "my-namespace"
        metadata.runtime.group_config.urls.brew_image_host = "brew-img-host"
        metadata.runtime.group_config.insecure_source = False
        metadata.runtime.working_dir = "my-working-dir"

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        tag_list = ["tag-a", "tag-b"]
        push_to_defaults = True

        expected = ("my-distgit-key", True)
        actual = repo.push_image(tag_list,
                                 push_to_defaults,
                                 version_release_tuple=("version", "release"),
                                 dry_run=True)
        self.assertEqual(expected, actual)

    @mock.patch("distgit.Dir")
    @mock.patch("distgit.os.mkdir", return_value=None)
    @mock.patch("distgit.os.path.isdir", return_value=False)
    @mock.patch("distgit.os.path.isfile", return_value=True)
    @mock.patch("distgit.os.remove", return_value=None)
    @mock.patch("distgit.exectools.cmd_gather", return_value=(0, "", ""))
    @mock.patch("__builtin__.open")
    def test_push_image_without_a_push_config_dir_previously_present(self, open_mock, *_):
        open_mock.write.return_value = None
        metadata = mock.Mock()
        metadata.config.push.late = distgit.Missing
        metadata.get_default_push_names.return_value = ["my-default-name"]
        metadata.get_additional_push_names.return_value = []
        metadata.distgit_key = "my-distgit-key"
        metadata.config.name = "my-name"
        metadata.config.namespace = "my-namespace"
        metadata.runtime.group_config.urls.brew_image_host = "brew-img-host"
        metadata.runtime.group_config.insecure_source = False
        metadata.runtime.working_dir = "my-working-dir"

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        tag_list = ["tag-a", "tag-b"]
        push_to_defaults = True

        expected = ("my-distgit-key", True)
        actual = repo.push_image(tag_list,
                                 push_to_defaults,
                                 version_release_tuple=("version", "release"))
        self.assertEqual(expected, actual)

    @mock.patch("distgit.Dir")
    @mock.patch("distgit.os.mkdir", side_effect=OSError)
    @mock.patch("distgit.os.path.isdir", return_value=False)
    @mock.patch("distgit.os.path.isfile", return_value=True)
    @mock.patch("distgit.os.remove", return_value=None)
    @mock.patch("distgit.exectools.cmd_gather", return_value=(0, "", ""))
    @mock.patch("__builtin__.open")
    def test_push_image_fail_to_create_a_push_config_dir(self, open_mock, *_):
        open_mock.write.return_value = None
        metadata = mock.Mock()
        metadata.config.push.late = distgit.Missing
        metadata.get_default_push_names.return_value = ["my-default-name"]
        metadata.get_additional_push_names.return_value = []
        metadata.distgit_key = "my-distgit-key"
        metadata.config.name = "my-name"
        metadata.config.namespace = "my-namespace"
        metadata.runtime.group_config.urls.brew_image_host = "brew-img-host"
        metadata.runtime.group_config.insecure_source = False
        metadata.runtime.working_dir = "my-working-dir"

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        tag_list = ["tag-a", "tag-b"]
        push_to_defaults = True
        version_release_tuple = ("version", "release")

        self.assertRaises(OSError,
                          repo.push_image,
                          tag_list,
                          push_to_defaults,
                          version_release_tuple=version_release_tuple)

    @mock.patch("distgit.Dir")
    @mock.patch("distgit.os.mkdir", side_effect=OSError(errno.EEXIST, os.strerror(errno.EEXIST)))
    @mock.patch("distgit.os.path.isdir", side_effect=[False, True])
    @mock.patch("distgit.os.path.isfile", return_value=False)
    @mock.patch("distgit.exectools.cmd_gather", return_value=(0, "", ""))
    @mock.patch("__builtin__.open")
    def test_push_image_push_config_dir_already_created_by_another_thread(self, open_mock, *_):
        open_mock.write.return_value = None
        metadata = mock.Mock()
        metadata.config.push.late = distgit.Missing
        metadata.get_default_push_names.return_value = ["my-default-name"]
        metadata.get_additional_push_names.return_value = []
        metadata.distgit_key = "my-distgit-key"
        metadata.config.name = "my-name"
        metadata.config.namespace = "my-namespace"
        metadata.runtime.group_config.urls.brew_image_host = "brew-img-host"
        metadata.runtime.group_config.insecure_source = False
        metadata.runtime.working_dir = "my-working-dir"

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        tag_list = ["tag-a", "tag-b"]
        push_to_defaults = True

        expected = ("my-distgit-key", True)
        actual = repo.push_image(tag_list,
                                 push_to_defaults,
                                 version_release_tuple=("version", "release"))
        self.assertEqual(expected, actual)

    @mock.patch("distgit.Dir")
    @mock.patch("distgit.os.mkdir", return_value=None)
    @mock.patch("distgit.os.path.isdir", return_value=True)
    @mock.patch("distgit.os.path.isfile", return_value=True)
    @mock.patch("distgit.os.remove", return_value=None)
    @mock.patch("distgit.time.sleep", return_value=None)
    @mock.patch("distgit.exectools.cmd_gather", return_value=(1, "", "stderr"))
    @mock.patch("__builtin__.open")
    def test_push_image_to_defaults_fail_mirroring(self, open_mock, *_):
        open_mock.write.return_value = None
        metadata = mock.Mock()
        metadata.config.push.late = distgit.Missing
        metadata.get_default_push_names.return_value = ["my-default-name"]
        metadata.get_additional_push_names.return_value = []
        metadata.distgit_key = "my-distgit-key"
        metadata.config.name = "my-name"
        metadata.config.namespace = "my-namespace"
        metadata.runtime.group_config.urls.brew_image_host = "brew-img-host"
        metadata.runtime.group_config.insecure_source = False
        metadata.runtime.working_dir = "my-working-dir"

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        tag_list = ["tag-a", "tag-b"]
        push_to_defaults = True
        version_release_tuple = ("version", "release")

        try:
            repo.push_image(tag_list,
                            push_to_defaults,
                            version_release_tuple=version_release_tuple)
            self.fail("Should have raised an exception, but didn't")
        except IOError as e:
            expected_msg = "Error pushing image: stderr"
            self.assertEqual(expected_msg, e.message)

    @mock.patch("distgit.Dir")
    @mock.patch("distgit.os.mkdir", return_value=None)
    @mock.patch("distgit.os.path.isdir", return_value=True)
    @mock.patch("distgit.os.path.isfile", return_value=True)
    @mock.patch("distgit.os.remove", return_value=None)
    @mock.patch("distgit.exectools.cmd_gather", return_value=(0, "", ""))
    @mock.patch("__builtin__.open")
    @mock.patch("doozerlib.state.record_image_success", return_value=None)
    def test_push_image_to_defaults_with_lstate(self, record_image_success_mock, open_mock, *_):
        open_mock.write.return_value = None
        metadata = mock.Mock()
        metadata.config.push.late = distgit.Missing
        metadata.get_default_push_names.return_value = ["my-default-name"]
        metadata.get_additional_push_names.return_value = []
        metadata.distgit_key = "my-distgit-key"
        metadata.config.name = "my-name"
        metadata.config.namespace = "my-namespace"
        metadata.runtime.group_config.urls.brew_image_host = "brew-img-host"
        metadata.runtime.group_config.insecure_source = False
        metadata.runtime.working_dir = "my-working-dir"
        metadata.runtime.state = {"images:push": "my-runtime-state"}
        metadata.runtime.command = "images:push"

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        tag_list = ["tag-a", "tag-b"]
        push_to_defaults = True

        expected = ("my-distgit-key", True)
        actual = repo.push_image(tag_list,
                                 push_to_defaults,
                                 version_release_tuple=("version", "release"))
        self.assertEqual(expected, actual)
        record_image_success_mock.assert_called_once_with("my-runtime-state", metadata)

    @mock.patch("distgit.Dir")
    @mock.patch("distgit.os.mkdir", return_value=None)
    @mock.patch("distgit.os.path.isdir", return_value=True)
    @mock.patch("distgit.os.path.isfile", return_value=True)
    @mock.patch("distgit.os.remove", return_value=None)
    @mock.patch("distgit.time.sleep", return_value=None)
    @mock.patch("distgit.exectools.cmd_gather", return_value=(1, "", "stderr"))
    @mock.patch("__builtin__.open")
    @mock.patch("doozerlib.state.record_image_fail", return_value=None)
    def test_push_image_to_defaults_fail_mirroring_with_lstate(self, record_image_fail_mock, open_mock, *_):
        open_mock.write.return_value = None
        metadata = mock.Mock()
        metadata.config.push.late = distgit.Missing
        metadata.get_default_push_names.return_value = ["my-default-name"]
        metadata.get_additional_push_names.return_value = []
        metadata.distgit_key = "my-distgit-key"
        metadata.config.name = "my-name"
        metadata.config.namespace = "my-namespace"
        metadata.runtime.group_config.urls.brew_image_host = "brew-img-host"
        metadata.runtime.group_config.insecure_source = False
        metadata.runtime.working_dir = "my-working-dir"
        metadata.runtime.state = {"images:push": "my-runtime-state"}
        metadata.runtime.command = "images:push"

        logger = mock.Mock()
        metadata.runtime.logger = logger

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        tag_list = ["tag-a", "tag-b"]
        push_to_defaults = True
        version_release_tuple = ("version", "release")

        try:
            repo.push_image(tag_list,
                            push_to_defaults,
                            version_release_tuple=version_release_tuple)
            self.fail("Should have raised an exception, but didn't")
        except IOError as e:
            expected_msg = "Error pushing image: stderr"
            self.assertEqual(expected_msg, e.message)

            expected_record_fail_calls = [
                mock.call("my-runtime-state", metadata, "Build failure", logger),
                mock.call("my-runtime-state", metadata, expected_msg, logger),
            ]
            self.assertEqual(expected_record_fail_calls, record_image_fail_mock.mock_calls)

    @mock.patch("distgit.Dir")
    @mock.patch("distgit.os.mkdir", return_value=None)
    @mock.patch("distgit.os.path.isdir", return_value=True)
    @mock.patch("distgit.os.path.isfile", return_value=True)
    @mock.patch("distgit.os.remove", return_value=None)
    @mock.patch("distgit.exectools.cmd_gather", return_value=(0, "", ""))
    @mock.patch("__builtin__.open")
    def test_push_image_insecure_source(self, open_mock, cmd_gather_mock, *_):
        open_mock.write.return_value = None
        metadata = mock.Mock()
        metadata.config.push.late = distgit.Missing
        metadata.get_default_push_names.return_value = ["my-default-name"]
        metadata.get_additional_push_names.return_value = []
        metadata.distgit_key = "my-distgit-key"
        metadata.config.name = "my-name"
        metadata.config.namespace = "my-namespace"
        metadata.runtime.group_config.urls.brew_image_host = "brew-img-host"
        metadata.runtime.group_config.insecure_source = True
        metadata.runtime.working_dir = "my-working-dir"

        repo = distgit.ImageDistGitRepo(metadata, autoclone=False)
        tag_list = ["tag-a", "tag-b"]
        push_to_defaults = True

        expected = ("my-distgit-key", True)
        actual = repo.push_image(tag_list,
                                 push_to_defaults,
                                 version_release_tuple=("version", "release"))
        self.assertEqual(expected, actual)

        expected_cmd = "oc image mirror  --insecure=true --filename=my-working-dir/push/my-distgit-key"
        cmd_gather_mock.assert_called_once_with(expected_cmd)
