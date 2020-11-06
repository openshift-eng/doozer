import unittest
from unittest import mock

import koji

from doozerlib import brew


class TestBrew(unittest.TestCase):
    def test_get_build_objects(self):
        build_infos = {
            "logging-fluentd-container-v3.11.141-2": {"cg_id": None, "package_name": "logging-fluentd-container", "extra": {"submitter": "osbs", "image": {"media_types": ["application/vnd.docker.distribution.manifest.list.v2+json", "application/vnd.docker.distribution.manifest.v1+json", "application/vnd.docker.distribution.manifest.v2+json"], "help": None, "index": {"pull": ["brew-pulp-docker01.web.prod.ext.phx2.redhat.com:8888/openshift3/ose-logging-fluentd@sha256:1df5eacdd98923590afdc85330aaac0488de96e991b24a7f4cb60113b7a66e80", "brew-pulp-docker01.web.prod.ext.phx2.redhat.com:8888/openshift3/ose-logging-fluentd:v3.11.141-2"], "digests": {"application/vnd.docker.distribution.manifest.list.v2+json": "sha256:1df5eacdd98923590afdc85330aaac0488de96e991b24a7f4cb60113b7a66e80"}, "tags": ["v3.11.141-2"]}, "autorebuild": False, "isolated": False, "yum_repourls": ["http://pkgs.devel.redhat.com/cgit/containers/logging-fluentd/plain/.oit/signed.repo?h=rhaos-3.11-rhel-7"], "parent_build_id": 955726, "parent_images": ["openshift/ose-base:rhel7"], "parent_image_builds": {"openshift/ose-base:rhel7": {"id": 955726, "nvr": "openshift-enterprise-base-container-v4.0-201908250221"}}}, "container_koji_task_id": 23188768}, "creation_time": "2019-08-26 07:34:32.613833", "completion_time": "2019-08-26 07:34:31", "package_id": 67151, "cg_name": None, "id": 956245, "build_id": 956245, "epoch": None, "source": "git://pkgs.devel.redhat.com/containers/logging-fluentd#7f4bcdc798fd72414a29dc1010c448e1ed52f591", "state": 1, "version": "v3.11.141", "completion_ts": 1566804871.0, "owner_id": 4078, "owner_name": "ocp-build/buildvm.openshift.eng.bos.redhat.com", "nvr": "logging-fluentd-container-v3.11.141-2", "start_time": "2019-08-26 07:03:41", "creation_event_id": 26029088, "start_ts": 1566803021.0, "volume_id": 0, "creation_ts": 1566804872.61383, "name": "logging-fluentd-container", "task_id": None, "volume_name": "DEFAULT", "release": "2"},
            "logging-fluentd-container-v4.1.14-201908291507": {"cg_id": None, "package_name": "logging-fluentd-container", "extra": {"submitter": "osbs", "image": {"media_types": ["application/vnd.docker.distribution.manifest.list.v2+json", "application/vnd.docker.distribution.manifest.v1+json", "application/vnd.docker.distribution.manifest.v2+json"], "help": None, "index": {"unique_tags": ["rhaos-4.1-rhel-7-containers-candidate-94076-20190829211225"], "pull": ["brew-pulp-docker01.web.prod.ext.phx2.redhat.com:8888/openshift/ose-logging-fluentd@sha256:7503f828aaf80e04b2aaab0b88626b97a20e5600ba75fef8b764e02cc1164a7c", "brew-pulp-docker01.web.prod.ext.phx2.redhat.com:8888/openshift/ose-logging-fluentd:v4.1.14-201908291507"], "floating_tags": ["latest", "v4.1.14", "v4.1.14.20190829.150756", "v4.1"], "digests": {"application/vnd.docker.distribution.manifest.list.v2+json": "sha256:7503f828aaf80e04b2aaab0b88626b97a20e5600ba75fef8b764e02cc1164a7c"}, "tags": ["v4.1.14-201908291507"]}, "autorebuild": False, "isolated": False, "yum_repourls": ["http://pkgs.devel.redhat.com/cgit/containers/logging-fluentd/plain/.oit/signed.repo?h=rhaos-4.1-rhel-7"], "parent_build_id": 958278, "parent_images": ["rhscl/ruby-25-rhel7:latest", "openshift/ose-base:ubi7"], "parent_image_builds": {"openshift/ose-base:ubi7": {"id": 958278, "nvr": "openshift-enterprise-base-container-v4.0-201908290538"}, "rhscl/ruby-25-rhel7:latest": {"id": 957642, "nvr": "rh-ruby25-container-2.5-50"}}}, "container_koji_task_id": 23241046}, "creation_time": "2019-08-29 21:42:46.062037", "completion_time": "2019-08-29 21:42:44", "package_id": 67151, "cg_name": None, "id": 958765, "build_id": 958765, "epoch": None, "source": "git://pkgs.devel.redhat.com/containers/logging-fluentd#ecac10b38f035ea2f9ea62b9efa63c051667ebbb", "state": 1, "version": "v4.1.14", "completion_ts": 1567114964.0, "owner_id": 4078, "owner_name": "ocp-build/buildvm.openshift.eng.bos.redhat.com", "nvr": "logging-fluentd-container-v4.1.14-201908291507", "start_time": "2019-08-29 21:12:51", "creation_event_id": 26063093, "start_ts": 1567113171.0, "volume_id": 0, "creation_ts": 1567114966.06204, "name": "logging-fluentd-container", "task_id": None, "volume_name": "DEFAULT", "release": "201908291507"},
            "logging-fluentd-container-v4.1.15-201909041605": {"cg_id": None, "package_name": "logging-fluentd-container", "extra": {"submitter": "osbs", "image": {"media_types": ["application/vnd.docker.distribution.manifest.list.v2+json", "application/vnd.docker.distribution.manifest.v1+json", "application/vnd.docker.distribution.manifest.v2+json"], "help": None, "index": {"unique_tags": ["rhaos-4.1-rhel-7-containers-candidate-96970-20190904214308"], "pull": ["brew-pulp-docker01.web.prod.ext.phx2.redhat.com:8888/openshift/ose-logging-fluentd@sha256:1ce1555b58982a29354c293948ee6c788743a08f39a0c530be791cb9bdaf4189", "brew-pulp-docker01.web.prod.ext.phx2.redhat.com:8888/openshift/ose-logging-fluentd:v4.1.15-201909041605"], "floating_tags": ["latest", "v4.1.15", "v4.1", "v4.1.15.20190904.160545"], "digests": {"application/vnd.docker.distribution.manifest.list.v2+json": "sha256:1ce1555b58982a29354c293948ee6c788743a08f39a0c530be791cb9bdaf4189"}, "tags": ["v4.1.15-201909041605"]}, "autorebuild": False, "isolated": False, "yum_repourls": ["http://pkgs.devel.redhat.com/cgit/containers/logging-fluentd/plain/.oit/signed.repo?h=rhaos-4.1-rhel-7"], "parent_build_id": 961131, "parent_images": ["rhscl/ruby-25-rhel7:latest", "openshift/ose-base:ubi7"], "parent_image_builds": {"openshift/ose-base:ubi7": {"id": 961131, "nvr": "openshift-enterprise-base-container-v4.0-201909040323"}, "rhscl/ruby-25-rhel7:latest": {"id": 957642, "nvr": "rh-ruby25-container-2.5-50"}}}, "container_koji_task_id": 23365465}, "creation_time": "2019-09-04 22:17:36.432110", "completion_time": "2019-09-04 22:17:35", "package_id": 67151, "cg_name": None, "id": 962144, "build_id": 962144, "epoch": None, "source": "git://pkgs.devel.redhat.com/containers/logging-fluentd#31cf3d4264dabb8892fb4b5921e5ff4d5d0ab2de", "state": 1, "version": "v4.1.15", "completion_ts": 1567635455.0, "owner_id": 4078, "owner_name": "ocp-build/buildvm.openshift.eng.bos.redhat.com", "nvr": "logging-fluentd-container-v4.1.15-201909041605", "start_time": "2019-09-04 21:43:32", "creation_event_id": 26176078, "start_ts": 1567633412.0, "volume_id": 0, "creation_ts": 1567635456.43211, "name": "logging-fluentd-container", "task_id": None, "volume_name": "DEFAULT", "release": "201909041605"},
        }

        def fake_get_build(nvr):
            return mock.MagicMock(result=build_infos[nvr])

        fake_session = mock.MagicMock()
        fake_context_manager = fake_session.multicall.return_value.__enter__.return_value
        fake_context_manager.getBuild.side_effect = fake_get_build
        expected = list(build_infos.values())
        actual = brew.get_build_objects(build_infos.keys(), fake_session)
        self.assertListEqual(actual, expected)

    def test_get_latest_builds(self):
        tag_component_tuples = [
            ("faketag1", "component1"),
            ("faketag2", "component2"),
            ("faketag2", None),
            ("faketag1", "component4"),
            ("", "component5"),
            ("faketag2", "component6"),
        ]
        expected = [
            [{"name": "component1", "nvr": "component1-v1.0.0-1.faketag1"}],
            [{"name": "component2", "nvr": "component2-v1.0.0-1.faketag2"}],
            [{"name": "a", "nvr": "a-v1.0.0-1.faketag2"}, {"name": "b", "nvr": "b-v1.0.0-1.faketag2"}],
            [{"name": "component4", "nvr": "component4-v1.0.0-1.faketag1"}],
            None,
            [{"name": "component6", "nvr": "component6-v1.0.0-1.faketag2"}],
        ]

        def fake_response(tag, event=None, package=None, type=None):
            packages = [package] if package else ["a", "b"]
            return mock.MagicMock(result=[{"name": pkg, "nvr": f"{pkg}-v1.0.0-1.{tag}"} for pkg in packages])

        fake_session = mock.MagicMock()
        fake_context_manager = fake_session.multicall.return_value.__enter__.return_value
        fake_context_manager.getLatestBuilds.side_effect = fake_response
        actual = brew.get_latest_builds(tag_component_tuples, None, None, fake_session)
        self.assertListEqual(actual, expected)

    def test_list_archives_by_builds(self):
        build_ids = [1, 2, 3, None, 4, 0, 5, None]
        rpms = [object()]
        expected = [
            [{"build_id": 1, "type_name": "tar", "arch": "x86_64", "btype": "image", "id": 1000000, "rpms": rpms}],
            [{"build_id": 2, "type_name": "tar", "arch": "x86_64", "btype": "image", "id": 2000000, "rpms": rpms}],
            [{"build_id": 3, "type_name": "tar", "arch": "x86_64", "btype": "image", "id": 3000000, "rpms": rpms}],
            None,
            [{"build_id": 4, "type_name": "tar", "arch": "x86_64", "btype": "image", "id": 4000000, "rpms": rpms}],
            None,
            [{"build_id": 5, "type_name": "tar", "arch": "x86_64", "btype": "image", "id": 5000000, "rpms": rpms}],
            None,
        ]

        def fake_archives_response(buildID, type):
            return mock.MagicMock(result=[{"build_id": buildID, "type_name": "tar", "arch": "x86_64", "btype": type, "id": buildID * 1000000}])

        def fake_rpms_response(imageID):
            return mock.MagicMock(result=rpms)

        fake_session = mock.MagicMock()
        fake_context_manager = fake_session.multicall.return_value.__enter__.return_value
        fake_context_manager.listArchives.side_effect = fake_archives_response
        fake_context_manager.listRPMs.side_effect = fake_rpms_response
        actual = brew.list_archives_by_builds(build_ids, "image", fake_session)
        self.assertListEqual(actual, expected)

    @mock.patch("koji_cli.lib.TaskWatcher")
    @mock.patch("koji.ClientSession")
    @mock.patch("time.time", return_value=10000)
    def test_watch_tasks(self, mock_time, MockSession, MockWatcher):
        brew_session = mock.MagicMock()
        log_func = mock.MagicMock()
        tasks = [1, 2, 3]
        terminate_event = mock.MagicMock()

        # all tasks are finished successfully
        MockWatcher.return_value.is_done.return_value = True
        MockWatcher.return_value.is_success.return_value = True
        MockWatcher.return_value.info = {"state": koji.TASK_STATES['CLOSED']}
        errors = brew.watch_tasks(brew_session, log_func, tasks, terminate_event)
        self.assertFalse(any(errors.values()))

        # all tasks fails with "some reason"
        MockWatcher.return_value.is_success.return_value = False
        MockWatcher.return_value.get_failure.return_value = "some reason"
        MockWatcher.return_value.info = {"state": koji.TASK_STATES['FAILED']}
        errors = brew.watch_tasks(brew_session, log_func, tasks, terminate_event)
        self.assertTrue(all(map(lambda failure: failure == "some reason", errors.values())))

        # interrupted
        MockWatcher.return_value.is_done.return_value = False
        MockWatcher.return_value.info = {"state": koji.TASK_STATES['OPEN']}
        terminate_event.wait.return_value = True
        brew_session.cancelTask.return_value = True
        errors = brew.watch_tasks(brew_session, log_func, tasks, terminate_event)
        self.assertTrue(all(map(lambda failure: failure == "Interrupted", errors.values())))
        brew_session.cancelTask.assert_has_calls([mock.call(task, recurse=True) for task in tasks], any_order=True)

        # timed out
        terminate_event.wait.return_value = False
        counter = 0

        def fake_time_func():
            nonlocal counter
            counter += 1
            if counter > 1:
                return 10000 + 5 * 60 * 60  # 5 hours
            return 10000
        mock_time.side_effect = fake_time_func
        brew_session.cancelTask.return_value = True
        brew_session.cancelTask.reset_mock()
        errors = brew.watch_tasks(brew_session, log_func, tasks, terminate_event)
        self.assertTrue(all(map(lambda failure: failure == "Timeout watching task", errors.values())))
        brew_session.cancelTask.assert_has_calls([mock.call(task, recurse=True) for task in tasks], any_order=True)
