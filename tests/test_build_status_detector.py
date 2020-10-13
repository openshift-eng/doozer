from unittest import TestCase
from unittest.mock import MagicMock, patch

from doozerlib.build_status_detector import BuildStatusDetector


class TestBuildStatusDetector(TestCase):
    def test_find_embargoed_builds(self):
        builds = [
            {"id": 1, "release": "1.p0"},
            {"id": 2, "release": "2.p0"},
            {"id": 3, "release": "3.p0"},
            {"id": 4, "release": "4.p1"},
            {"id": 5, "release": "5.p1"},
        ]
        tags = [
            [{"name": "foo-candidate"}],
            [{"name": "foo-candidate"}],
            [{"name": "foo-released"}],
            [{"name": "foo-candidate"}],
            [{"name": "foo-released"}],
        ]
        rpm_lists = [
            [{"id": 0, "build_id": 101, "release": "101.p1"}],
            [{"id": 1, "build_id": 201, "release": "201.p1"}],
        ]
        archive_lists = {
            1: [{"build_id": 1, "id": 11, "rpms": rpm_lists[0]}],
            2: [{"build_id": 2, "id": 21, "rpms": rpm_lists[1]}],
            3: [{"build_id": 3, "id": 31, "rpms": []}],
            4: [{"build_id": 4, "id": 41, "rpms": []}],
            5: [{"build_id": 5, "id": 51, "rpms": []}],
        }
        shipped_builds = {3, 5, 101}
        expected = {2, 4}

        with patch("doozerlib.brew.get_builds_tags", return_value=tags), \
             patch("doozerlib.brew.list_image_rpms", return_value=rpm_lists), \
             patch("doozerlib.build_status_detector.BuildStatusDetector.find_shipped_builds", side_effect=lambda builds: {b for b in builds if b in shipped_builds}):
            detector = BuildStatusDetector(MagicMock(), MagicMock())
            detector.archive_lists = archive_lists
            actual = detector.find_embargoed_builds(builds)
            self.assertEqual(actual, expected)

    def test_find_shipped_builds(self):
        rpms = [
            {"build_id": 1},
            {"build_id": 2},
        ]
        build_ids = [r["build_id"] for r in rpms]
        with patch("doozerlib.brew.get_builds_tags") as get_builds_tags:
            get_builds_tags.return_value = [
                [{"name": "foo-candidate"}],
                [{"name": "bar-candidate"}, {"name": "bar-released"}, ],
            ]
            expected = {2}
            actual = BuildStatusDetector(MagicMock(), MagicMock()).find_shipped_builds(build_ids)
            self.assertEqual(actual, expected)
            get_builds_tags.return_value = [
                [{"name": "foo-candidate"}, {"name": "bar-released"}],
                [{"name": "bar-candidate"}, {"name": "bar-released"}, ],
            ]
            expected = {1, 2}
            actual = BuildStatusDetector(MagicMock(), MagicMock()).find_shipped_builds(build_ids)
            self.assertEqual(actual, expected)
            get_builds_tags.return_value = [
                [],
                [],
            ]
            expected = set()
            actual = BuildStatusDetector(MagicMock(), MagicMock()).find_shipped_builds(build_ids)
            self.assertEqual(actual, expected)
            get_builds_tags.return_value = [
                [{"name": "foo-released"}],
                [{"name": "bar-candidate"}, {"name": "bar-released"}, ]
            ]
            expected = {1, 2}
            actual = BuildStatusDetector(MagicMock(), MagicMock()).find_shipped_builds(build_ids)
            self.assertEqual(actual, expected)

    def test_find_unshipped_candidate_rpms(self):
        latest = [dict(id=1), dict(id=2), dict(id=3)]
        shipped_ids = {2}
        rpms = [  # just need to get the same thing back, not inspect contents
            [object(), object()],
            [object()],
        ]
        session = MagicMock()
        detector = BuildStatusDetector(session, MagicMock())
        session.getLatestBuilds.return_value = latest
        with patch.object(detector, 'find_shipped_builds') as find_shipped_builds, \
             patch("doozerlib.brew.list_build_rpms") as list_build_rpms:
            find_shipped_builds.return_value = shipped_ids
            list_build_rpms.return_value = rpms

            expected = rpms[0] + rpms[1]
            actual = detector.find_unshipped_candidate_rpms("tag-candidate")
            self.assertEqual(actual, expected)

            actual = detector.find_unshipped_candidate_rpms("tag-candidate")  # should be cached
            self.assertEqual(actual, expected)
            session.getLatestBuilds.assert_called_once_with("tag-candidate", event=None, type="rpm")
            find_shipped_builds.assert_called_once_with([1, 2, 3])
            list_build_rpms.assert_called_once_with([1, 3], session)
