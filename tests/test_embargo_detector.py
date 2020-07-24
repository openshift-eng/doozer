from unittest import TestCase
from unittest.mock import MagicMock, patch

from doozerlib.embargo_detector import EmbargoDetector


class TestEmbargoDetector(TestCase):
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
        archive_lists = {
            1: [{"build_id": 1, "id": 11}],
            2: [{"build_id": 2, "id": 21}],
            3: [{"build_id": 3, "id": 31}],
            4: [{"build_id": 4, "id": 41}],
            5: [{"build_id": 5, "id": 51}],
        }
        rpm_lists = [
            [{"id": 0, "build_id": 101, "release": "101.p1"}],
            [{"id": 1, "build_id": 201, "release": "201.p1"}],
        ]
        shipped_builds = {3, 5, 101}
        expected = {2, 4}

        with patch("doozerlib.brew.get_builds_tags", return_value=tags), \
             patch("doozerlib.brew.list_image_rpms", return_value=rpm_lists), \
             patch("doozerlib.embargo_detector.EmbargoDetector.find_shipped_builds", side_effect=lambda builds: {b for b in builds if b in shipped_builds}):
            detector = EmbargoDetector(MagicMock(), MagicMock())
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
            actual = EmbargoDetector(MagicMock(), MagicMock()).find_shipped_builds(build_ids)
            self.assertEqual(actual, expected)
            get_builds_tags.return_value = [
                [{"name": "foo-candidate"}, {"name": "bar-released"}],
                [{"name": "bar-candidate"}, {"name": "bar-released"}, ],
            ]
            expected = {1, 2}
            actual = EmbargoDetector(MagicMock(), MagicMock()).find_shipped_builds(build_ids)
            self.assertEqual(actual, expected)
            get_builds_tags.return_value = [
                [],
                [],
            ]
            expected = set()
            actual = EmbargoDetector(MagicMock(), MagicMock()).find_shipped_builds(build_ids)
            self.assertEqual(actual, expected)
            get_builds_tags.return_value = [
                [{"name": "foo-released"}],
                [{"name": "bar-candidate"}, {"name": "bar-released"}, ]
            ]
            expected = {1, 2}
            actual = EmbargoDetector(MagicMock(), MagicMock()).find_shipped_builds(build_ids)
            self.assertEqual(actual, expected)
