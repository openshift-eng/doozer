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
             patch("doozerlib.build_status_detector.BuildStatusDetector.find_shipped_builds",
                   side_effect=lambda builds: {b for b in builds if b in shipped_builds}):
            detector = BuildStatusDetector(MagicMock(), MagicMock())
            detector.archive_lists = archive_lists
            actual = detector.find_embargoed_builds(builds, [])
            self.assertEqual(actual, expected)

    def test_find_with_embargoed_rpms(self):
        # this is tested indirectly via find_embargoed_builds, but test the embargoed tags too
        image_builds = [
            {"id": 1, "release": "1.p0"},
            {"id": 2, "release": "2.p0"},
            {"id": 3, "release": "3.p0"},
            {"id": 4, "release": "4.p0"},
        ]
        rpm_lists = [
            [{"id": 0, "build_id": 101, "release": "101.p1"}],  # embargoed per source
            [{"id": 1, "build_id": 201, "release": "201.p0"}],  # never embargoed
            [{"id": 2, "build_id": 301, "release": "301.el8"}],  # in embargoed tag, but now shipped
            [{"id": 3, "build_id": 401, "release": "401.el8"}],  # still embargoed by tag
        ]
        archive_lists = {
            1: [{"build_id": 1, "id": 11, "rpms": rpm_lists[0]}],
            2: [{"build_id": 2, "id": 21, "rpms": rpm_lists[1]}],
            3: [{"build_id": 3, "id": 31, "rpms": rpm_lists[2]}],
            4: [{"build_id": 4, "id": 41, "rpms": rpm_lists[3]}],
        }
        shipped_rpm_builds = {301}
        embargoed_tag_builds = {301, 401}
        expected = {1, 4}

        with patch("doozerlib.build_status_detector.BuildStatusDetector.rpms_in_embargoed_tag",
                   return_value=embargoed_tag_builds), \
             patch("doozerlib.build_status_detector.BuildStatusDetector.find_shipped_builds",
                   side_effect=lambda builds: {b for b in builds if b in shipped_rpm_builds}):
            detector = BuildStatusDetector(MagicMock(), MagicMock())
            detector.archive_lists = archive_lists
            actual = detector.find_with_embargoed_rpms(set(b["id"] for b in image_builds), ["test-candidate"])
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
        latest = [dict(id=1, package_name='p1'), dict(id=2, package_name='p2'), dict(id=3, package_name='p3')]
        shipped_ids = {2}
        rpms = [  # just need to get the same thing back, not inspect contents
            [object(), object()],
            [object()],
        ]
        runtime = MagicMock()
        detector = BuildStatusDetector(runtime, MagicMock())
        session = detector.koji_session
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

    def test_rpms_in_embargoed_tag(self):
        session = MagicMock()
        detector = BuildStatusDetector(session, MagicMock())
        session = detector.koji_session
        session.listTagged.return_value = [{"id": 42}]

        expected = {42}
        actual = detector.rpms_in_embargoed_tag("foo-candidate")
        self.assertEqual(actual, expected)
        actual = detector.rpms_in_embargoed_tag("foo-candidate")  # second time should be cached
        self.assertEqual(actual, expected)
        session.listTagged.assert_called_once_with("foo-embargoed", event=None, type="rpm")
