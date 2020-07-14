from unittest import TestCase, mock

from doozerlib.cli import release_gen_payload


class TestReleaseGenPayload(TestCase):
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
        archives_list = [
            [{"build_id": 1, "id": 11}],
            [{"build_id": 2, "id": 21}],
            [{"build_id": 3, "id": 31}],
            [{"build_id": 4, "id": 41}],
            [{"build_id": 5, "id": 51}],
        ]
        rpm_lists = [
            [{"id": 0, "build_id": 101, "release": "101.p1"}],
            [{"id": 1, "build_id": 201, "release": "201.p1"}],
        ]
        has_unshipped_rpms = [False, True]
        expected = {2, 4}
        counter = 0

        def _fake_has_unshipped_rpms(rpms, cache, brew_session, logger):
            nonlocal counter
            counter += 1
            return has_unshipped_rpms[counter - 1]

        with mock.patch("doozerlib.brew.get_builds_tags") as get_builds_tags, \
                mock.patch("doozerlib.brew.list_image_rpms") as list_image_rpms, \
                mock.patch("doozerlib.cli.release_gen_payload.has_unshipped_rpms") as has_unshipped_rpms_func:
            get_builds_tags.return_value = tags
            list_image_rpms.return_value = rpm_lists
            has_unshipped_rpms_func.side_effect = _fake_has_unshipped_rpms
            actual = release_gen_payload.find_embargoed_builds(builds, archives_list, mock.MagicMock(), mock.MagicMock())
            self.assertEqual(actual, expected)

    def test_has_unshipped_rpms(self):
        rpms = [
            {"build_id": 1},
            {"build_id": 2},
        ]
        with mock.patch("doozerlib.brew.get_builds_tags") as get_builds_tags:
            get_builds_tags.return_value = [
                [{"name": "foo-candidate"}],
                [{"name": "bar-candidate"}, {"name": "bar-released"}, ],
            ]
            actual = release_gen_payload.has_unshipped_rpms(rpms, {}, mock.MagicMock(), mock.MagicMock())
            self.assertTrue(actual)
            get_builds_tags.return_value = [
                [{"name": "foo-candidate"}, {"name": "bar-released"}],
                [{"name": "bar-candidate"}, {"name": "bar-released"}, ],
            ]
            actual = release_gen_payload.has_unshipped_rpms(rpms, {}, mock.MagicMock(), mock.MagicMock())
            self.assertFalse(actual)
            get_builds_tags.return_value = [
                [],
                [],
            ]
            actual = release_gen_payload.has_unshipped_rpms(rpms, {}, mock.MagicMock(), mock.MagicMock())
            self.assertTrue(actual)
            get_builds_tags.return_value = [
                [{"name": "foo-released"}],
                [{"name": "bar-candidate"}, {"name": "bar-released"}, ]
            ]
            actual = release_gen_payload.has_unshipped_rpms(rpms, {}, mock.MagicMock(), mock.MagicMock())
            self.assertFalse(actual)
