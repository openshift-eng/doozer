from unittest import TestCase

from unittest.mock import MagicMock, Mock, patch

from doozerlib.model import Model
from doozerlib.plashet import PlashetBuilder


class TestPlashetBuilder(TestCase):
    @patch("doozerlib.plashet.get_build_objects")
    def test_get_builds(self, get_build_objects: Mock):
        builder = PlashetBuilder(MagicMock())
        builder._build_cache = {
            1: {"id": 1, "build_id": 1, "nvr": "fake1-1.2.3-1.el8"},
            "bar-1.2.3-1.el8": {"id": 3, "build_id": 3, "nvr": "bar-1.2.3-1.el8"},
        }
        get_build_objects.return_value = [
            {"id": 2, "build_id": 2, "nvr": "fake2-1.2.3-1.el8"},
            {"id": 4, "build_id": 4, "nvr": "foo-1.2.3-1.el8", "epoch": "2"},
        ]
        actual = builder._get_builds([1, 2, "foo-1.2.3-1.el8:2", "bar-1.2.3-1.el8"])
        get_build_objects.assert_called_once()
        self.assertListEqual([b["id"] for b in actual], [1, 2, 4, 3])

    def test_cache_build(self):
        builder = PlashetBuilder(MagicMock())
        builder._build_cache = {
            1: {"id": 1, "build_id": 1, "nvr": "fake1-1.2.3-1.el8"},
            "bar-1.2.3-1.el8": {"id": 3, "build_id": 3, "nvr": "bar-1.2.3-1.el8"},
        }
        builder._cache_build({"id": 4, "build_id": 4, "nvr": "foo-1.2.3-1.el8", "epoch": "2"})
        self.assertEqual(set(builder._build_cache.keys()), {1, "bar-1.2.3-1.el8", 4, "foo-1.2.3-1.el8", "foo-1.2.3-1.el8:2"})

    def test_from_tag_with_assembly_disabled(self):
        koji_api = MagicMock()
        koji_api.listTagged.return_value = [
            {"id": 1, "build_id": 1, "nvr": "fake2-1.2.4-1.assembly.stream.el8", "name": "fake2", "release": "1.assembly.stream.el8", "tag_name": "fake-rhel-8-candidate"},
            {"id": 4, "build_id": 4, "nvr": "foo-1.2.3-1.assembly.stream.el8", "epoch": "2", "name": "foo", "release": "1.assembly.stream.el8", "tag_name": "fake-rhel-8-candidate"},
        ]
        builder = PlashetBuilder(koji_api)
        actual = builder.from_tag("fake-rhel-8-candidate", True, None, None)
        expected = {1, 4}
        self.assertEqual({b["id"] for b in actual.values()}, expected)

    def test_from_tag_with_assembly_enabled(self):
        koji_api = MagicMock()
        koji_api.listTagged.return_value = [
            {"id": 1, "build_id": 1, "nvr": "fake2-1.2.4-1.assembly.stream.el8", "name": "fake2", "release": "1.assembly.stream.el8", "tag_name": "fake-rhel-8-candidate"},
            {"id": 2, "build_id": 2, "nvr": "fake2-1.2.3-1.assembly.art1.el8", "name": "fake2", "release": "1.assembly.art1.el8", "tag_name": "fake-rhel-8-candidate"},
            {"id": 4, "build_id": 4, "nvr": "foo-1.2.3-1.assembly.stream.el8", "epoch": "2", "name": "foo", "release": "1.assembly.stream.el8", "tag_name": "fake-rhel-8-candidate"},
        ]
        builder = PlashetBuilder(koji_api)
        actual = builder.from_tag("fake-rhel-8-candidate", True, "art1", None)
        expected = {2, 4}
        self.assertEqual({b["id"] for b in actual.values()}, expected)

    def test_from_group_deps(self):
        builder = PlashetBuilder(MagicMock())
        group_config = Model({
            "dependencies": {
                "rpms": [
                    {"el8": "fake1-1.2.3-1.el8"},
                    {"el8": "fake2-1.2.3-1.el8"},
                    {"el7": "fake2-1.2.3-1.el7"},
                    {"el7": "fake2-1.2.3-1.el7"},
                ]
            }
        })
        builder._get_builds = MagicMock(return_value=[
            {"id": 1, "build_id": 1, "name": "fake1", "nvr": "fake1-1.2.3-1.el8"},
            {"id": 2, "build_id": 2, "name": "fake2", "nvr": "fake2-1.2.3-1.el8"},
        ])
        actual = builder.from_group_deps(8, group_config, {})
        self.assertEqual([b["nvr"] for b in actual.values()], ["fake1-1.2.3-1.el8", "fake2-1.2.3-1.el8"])
        builder._get_builds.assert_called_once()

    def test_from_group_deps_with_art_managed_rpms(self):
        builder = PlashetBuilder(MagicMock())
        group_config = Model({
            "dependencies": {
                "rpms": [
                    {"el8": "fake1-1.2.3-1.el8"},
                    {"el8": "fake2-1.2.3-1.el8"},
                    {"el8": "fake3-1.2.3-1.el8"},
                    {"el7": "fake2-1.2.3-1.el7"},
                    {"el7": "fake2-1.2.3-1.el7"},
                ]
            }
        })
        builder._get_builds = MagicMock(return_value=[
            {"id": 1, "build_id": 1, "name": "fake1", "nvr": "fake1-1.2.3-1.el8"},
            {"id": 2, "build_id": 2, "name": "fake2", "nvr": "fake2-1.2.3-1.el8"},
            {"id": 3, "build_id": 3, "name": "fake3", "nvr": "fake3-1.2.3-1.el8"},
        ])
        with self.assertRaises(ValueError) as ex:
            builder.from_group_deps(8, group_config, {"fake3": MagicMock(rpm_name="fake3")})
        self.assertIn("Group dependencies cannot have ART managed RPMs", str(ex.exception))
        builder._get_builds.assert_called_once()

    @patch("doozerlib.plashet.assembly_metadata_config")
    def test_from_pinned_by_is(self, assembly_metadata_config: Mock):
        builder = PlashetBuilder(MagicMock())
        releases_config = Model()
        rpm_metas = {
            "fake1": MagicMock(rpm_name="fake1"),
            "fake2": MagicMock(rpm_name="fake2"),
        }
        meta_configs = {
            "fake1": Model({
                "is": {
                    "el8": "fake1-1.2.3-1.el8"
                }
            }),
            "fake2": Model({
                "is": {
                    "el8": "fake2-1.2.3-1.el8"
                }
            }),
        }
        builder._get_builds = MagicMock(return_value=[
            {"id": 1, "build_id": 1, "name": "fake1", "nvr": "fake1-1.2.3-1.el8"},
            {"id": 2, "build_id": 2, "name": "fake2", "nvr": "fake2-1.2.3-1.el8"},
        ])
        assembly_metadata_config.side_effect = lambda *args: meta_configs[args[3]]
        actual = builder.from_pinned_by_is(8, "art1", releases_config, rpm_metas)
        self.assertEqual([b["nvr"] for b in actual.values()], ["fake1-1.2.3-1.el8", "fake2-1.2.3-1.el8"])
        builder._get_builds.assert_called_once()

    @patch("doozerlib.plashet.list_archives_by_builds")
    def test_from_images(self, list_archives_by_builds: Mock):
        builder = PlashetBuilder(MagicMock())
        image_map = {
            "fake-image1": MagicMock(),
            "fake-image2": MagicMock(),
        }
        list_archives_by_builds.return_value = [
            [{"rpms": [{"build_id": 101}, {"build_id": 102}, {"build_id": 103}]}, {"rpms": [{"build_id": 102}, {"build_id": 104}]}],
            [{"rpms": [{"build_id": 201}, {"build_id": 202}]}],
        ]
        builder._get_builds = MagicMock(return_value=[
            {"id": 101, "build_id": 101, "name": "fake101", "nvr": "fake101-1.2.3-1.el8"},
            {"id": 102, "build_id": 102, "name": "fake102", "nvr": "fake102-1.2.3-1.el8"},
            {"id": 103, "build_id": 103, "name": "fake103", "nvr": "fake103-1.2.3-1.el8"},
            {"id": 104, "build_id": 104, "name": "fake104", "nvr": "fake104-1.2.3-1.el8"},
            {"id": 201, "build_id": 201, "name": "fake201", "nvr": "fake201-1.2.3-1.el8"},
            {"id": 202, "build_id": 202, "name": "fake202", "nvr": "fake202-1.2.3-1.el8"},
        ])
        actual = builder.from_images(image_map)
        self.assertEqual({rpm_build["nvr"] for rpm_build in actual["fake-image1"]}, {"fake101-1.2.3-1.el8", "fake102-1.2.3-1.el8", "fake103-1.2.3-1.el8", "fake104-1.2.3-1.el8"})
        self.assertEqual({rpm_build["nvr"] for rpm_build in actual["fake-image2"]}, {"fake201-1.2.3-1.el8", "fake202-1.2.3-1.el8"})
        image_map["fake-image1"].get_latest_build.assert_called_once()
        image_map["fake-image2"].get_latest_build.assert_called_once()
        builder._get_builds.assert_called_once_with({101, 102, 103, 104, 201, 202})
        list_archives_by_builds.assert_called_once()

    @patch("doozerlib.plashet.assembly_metadata_config")
    def test_from_image_member_deps(self, assembly_metadata_config: Mock):
        builder = PlashetBuilder(MagicMock())

        builder._get_builds = MagicMock(return_value=[
            {"id": 1, "build_id": 1, "name": "fake1", "nvr": "fake1-1.2.3-1.el8"},
            {"id": 2, "build_id": 2, "name": "fake2", "nvr": "fake2-1.2.3-1.el8"},
            {"id": 3, "build_id": 3, "name": "fake3", "nvr": "fake3-1.2.3-1.el8"},
        ])
        assembly_metadata_config.return_value = Model({
            "dependencies": {
                "rpms": [
                    {"el8": "fake1-1.2.3-1.el8"},
                    {"el8": "fake2-1.2.3-1.el8"},
                    {"el8": "fake3-1.2.3-1.el8"},
                    {"el7": "fake2-1.2.3-1.el7"},
                    {"el7": "fake2-1.2.3-1.el7"},
                ]
            }
        })
        image_meta = Model({
            "distgit_key": "fake-image",
        })
        actual = builder.from_image_member_deps(8, "art1", Model(), image_meta, {})
        self.assertEqual([b["nvr"] for b in actual.values()], ["fake1-1.2.3-1.el8", "fake2-1.2.3-1.el8", "fake3-1.2.3-1.el8"])
        builder._get_builds.assert_called_once_with(["fake1-1.2.3-1.el8", "fake2-1.2.3-1.el8", "fake3-1.2.3-1.el8"])
        assembly_metadata_config.assert_called_once()

    @patch("doozerlib.plashet.assembly_rhcos_config")
    def test_from_rhcos_deps(self, assembly_rhcos_config: Mock):
        builder = PlashetBuilder(MagicMock())

        builder._get_builds = MagicMock(return_value=[
            {"id": 1, "build_id": 1, "name": "fake1", "nvr": "fake1-1.2.3-1.el8"},
            {"id": 2, "build_id": 2, "name": "fake2", "nvr": "fake2-1.2.3-1.el8"},
            {"id": 3, "build_id": 3, "name": "fake3", "nvr": "fake3-1.2.3-1.el8"},
        ])
        assembly_rhcos_config.return_value = Model({
            "dependencies": {
                "rpms": [
                    {"el8": "fake1-1.2.3-1.el8"},
                    {"el8": "fake2-1.2.3-1.el8"},
                    {"el8": "fake3-1.2.3-1.el8"},
                    {"el7": "fake2-1.2.3-1.el7"},
                    {"el7": "fake2-1.2.3-1.el7"},
                ]
            }
        })
        actual = builder.from_rhcos_deps(8, "art1", Model(), {})
        self.assertEqual([b["nvr"] for b in actual.values()], ["fake1-1.2.3-1.el8", "fake2-1.2.3-1.el8", "fake3-1.2.3-1.el8"])
        builder._get_builds.assert_called_once_with(["fake1-1.2.3-1.el8", "fake2-1.2.3-1.el8", "fake3-1.2.3-1.el8"])
        assembly_rhcos_config.assert_called_once()
