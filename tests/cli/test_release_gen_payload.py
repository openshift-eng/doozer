import mock
from doozerlib.model import Model
from doozerlib.image import ImageMetadata
import json
from unittest import TestCase
from unittest.mock import MagicMock, patch

from doozerlib.cli import release_gen_payload as rgp
import doozerlib.runtime


def _fake_generator():
    runtime = MagicMock(doozerlib.runtime)
    runtime.command = "dummy"
    runtime.state = {}
    runtime.logger = MagicMock()
    return rgp.PayloadGenerator(
        runtime,
        brew_session=MagicMock(),
        brew_event=None,
        base_target=rgp.SyncTarget(),
    )


class TestReleaseGenPayloadCli(TestCase):

    def test_get_istag_spec(self):
        gen = _fake_generator()
        tag_name = "cvo-tag"
        pullspec = "dummy-pullspec"
        record = rgp.BuildRecord(image=MagicMock(), archives=None)
        record.image.candidate_brew_tag.return_value = "tag-candidate"
        inconsistencies = {"foo is old": "should be foo-new"}
        expected = {
            'annotations': {'release.openshift.io/inconsistency': '["foo is old"]'},
            'name': tag_name,
            'from': {
                'kind': 'DockerImage',
                'name': pullspec
            }
        }
        with patch.object(gen, "_find_rpm_inconsistencies") as find_inc, \
             patch.object(gen, "_build_dest_name") as bdest:
            find_inc.return_value = inconsistencies
            bdest.return_value = pullspec
            source = dict(build_record=record, archive=None)
            actual = gen._get_istag_spec(tag_name, source, gen.base_target)
            self.assertEqual(actual, expected)
            self.assertEqual({tag_name: ["should be foo-new"]}, gen.state["inconsistencies"])

    def test_find_rpm_inconsistencies(self):
        archive = dict(
            build_id=1,
            rpms=[
                dict(name="foo", nvr="foo-1.2-1"),
                dict(name="bar", nvr="bar-3.4-1"),
            ],
        )
        gen = _fake_generator()
        with patch.object(gen.bs_detector, "find_unshipped_candidate_rpms") as func_rpms:
            func_rpms.return_value = [
                dict(name="foo", nvr="foo-1.2-1", build_id=2),
                dict(name="bar", nvr="bar-3.4-42", build_id=3),
            ]
            expected = {
                "Contains outdated RPM bar":
                "RPM bar-3.4-1 is installed in image build 1 but bar-3.4-42 from package build 3 is latest candidate"
            }
            actual = gen._find_rpm_inconsistencies(archive, "tag-candidate")
            self.assertEqual(actual, expected)

    def test_inconsistency_annotation(self):
        entries = ["a", "b", "c", "d", "e"]
        expected = json.dumps(entries)
        actual = list(_fake_generator()._inconsistency_annotation(entries).values())[0]
        self.assertEqual(actual, expected)

        entries.append("f")
        annotation = list(_fake_generator()._inconsistency_annotation(entries).values())[0]
        self.assertIn("(...and more)", annotation)

        entries = []
        expected = {}
        actual = _fake_generator()._inconsistency_annotation(entries)
        self.assertEqual(actual, expected)


class TestPayloadGenerator(TestCase):
    @mock.patch("doozerlib.cli.release_gen_payload.brew.get_tagged_builds")
    def test_get_latest_builds(self, get_tagged_builds: mock.Mock):
        runtime = MagicMock()
        image_metas = [
            ImageMetadata(runtime, Model({
                "key": "a",
                'data': {
                    'name': 'openshift/a',
                    'distgit': {'branch': 'fake-branch-rhel-8'},
                },
            })),
            ImageMetadata(runtime, Model({
                "key": "b",
                'data': {
                    'name': 'openshift/b',
                    'distgit': {'branch': 'fake-branch-rhel-7'},
                },
            })),
            ImageMetadata(runtime, Model({
                "key": "c",
                'data': {
                    'name': 'openshift/c',
                    'distgit': {'branch': 'fake-branch-rhel-8'},
                },
            })),
        ]
        get_tagged_builds.return_value = [
            [
                {"id": 13, "name": "a-container", "version": "v1.2.3", "release": "3.assembly.stream"},
                {"id": 12, "name": "a-container", "version": "v1.2.3", "release": "2.assembly.hotfix_a"},
                {"id": 11, "name": "a-container", "version": "v1.2.3", "release": "1.assembly.hotfix_a"},
            ],
            [
                {"id": 23, "name": "b-container", "version": "v1.2.3", "release": "3.assembly.test"},
                {"id": 22, "name": "b-container", "version": "v1.2.3", "release": "2.assembly.hotfix_b"},
                {"id": 21, "name": "b-container", "version": "v1.2.3", "release": "1.assembly.stream"},
            ],
            [
                {"id": 33, "name": "c-container", "version": "v1.2.3", "release": "3"},
                {"id": 32, "name": "c-container", "version": "v1.2.3", "release": "2.assembly.hotfix_b"},
                {"id": 31, "name": "c-container", "version": "v1.2.3", "release": "1"},
            ],
        ]
        generator = rgp.PayloadGenerator(runtime, MagicMock(), None, MagicMock())

        # assembly == "hotfix_a"
        runtime.assembly = "hotfix_a"
        expected = [12, 21]
        latest, _ = generator._get_latest_builds(image_metas)
        actual = [record.build["id"] for record in latest]
        self.assertEqual(actual, expected)

        # assembly == "hotfix_b"
        runtime.assembly = "hotfix_b"
        expected = [13, 22, 32]
        latest, _ = generator._get_latest_builds(image_metas)
        actual = [record.build["id"] for record in latest]
        self.assertEqual(actual, expected)

        # assembly == "hotfix_c"
        runtime.assembly = "hotfix_c"
        expected = [13, 21]
        latest, _ = generator._get_latest_builds(image_metas)
        actual = [record.build["id"] for record in latest]
        self.assertEqual(actual, expected)

        # assembly == None
        runtime.assembly = None
        expected = [13, 23, 33]
        latest, _ = generator._get_latest_builds(image_metas)
        actual = [record.build["id"] for record in latest]
        self.assertEqual(actual, expected)
