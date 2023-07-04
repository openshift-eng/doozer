import io
import json
from unittest import TestCase
from unittest.mock import MagicMock, patch

import yaml

from doozerlib.cli import detect_embargo


class TestDetectEmbargoCli(TestCase):
    def test_detect_embargoes_in_nvrs(self):
        builds = [
            {"id": 1, "nvr": "foo-1.2.3-1.p0"},
            {"id": 2, "nvr": "bar-1.2.3-1.p1"}
        ]
        nvrs = [b["nvr"] for b in builds]
        expected = [builds[1]]
        with patch("doozerlib.brew.get_build_objects", return_value=builds), \
             patch("doozerlib.build_status_detector.BuildStatusDetector.find_embargoed_builds", return_value=[2]):
            actual = detect_embargo.detect_embargoes_in_nvrs(MagicMock(), nvrs)
        self.assertListEqual(actual, expected)

    def test_detect_embargoes_in_tags(self):
        included_tags = ["a-candidate", "b-candidate"]
        candidate_tags = set(included_tags)
        runtime = MagicMock()
        runtime.get_candidate_brew_tags.return_value = candidate_tags
        included_builds = [
            [{"id": 11, "nvr": "foo11-1.2.3-1.p0"}, {"id": 12, "nvr": "foo12-1.2.3-1.p1"}, {"id": 13, "nvr": "foo13-1.2.3-1.p1"}],
            [{"id": 21, "nvr": "foo21-1.2.3-1.p0"}, {"id": 22, "nvr": "foo22-1.2.3-1.p1"}, {"id": 23, "nvr": "foo23-1.2.3-1.p1"}],
        ]
        excluded_tags = ["a", "b"]
        excluded_builds = [
            [{"id": 12, "nvr": "foo12-1.2.3-1.p1"}],
            [{"id": 22, "nvr": "foo22-1.2.3-1.p1"}],
        ]
        builds_to_detect = [b for builds in included_builds for b in builds if b["id"] in {11, 13, 21, 23}]
        event_id = 42
        expected = [b for builds in included_builds for b in builds if b["id"] in {13, 23}]
        with patch("doozerlib.brew.get_latest_builds", return_value=included_builds), \
             patch("doozerlib.brew.get_tagged_builds", return_value=excluded_builds), \
             patch("doozerlib.build_status_detector.BuildStatusDetector.find_embargoed_builds", return_value=[13, 23]) as find_embargoed_builds:
            actual = detect_embargo.detect_embargoes_in_tags(runtime, "all", included_tags, excluded_tags, event_id)
            find_embargoed_builds.assert_called_once_with(builds_to_detect, candidate_tags)
        self.assertEqual(actual, expected)

    @patch("doozerlib.exectools.parallel_exec")
    def test_detect_embargoes_in_pullspecs(self, parallel_exec):
        pullspecs = ["example.com/repo:foo", "example.com/repo:bar"]
        builds = [
            {"id": 1, "nvr": "foo-1.2.3-1.p0"},
            {"id": 2, "nvr": "bar-1.2.3-1.p1"}
        ]
        nvrs = [("foo", "1.2.3", "1.p0"), ("bar", "1.2.3", "1.p1")]
        expected = ([pullspecs[1]], [builds[1]])
        fake_runtime = MagicMock()
        parallel_exec.return_value.get.return_value = nvrs
        with patch("doozerlib.cli.detect_embargo.detect_embargoes_in_nvrs", return_value=[builds[1]]) as detect_embargoes_in_nvrs:
            actual = detect_embargo.detect_embargoes_in_pullspecs(fake_runtime, pullspecs)
            detect_embargoes_in_nvrs.assert_called_once_with(fake_runtime, [f"{n}-{v}-{r}" for n, v, r in nvrs])
        self.assertEqual(actual, expected)

    @patch("doozerlib.exectools.parallel_exec")
    def test_detect_embargoes_in_releases(self, parallel_exec):
        releases = ["a", "b"]
        release_pullspecs = {
            "a": ["example.com/repo:dead", "example.com/repo:beef"],
            "b": ["example.com/repo:foo", "example.com/repo:bar"],
        }
        builds = [
            {"id": 1, "nvr": "foo-1.2.3-1.p0"},
            {"id": 2, "nvr": "bar-1.2.3-1.p1"}
        ]
        expected = ([releases[1]], [release_pullspecs["b"][1]], [builds[1]])
        fake_runtime = MagicMock()
        parallel_exec.return_value.get.return_value = [release_pullspecs[k] for k in releases]
        with patch("doozerlib.cli.detect_embargo.detect_embargoes_in_pullspecs") as detect_embargoes_in_pullspecs:
            detect_embargoes_in_pullspecs.side_effect = lambda _, pullspecs: (["example.com/repo:bar"], [builds[1]]) if "example.com/repo:bar" in pullspecs else ([], [])
            actual = detect_embargo.detect_embargoes_in_releases(fake_runtime, releases)
            detect_embargoes_in_pullspecs.assert_called()
            detect_embargoes_in_pullspecs.reset_mock()
        self.assertEqual(actual, expected)

    @patch("doozerlib.exectools.cmd_assert")
    def test_get_nvr_by_pullspec(self, fake_cmd_assert):
        pullspec = "registry-proxy.engineering.redhat.com/rh-osbs/openshift-ose-cluster-autoscaler:v4.3.25-202006081335"
        expected = ("atomic-openshift-cluster-autoscaler-container", "v4.3.25", "202006081335")
        fake_cmd_assert.return_value = ("""
        {"config":{"Labels": {"com.redhat.component":"atomic-openshift-cluster-autoscaler-container", "version":"v4.3.25", "release":"202006081335"}}}
        """, "")
        actual = detect_embargo.get_nvr_by_pullspec(pullspec)
        self.assertEqual(actual, expected)

    @patch("doozerlib.exectools.cmd_assert")
    def test_get_image_pullspecs_from_release_payload(self, fake_cmd_assert):
        fake_cmd_assert.return_value = ("""
        {"references":{"spec":{"tags":[{"name":"foo","from":{"name":"registry.example.com/foo:abc"}}, {"name":"bar","from":{"name":"registry.example.com/bar:def"}}]}}}
        """, "")
        actual = list(detect_embargo.get_image_pullspecs_from_release_payload("doesn't matter"))
        expected = ["registry.example.com/foo:abc", "registry.example.com/bar:def"]
        self.assertListEqual(actual, expected)

    @patch("builtins.exit")
    @patch('sys.stdout', new_callable=io.StringIO)
    def test_print_result_and_exit(self, mock_stdout, mock_exit):
        embargoed_builds = [{"id": 1}, {"id": 2}]
        embargoed_pullspecs = ["a", "b"]
        embargoed_releases = ["d", "e"]
        expected = {
            "has_embargoes": True,
            "builds": embargoed_builds,
            "pullspecs": embargoed_pullspecs,
            "releases": embargoed_releases
        }
        detect_embargo.print_result_and_exit(embargoed_builds, embargoed_pullspecs, embargoed_releases, True, False)
        mock_exit.assert_called_once_with(0)
        actual = yaml.safe_load(mock_stdout.getvalue())
        self.assertEqual(actual, expected)
        mock_exit.reset_mock()
        mock_stdout.truncate(0)
        mock_stdout.seek(0)
        detect_embargo.print_result_and_exit(embargoed_builds, embargoed_pullspecs, embargoed_releases, False, True)
        mock_exit.assert_called_once_with(0)
        actual = json.loads(mock_stdout.getvalue())
        self.assertEqual(actual, expected)
        mock_exit.reset_mock()
        detect_embargo.print_result_and_exit(None, None, None, False, False)
        mock_exit.assert_called_once_with(2)
