from typing import List, Dict, Optional, Set, Tuple
from unittest import TestCase
from unittest.mock import MagicMock, patch
import json

from doozerlib.cli import get_nightlies as subject
from doozerlib.model import Model


class TestGetNightlies(TestCase):
    def setUp(self):
        self.runtime = MagicMock(group_config=Model(dict(
            arches=["x86_64", "s390x", "ppc64le", "aarch64"],
            multi_arch=dict(enabled=True),
        )))
        subject.image_info_cache = {}

    def test_determine_arch_list(self):
        self.assertEqual(
            # {"aarch64", "x86_64", "multi"},  # no multi yet
            {"aarch64", "x86_64"},
            set(subject.determine_arch_list(self.runtime, ["s390x", "ppc64le"]))
        )

        runtime = MagicMock(group_config=Model(dict(arches=["x86_64", "aarch64"])))
        with self.assertRaises(ValueError, msg="should fail when specifying non-configured arch"):
            subject.determine_arch_list(runtime, ["bogus"])
        # with self.assertRaises(ValueError, msg="should fail when specifying multi if not configured"):
        #     subject.determine_arch_list(runtime, ["multi"])  # no multi yet

        self.assertEqual({"aarch64"}, subject.determine_arch_list(runtime, {"x86_64"}))
        self.assertEqual({"x86_64", "aarch64"}, subject.determine_arch_list(runtime, set()))

    @patch('urllib.request.urlopen')
    def test_find_rc_nightlies(self, urlopen_mock):
        data = """
        {
          "name": "4.12.0-0.nightly",
          "tags": [
            {
              "name": "4.12.0-0.nightly-2022-07-15-132344",
              "phase": "Ready",
              "pullSpec": "registry.ci.openshift.org/ocp/release:4.12.0-0.nightly-2022-07-15-132344",
              "downloadURL": "https://openshift-release-artifacts.apps.ci.l2s4.p1.openshiftapps.com/4.12.0-0.nightly-2022-07-15-132344"
            },
            {
              "name": "4.12.0-0.nightly-2022-07-15-065851",
              "phase": "Rejected",
              "pullSpec": "registry.ci.openshift.org/ocp/release:4.12.0-0.nightly-2022-07-15-065851",
              "downloadURL": "https://openshift-release-artifacts.apps.ci.l2s4.p1.openshiftapps.com/4.12.0-0.nightly-2022-07-15-065851"
            },
            {
              "name": "4.12.0-0.nightly-2022-07-15-024227",
              "phase": "Accepted",
              "pullSpec": "registry.ci.openshift.org/ocp/release:4.12.0-0.nightly-2022-07-15-024227",
              "downloadURL": "https://openshift-release-artifacts.apps.ci.l2s4.p1.openshiftapps.com/4.12.0-0.nightly-2022-07-15-024227"
            }
        ]}
        """
        cm = MagicMock(getcode=200)
        cm.read.return_value = bytes(data, encoding="utf-8")
        cm.__enter__.return_value = cm
        urlopen_mock.return_value = cm

        self.assertEqual(1, len(subject.find_rc_nightlies(self.runtime, {"x86_64"}, False, False)["x86_64"]))
        self.assertEqual(3, len(subject.find_rc_nightlies(self.runtime, {"x86_64"}, True, True)["x86_64"]))
        self.assertEqual(1, len(subject.find_rc_nightlies(self.runtime, {"x86_64"}, True, True, ["4.12.0-0.nightly-2022-07-15-132344"])["x86_64"]))

        with self.assertRaises(subject.NoMatchingNightlyException):
            subject.find_rc_nightlies(self.runtime, {"x86_64"}, True, True, ["not-found-name"])

        cm.read.return_value = b"{}"
        with self.assertRaises(subject.EmptyArchException):
            subject.find_rc_nightlies(self.runtime, {"x86_64"}, True, True)

    @staticmethod
    def vanilla_nightly(release_image_info=None, name=None):
        # just give me an instance to test (default supplies "pod" entry)
        nightly = subject.Nightly(
            release_image_info=release_image_info or {
                "references": {"spec": {"tags": [
                    {
                        "name": "pod",
                        "annotations": {"io.openshift.build.commit.id": "pod-commit"},
                        "from": {"name": "pod-pullspec"},
                    }
                ]}}
            },
            name=name or "name", phase="Accepted", pullspec="nightly-pullspec",
        )
        nightly._process_nightly_release_data()
        return nightly

    def test_nightly_eq(self):
        test_json = """
            {
                "two-tags": {
                    "references": {
                      "spec": {
                        "tags": [
                            {
                              "name": "cluster-version-operator",
                              "annotations": { "io.openshift.build.commit.id": "cvo-commit" },
                              "from": { "name": "ignore" }
                            },
                            {
                              "name": "pod",
                              "annotations": { "io.openshift.build.commit.id": "pod-commit" },
                              "from": { "name": "ignore" }
                            }
                        ]
                }}},
                "extra-tag": {
                    "references": {
                      "spec": {
                        "tags": [
                            {
                              "name": "cluster-version-operator",
                              "annotations": { "io.openshift.build.commit.id": "cvo-commit" },
                              "from": { "name": "ignore" }
                            },
                            {
                              "name": "pod",
                              "annotations": { "io.openshift.build.commit.id": "pod-commit" },
                              "from": { "name": "ignore" }
                            },
                            {
                              "name": "unique to this nightly",
                              "annotations": { "io.openshift.build.commit.id": "b5932d451e2514e932be49a95b3c46b2a74b5b0c" },
                              "from": { "name": "ignore" }
                            }
                        ]
                }}},
                "cvo-tag-replaced-by-pod": {
                    "references": {
                      "spec": {
                        "tags": [
                            {
                              "name": "cluster-version-operator",
                              "annotations": { "io.openshift.build.commit.id": "pod-commit" },
                              "from": { "name": "ignore" }
                            },
                            {
                              "name": "pod",
                              "annotations": { "io.openshift.build.commit.id": "pod-commit" },
                              "from": { "name": "ignore" }
                            }
                        ]
                }}},
                "two-tag-different-pod": {
                    "references": {
                      "spec": {
                        "tags": [
                            {
                              "name": "cluster-version-operator",
                              "annotations": { "io.openshift.build.commit.id": "cvo-commit" },
                              "from": { "name": "ignore" }
                            },
                            {
                              "name": "pod",
                              "annotations": { "io.openshift.build.commit.id": "different-pod-commit" },
                              "from": { "name": "ignore" }
                            }
                        ]
                }}}
            }
        """
        rii: Dict[str, Dict] = json.loads(test_json)

        def make_nightly(ri_name):
            nightly = self.vanilla_nightly(release_image_info=rii[ri_name])
            return nightly

        n: Dict[str, subject.Nightly] = {name: make_nightly(name) for name in rii}
        self.assertEqual(n["two-tags"], n["extra-tag"])
        self.assertEqual(n["two-tags"], n["cvo-tag-replaced-by-pod"])
        self.assertNotEqual(n["two-tags"], n["two-tag-different-pod"])

    @patch('doozerlib.cli.get_nightlies.Nightly.retrieve_image_info')
    def test_retrieve_nvr_for_tag(self, mock_rii):
        mock_rii.return_value = Model(dict(config=dict(config=dict(Labels={
            "com.redhat.component": "spam",
            "version": "1.0",
            "release": "1.el8",
        }))))
        nightly = self.vanilla_nightly()
        self.assertEqual(("spam", "1.0", "1.el8"), nightly.retrieve_nvr_for_tag("pod"))

        mock_rii.return_value = Exception()  # should be cached from last call
        self.assertEqual(("spam", "1.0", "1.el8"), nightly.retrieve_nvr_for_tag("pod"))

        mock_rii.return_value = Model()  # no labels provided
        nightly.pullspec_for_tag["rhcos"] = "rhcos_ps"
        self.assertIsNone(nightly.retrieve_nvr_for_tag("rhcos"))

    def test_deeper_nightly(self):
        n1 = self.vanilla_nightly()
        n2 = self.vanilla_nightly()
        n1.rhcos_inspector = n2.rhcos_inspector = MagicMock()  # always match
        n1.commit_for_tag["pod"] = n2.commit_for_tag["pod"] = "commit1"
        n1.nvr_for_tag["pod"] = n2.nvr_for_tag["pod"] = ("nvr", "1", "1")
        self.assertTrue(n1.deeper_equivalence(n2))

        # works with missing entries too
        n1.commit_for_tag["missing1"] = n2.commit_for_tag["missing2"] = "mcommit"
        self.assertTrue(n1.deeper_equivalence(n2), "un-shared tags are ignored")
        self.assertTrue(n2.deeper_equivalence(n1), "... in both directions")

        # get a failure
        n2.nvr_for_tag["pod"] = ("nvr", "2", "2")
        self.assertFalse(n1.deeper_equivalence(n2))

        # give alt images (where components differ for the same tag) a pass.
        # most of the time they'll have the same VR but that's not absolutely
        # guaranteed (one build could flake then succeed with later R).
        # so just rely on source commit equivalence (already verified)
        # and ignore the slim possibility that the RPMs installed differ.
        n2.nvr_for_tag["pod"] = ("nvr-alt", "1", "1")
        self.assertTrue(n1.deeper_equivalence(n2), "alt images allowed to differ")

    def test_deeper_nightly_rhcos(self):
        n1 = self.vanilla_nightly()
        n2 = self.vanilla_nightly()
        ri1 = n1.rhcos_inspector = MagicMock()
        ri2 = n2.rhcos_inspector = MagicMock()
        for n in (n1, n2):
            n.rhcos_inspector.get_os_metadata_rpm_list.return_value = [("spam", 0, 1, 1, "noarch")]

        self.assertTrue(n1.deeper_nightly_rhcos(n2), "same RPM content")

        ri2.get_os_metadata_rpm_list.return_value = [("eggs", 0, 2, 3, "noarch")]
        self.assertTrue(n1.deeper_nightly_rhcos(n2), "unchecked RPM content")
        self.assertTrue(n2.deeper_nightly_rhcos(n1), "unmatched RPM content")

        ri1.get_os_metadata_rpm_list.return_value = [("sausage", 0, 1, 1, "noarch")]
        ri2.get_os_metadata_rpm_list.return_value = [("sausage", 0, 2, 3, "noarch")]
        self.assertFalse(n1.deeper_nightly_rhcos(n2), "mismatched RPM content")

    def test_nightly_set(self):
        nightlies = [
            {
                "name": "4.12.0-0.nightly-2022-07-15-132344",
                "phase": "Ready",
                "pullSpec": "registry.ci.openshift.org/ocp/release:4.12.0-0.nightly-2022-07-15-132344",
                "downloadURL": "https://openshift-release-artifacts.apps.ci.l2s4.p1.openshiftapps.com/4.12.0-0.nightly-2022-07-15-132344",
                "equivalence": "digest1",  # represents the Nightly determined for this nightly
                "releaseInfo": {"config": {"created": "2022-07-16"}},
            },
            {
                "name": "4.12.0-0.nightly-s390x-2022-07-15-065851",
                "phase": "Rejected",
                "pullSpec": "registry.ci.openshift.org/ocp/release:4.12.0-0.nightly-2022-07-15-065851",
                "downloadURL": "https://openshift-release-artifacts.apps.ci.l2s4.p1.openshiftapps.com/4.12.0-0.nightly-2022-07-15-065851",
                "equivalence": "digest2",
                "releaseInfo": {"config": {"created": "2022-07-17"}},
            },
            {
                "name": "4.12.0-0.nightly-arm64-2022-07-15-024227",
                "phase": "Accepted",
                "pullSpec": "registry.ci.openshift.org/ocp/release:4.12.0-0.nightly-2022-07-15-024227",
                "downloadURL": "https://openshift-release-artifacts.apps.ci.l2s4.p1.openshiftapps.com/4.12.0-0.nightly-2022-07-15-024227",
                "equivalence": "digest1",
                "releaseInfo": {"config": {"created": "2022-07-18"}},
            },
        ]

        def make(ndict):
            nightly = subject.Nightly(release_image_info=ndict["releaseInfo"], nightly_info=ndict)
            nightly.commit_for_tag["pod"] = ndict["equivalence"]  # set up comparison
            return nightly

        nset = subject.NightlySet({"x86_64": make(nightlies[0])})
        self.assertFalse(nset.generate_equivalents_with("s390x", [make(nightlies[1])]))

        set1 = nset.generate_equivalents_with("aarch64", [make(nightlies[2])])[0]
        self.assertIsInstance(set1, subject.NightlySet)
        self.assertEqual(set1.timestamp, "2022-07-18")  # greater of first and third

        self.assertFalse(set1.generate_equivalents_with("s390x", [make(nightlies[1])]))
        with self.assertRaises(subject.NightlySetDuplicateArchException):
            nset.generate_equivalents_with("x86_64", make(nightlies[0]))

    def test_generate_nightly_sets(self):

        def make(ndict):
            nightly = subject.Nightly(release_image_info=ndict["releaseInfo"], nightly_info=ndict, phase="Accepted", pullspec="ignore")
            nightly.commit_for_tag["pod"] = ndict["equivalence"]  # set up comparison
            return nightly

        nightlies_for_arch = {
            "x86_64": [
                make({
                    "name": "nightly1",
                    "equivalence": "digest1",  # represents the matching for this nightly
                    "releaseInfo": {"config": {"created": "2022-07-17"}},
                }),
                make({
                    "name": "nightly2",
                    "equivalence": "digest1",
                    "releaseInfo": {"config": {"created": "2022-07-18"}},
                }),
                make({
                    "name": "nightly3",
                    "equivalence": "digest2",
                    "releaseInfo": {"config": {"created": "2022-07-16"}},
                }),
            ],
            "s390x": [
                make({
                    "name": "nightly4",
                    "equivalence": "digest1",  # matches two
                    "releaseInfo": {"config": {"created": "2022-07-17"}},
                }),
                make({
                    "name": "nightly5",
                    "equivalence": "digest2",  # matches one
                    "releaseInfo": {"config": {"created": "2022-07-15"}},
                }),
            ],
        }
        sets = subject.generate_nightly_sets(nightlies_for_arch)
        self.assertEqual(3, len(sets))
        # also check that they sort by timestamp desc
        self.assertEqual("2022-07-18", sets[0].timestamp)
        self.assertEqual("2022-07-16", sets[2].timestamp)

        # check that an incompatible nightly torpedoes set creation
        nightlies_for_arch["ppc64le"] = [
            make({
                "name": "nightly6",
                "equivalence": "digest3",  # incompatible with all
                "releaseInfo": {"config": {"created": "2022-07-17"}},
            }),
        ]
        self.assertEqual(0, len(subject.generate_nightly_sets(nightlies_for_arch)))
