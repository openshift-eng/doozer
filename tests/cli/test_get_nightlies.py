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

    def test_populate_nightly_release_data(self):
        pass

    def test_equivalence_base(self):
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
        release_info: Dict[str, Dict] = json.loads(test_json)
        two_tags = subject.EquivalenceBase(release_info["two-tags"])
        extra_tag = subject.EquivalenceBase(release_info["extra-tag"])
        cvo_replaced = subject.EquivalenceBase(release_info["cvo-tag-replaced-by-pod"])
        diff_pod = subject.EquivalenceBase(release_info["two-tag-different-pod"])

        self.assertEqual(two_tags, extra_tag)
        self.assertEqual(two_tags, cvo_replaced)
        self.assertNotEqual(two_tags, diff_pod)

    @patch('doozerlib.cli.get_nightlies.EquivalenceBase.retrieve_rhcos_tag_digest')
    def test_equivalence_base_rhcos(self, mock_digest):
        test_data = json.loads("""
            {
                "references": {
                  "spec": {
                    "tags": [
                        {
                          "name": "some-rhcos-tag",
                          "annotations": { "io.openshift.build.commit.id": "" },
                          "from": { "name": "mocked-out" }
                        },
                        {
                          "name": "pod",
                          "annotations": { "io.openshift.build.commit.id": "pod-commit" },
                          "from": { "name": "ignored" }
                        }
                    ]
                }}
            }
        """)
        rhcos1 = subject.EquivalenceBase(test_data)
        rhcos2 = subject.EquivalenceBase(test_data)
        mock_digest.side_effect = ["digest1", "digest1"]
        self.assertEqual(rhcos1, rhcos2)

        rhcos1 = subject.EquivalenceBase(test_data)
        rhcos2 = subject.EquivalenceBase(test_data)
        mock_digest.side_effect = ["digest1", "digest2"]
        self.assertNotEqual(rhcos1, rhcos2)

    @staticmethod
    def vanilla_eq_base():
        # just give me an instance to test (supplies "pod" entry)
        return subject.EquivalenceBase({
            "references": {"spec": {"tags": [
                {
                    "name": "pod",
                    "annotations": {"io.openshift.build.commit.id": "pod-commit"},
                    "from": {"name": "pod-pullspec"},
                }
            ]}}})

    @patch('doozerlib.cli.get_nightlies.EquivalenceBase.retrieve_image_info')
    def test_retrieve_nvr_for_tag(self, mock_rii):
        eq = self.vanilla_eq_base()
        mock_rii.return_value = Model(dict(config=dict(config=dict(Labels={
            "com.redhat.component": "spam",
            "version": "1.0",
            "release": "1.el8",
        }))))
        self.assertEqual(("spam", "1.0", "1.el8"), eq.retrieve_nvr_for_tag("pod"))

        mock_rii.return_value = Exception()  # should be cached from last call
        self.assertEqual(("spam", "1.0", "1.el8"), eq.retrieve_nvr_for_tag("pod"))

        mock_rii.return_value = Model()  # no labels provided
        eq.pullspec_for_tag["rhcos"] = "rhcos_ps"
        self.assertIsNone(eq.retrieve_nvr_for_tag("rhcos"))

    def test_deeper_equivalence(self):
        eq1 = self.vanilla_eq_base()
        eq2 = self.vanilla_eq_base()
        eq1.rhcos_inspector = eq2.rhcos_inspector = MagicMock()  # always match
        eq1.commit_for_tag["pod"] = eq2.commit_for_tag["pod"] = "commit1"
        eq1.nvr_for_tag["pod"] = eq2.nvr_for_tag["pod"] = ("nvr", "1", "1")
        self.assertTrue(eq1.deeper_equivalence(eq2))

        # works with missing entries too
        eq1.commit_for_tag["missing1"] = eq2.commit_for_tag["missing2"] = "mcommit"
        self.assertTrue(eq1.deeper_equivalence(eq2), "un-shared tags are ignored")
        self.assertTrue(eq2.deeper_equivalence(eq1), "... in both directions")

        # get a failure
        eq2.nvr_for_tag["pod"] = ("nvr", "2", "2")
        self.assertFalse(eq1.deeper_equivalence(eq2))

        # give alt images (where components differ for the same tag) a pass.
        # most of the time they'll have the same VR but that's not absolutely
        # guaranteed (one build could flake then succeed with later R).
        # so just rely on source commit equivalence (already verified)
        # and ignore the slim possibility that the RPMs installed differ.
        eq2.nvr_for_tag["pod"] = ("nvr-alt", "1", "1")
        self.assertTrue(eq1.deeper_equivalence(eq2), "alt images allowed to differ")

    def test_deeper_equivalence_rhcos(self):
        eq1 = self.vanilla_eq_base()
        eq2 = self.vanilla_eq_base()
        ri1 = eq1.rhcos_inspector = MagicMock()
        ri2 = eq2.rhcos_inspector = MagicMock()
        for eq in (eq1, eq2):
            eq.rhcos_inspector.get_os_metadata_rpm_list.return_value = [("spam", 0, 1, 1, "noarch")]

        self.assertTrue(eq1.deeper_equivalence_rhcos(eq2), "same RPM content")

        ri2.get_os_metadata_rpm_list.return_value = [("eggs", 0, 2, 3, "noarch")]
        self.assertTrue(eq1.deeper_equivalence_rhcos(eq2), "unchecked RPM content")
        self.assertTrue(eq2.deeper_equivalence_rhcos(eq1), "unmatched RPM content")

        ri1.get_os_metadata_rpm_list.return_value = [("sausage", 0, 1, 1, "noarch")]
        ri2.get_os_metadata_rpm_list.return_value = [("sausage", 0, 2, 3, "noarch")]
        self.assertFalse(eq1.deeper_equivalence_rhcos(eq2), "mismatched RPM content")

    @patch('doozerlib.exectools.cmd_assert')
    def test_retrieve_rhcos_tag_digest(self, mock_image_info):
        test_data = json.loads("""
            {
                "references": {
                  "spec": {
                    "tags": [
                        {
                          "name": "some-rhcos-tag",
                          "annotations": { "io.openshift.build.commit.id": "" },
                          "from": { "name": "mocked-out" }
                        },
                        {
                          "name": "other-rhcos-tag",
                          "annotations": { "io.openshift.build.commit.id": "" },
                          "from": { "name": "mocked-out-2" }
                        },
                        {
                          "name": "ya-rhcos-tag",
                          "annotations": { "io.openshift.build.commit.id": "" },
                          "from": { "name": "mocked-out-3" }
                        },
                        {
                          "name": "pod",
                          "annotations": { "io.openshift.build.commit.id": "pod-commit" },
                          "from": { "name": "ignored" }
                        }
                    ]
                }}
            }
        """)
        test_image_info1 = json.dumps(dict(config=dict(config=dict(Labels={"com.coreos.rpm.foo": "foo-1.0"}))))
        test_image_info2 = json.dumps(dict(config=dict(config=dict(Labels={"com.coreos.rpm.foo": "foo-2.0"}))))
        test_image_info3 = json.dumps(dict(config=dict(config=dict(Labels={"com.coreos.rpm.foo": "foo-1.0", "com.coreos.rpm.kernel-rt": "kernel-1.0"}))))

        rhcos = subject.EquivalenceBase(test_data)
        mock_image_info.side_effect = [(test_image_info1, None), (test_image_info2, None)]
        self.assertNotEqual(
            rhcos.retrieve_rhcos_tag_digest("some-rhcos-tag"),
            rhcos.retrieve_rhcos_tag_digest("other-rhcos-tag")
        )

        mock_image_info.side_effect = [(test_image_info1, None), (test_image_info3, None)]
        self.assertEqual(
            rhcos.retrieve_rhcos_tag_digest("some-rhcos-tag"),
            rhcos.retrieve_rhcos_tag_digest("ya-rhcos-tag")
        )

        rhcos = subject.EquivalenceBase(test_data)
        # these will be retrieved from cache
        mock_image_info.side_effect = Exception("should not get here")
        self.assertNotEqual(
            rhcos.retrieve_rhcos_tag_digest("other-rhcos-tag"),
            rhcos.retrieve_rhcos_tag_digest("ya-rhcos-tag"),
            "image infos should be cached"
        )

    def test_equivalence_set(self):
        nightlies = [
            {
                "name": "4.12.0-0.nightly-2022-07-15-132344",
                "phase": "Ready",
                "pullSpec": "registry.ci.openshift.org/ocp/release:4.12.0-0.nightly-2022-07-15-132344",
                "downloadURL": "https://openshift-release-artifacts.apps.ci.l2s4.p1.openshiftapps.com/4.12.0-0.nightly-2022-07-15-132344",
                "equivalence": "digest1",  # represents the EquivalenceBase determined for this nightly
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
        eqset = subject.EquivalenceSet({"x86_64": nightlies[0]})
        self.assertIsNone(eqset.augment("s390x", nightlies[1]))

        set1 = eqset.augment("aarch64", nightlies[2])
        self.assertIsInstance(set1, subject.EquivalenceSet)
        self.assertEqual(set1.timestamp, "2022-07-18")  # greater of first and third

        self.assertIsNone(set1.augment("s390x", nightlies[1]))
        with self.assertRaises(subject.EqSetDuplicateArchException):
            eqset.augment("x86_64", nightlies[0])

    def test_generate_equivalence_sets(self):
        nightlies_for_arch = {
            "x86_64": [
                {
                    "name": "nightly1",
                    "equivalence": "digest1",  # represents the EquivalenceBase for this nightly
                    "releaseInfo": {"config": {"created": "2022-07-17"}},
                },
                {
                    "name": "nightly2",
                    "equivalence": "digest1",
                    "releaseInfo": {"config": {"created": "2022-07-18"}},
                },
                {
                    "name": "nightly3",
                    "equivalence": "digest2",
                    "releaseInfo": {"config": {"created": "2022-07-16"}},
                },
            ],
            "s390x": [
                {
                    "name": "nightly4",
                    "equivalence": "digest1",  # matches two
                    "releaseInfo": {"config": {"created": "2022-07-17"}},
                },
                {
                    "name": "nightly5",
                    "equivalence": "digest2",  # matches one
                    "releaseInfo": {"config": {"created": "2022-07-15"}},
                },
            ],
        }
        sets = subject.generate_equivalence_sets(nightlies_for_arch)
        self.assertEqual(3, len(sets))
        # also check that they sort by timestamp desc
        self.assertEqual("2022-07-18", sets[0].timestamp)
        self.assertEqual("2022-07-16", sets[2].timestamp)

        # check that an incompatible nightly torpedoes set creation
        nightlies_for_arch["ppc64le"] = [
            {
                "name": "nightly6",
                "equivalence": "digest3",  # incompatible with all
                "releaseInfo": {"config": {"created": "2022-07-17"}},
            },
        ]
        self.assertEqual(0, len(subject.generate_equivalence_sets(nightlies_for_arch)))
