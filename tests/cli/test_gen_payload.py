from unittest import TestCase
from unittest.mock import MagicMock, patch

import yaml

from doozerlib.assembly import AssemblyIssueCode
from doozerlib.cli import release_gen_payload as rgp_cli
from doozerlib.model import Model
from doozerlib import rhcos


class TestGenPayloadCli(TestCase):

    def test_find_rhcos_payload_entries(self):
        rhcos_build = MagicMock()
        assembly_inspector = MagicMock()
        assembly_inspector.get_rhcos_build.return_value = rhcos_build
        rhcos_build.get_container_configs.return_value = [
            Model(dict(name="spam", build_metadata_tag="eggs", primary=True)),
            Model(dict(name="foo", build_metadata_tag="bar")),
        ]

        # test when a primary container is missing from rhcos build
        rhcos_build.get_container_pullspec.side_effect = [
            rhcos.RhcosMissingContainerException("primary missing"),
            "somereg/somerepo@sha256:somesum",
        ]
        rhcos_entries, issues = rgp_cli.PayloadGenerator._find_rhcos_payload_entries(assembly_inspector, "arch")
        self.assertNotIn("spam", rhcos_entries)
        self.assertIn("foo", rhcos_entries)
        self.assertEqual(issues[0].code, AssemblyIssueCode.IMPERMISSIBLE)

        # test when a non-primary container is missing from rhcos build
        rhcos_build.get_container_pullspec.side_effect = [
            "somereg/somerepo@sha256:somesum",
            rhcos.RhcosMissingContainerException("non-primary missing"),
        ]
        rhcos_entries, issues = rgp_cli.PayloadGenerator._find_rhcos_payload_entries(assembly_inspector, "arch")
        self.assertIn("spam", rhcos_entries)
        self.assertNotIn("foo", rhcos_entries)
        self.assertEqual(issues[0].code, AssemblyIssueCode.MISSING_RHCOS_CONTAINER)

        # test when no container is missing from rhcos build
        rhcos_build.get_container_pullspec.side_effect = [
            "somereg/somerepo@sha256:somesum",
            "somereg/somerepo@sha256:someothersum",
        ]
        rhcos_entries, issues = rgp_cli.PayloadGenerator._find_rhcos_payload_entries(assembly_inspector, "arch")
        self.assertEqual([], issues)
        self.assertEqual(2, len(rhcos_entries))
