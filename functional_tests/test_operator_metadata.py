from __future__ import absolute_import, print_function, unicode_literals
import unittest
import io
import os
import pygit2
import yaml
import tempfile
import koji
from . import run_doozer, get_working_dir, BREW_HUB


class TestOperatorMetadata(unittest.TestCase):
    def test_operator_metadata_latest_build(self):
        brew = koji.ClientSession(BREW_HUB)
        build = brew.getLatestBuilds("rhaos-4.2-rhel-7-candidate", package="cluster-logging-operator-container")[0]

        _, out, _ = run_doozer([
            "--group=openshift-4.2",
            "--images=cluster-logging-operator",
            "operator-metadata:latest-build",
            "--stream=dev",
            "--nvr",
            build["nvr"],
        ])
        self.assertIn("cluster-logging-operator-metadata-container-", out)

    def test_operator_metadata_build(self):
        # FIXME: operator_metadata:build is not tested because --dry-run mode is not supported
        pass
