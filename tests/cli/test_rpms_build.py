import asyncio
import logging
from unittest import TestCase

from mock import AsyncMock, MagicMock, Mock, patch

from doozerlib import gitdata, rpmcfg
from doozerlib.cli.rpms_build import _rpms_rebase_and_build
from doozerlib.exectools import RetryException


class TestRPMsBuildCli(TestCase):
    @patch("doozerlib.cli.rpms_build.RPMBuilder")
    def test_rpms_build_success(self, MockedRPMBuilder: Mock):
        runtime = MagicMock(local=False)
        runtime.assert_mutation_is_permitted = MagicMock()
        version = "v1.2.3"
        release = "202104070000.yuxzhu.test.p?"
        embargoed = True
        scratch = False
        dry_run = False
        rpms = []

        data_obj = gitdata.DataObj("foo", "/path/to/ocp-build-data/rpms/foo.yml", {
            "name": "foo",
            "content": {
                "source": {
                    "git": {"url": "git@github.com:openshift-priv/foo.git", "branch": {"target": "release-4.8"}},
                    "specfile": "foo.spec",
                    "modifications": [
                        {"action": "add", "command": ["my-command", "--my-arg"]}
                    ],
                }
            },
            "targets": ["rhaos-4.4-rhel-8-candidate", "rhaos-4.4-rhel-7-candidate"],
        })
        rpm = rpmcfg.RPMMetadata(runtime, data_obj, clone_source=False)
        rpm.distgit_repo = MagicMock(branch="rhaos-4.4-rhel-8")
        rpm.logger = MagicMock(spec=logging.Logger)
        rpms.append(rpm)

        data_obj = gitdata.DataObj("bar", "/path/to/ocp-build-data/rpms/bar.yml", {
            "name": "bar",
            "content": {
                "source": {
                    "git": {"url": "git@github.com:openshift-priv/bar.git", "branch": {"target": "release-4.8"}},
                    "specfile": "bar.spec",
                }
            },
            "targets": ["rhaos-4.4-rhel-8-candidate"],
        })
        rpm = rpmcfg.RPMMetadata(runtime, data_obj, clone_source=False)
        rpm.distgit_repo = MagicMock(branch="rhaos-4.4-rhel-8")
        rpm.logger = MagicMock(spec=logging.Logger)
        rpms.append(rpm)

        runtime.rpm_metas.return_value = rpms
        builder = MockedRPMBuilder.return_value = AsyncMock()
        builder.build.return_value = ([10001, 10002], ["https://brewweb.example.com/brew/taskinfo?taskID=10001", "https://brewweb.example.com/brew/taskinfo?taskID=10002"], ["foo-1.2.3-1.el8", "foo-1.2.3-1.el7"])

        result = asyncio.get_event_loop().run_until_complete(_rpms_rebase_and_build(runtime, version, release, embargoed, scratch, dry_run))

        self.assertEqual(result, 0)

    @patch("doozerlib.cli.rpms_build.RPMBuilder")
    def test_rpms_build_failure(self, MockedRPMBuilder: Mock):
        runtime = MagicMock(local=False)
        runtime.assert_mutation_is_permitted = MagicMock()
        version = "v1.2.3"
        release = "202104070000.yuxzhu.test.p?"
        embargoed = True
        scratch = False
        dry_run = False
        rpms = []

        data_obj = gitdata.DataObj("foo", "/path/to/ocp-build-data/rpms/foo.yml", {
            "name": "foo",
            "content": {
                "source": {
                    "git": {"url": "git@github.com:openshift-priv/foo.git", "branch": {"target": "release-4.8"}},
                    "specfile": "foo.spec",
                    "modifications": [
                        {"action": "add", "command": ["my-command", "--my-arg"]}
                    ],
                }
            },
            "targets": ["rhaos-4.4-rhel-8-candidate", "rhaos-4.4-rhel-7-candidate"],
        })
        rpm = rpmcfg.RPMMetadata(runtime, data_obj, clone_source=False)
        rpm.logger = MagicMock(spec=logging.Logger)
        rpm.distgit_repo = MagicMock(branch="rhaos-4.4-rhel-8")
        rpms.append(rpm)

        data_obj = gitdata.DataObj("bar", "/path/to/ocp-build-data/rpms/bar.yml", {
            "name": "bar",
            "content": {
                "source": {
                    "git": {"url": "git@github.com:openshift-priv/bar.git", "branch": {"target": "release-4.8"}},
                    "specfile": "bar.spec",
                }
            },
            "targets": ["rhaos-4.4-rhel-8-candidate"],
        })
        rpm = rpmcfg.RPMMetadata(runtime, data_obj, clone_source=False)
        rpm.logger = MagicMock(spec=logging.Logger)
        rpm.distgit_repo = MagicMock(branch="rhaos-4.4-rhel-8")
        rpms.append(rpm)

        runtime.rpm_metas.return_value = rpms
        builder = MockedRPMBuilder.return_value = AsyncMock()
        builder.side_effect = RetryException("Retry error", ([10001, 10002], ["https://brewweb.example.com/brew/taskinfo?taskID=10001", "https://brewweb.example.com/brew/taskinfo?taskID=10002"]))

        result = asyncio.get_event_loop().run_until_complete(_rpms_rebase_and_build(runtime, version, release, embargoed, scratch, dry_run))

        self.assertEqual(result, 1)
