
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock
from doozerlib.assembly import AssemblyTypes

from doozerlib.assembly_inspector import AssemblyInspector
from doozerlib.model import Model


class TestAssemblyInspector(IsolatedAsyncioTestCase):
    def test_check_installed_packages_for_rpm_delivery(self):
        # Test a standard release with a package tagged into my-ship-ok-tag
        rt = MagicMock(mode="both", group_config=Model({
            "rpm_deliveries": [
                {
                    "packages": ["kernel", "kernel-rt"],
                    "integration_tag": "my-integration-tag",
                    "ship_ok_tag": "my-ship-ok-tag",
                    "stop_ship_tag": "my-stop-ship-tag",
                    "target_tag": "my-target-tag",
                }
            ]
        }))
        brew_session = MagicMock()
        brew_session.listTags.return_value = [
            {"name": "tag-a"},
            {"name": "my-integration-tag"},
            {"name": "my-ship-ok-tag"},
        ]
        ai = AssemblyInspector(rt, brew_session)
        ai.assembly_type = AssemblyTypes.STANDARD
        rpm_packages = {
            "kernel": {"nvr": "kernel-1.2.3-1", "id": 1}
        }
        issues = ai._check_installed_packages_for_rpm_delivery("foo", "foo-1.2.3-1", rpm_packages)
        self.assertEqual(issues, [])

        # Test a standard release with a package not tagged into my-ship-ok-tag
        brew_session.listTags.return_value = [
            {"name": "tag-a"},
            {"name": "my-integration-tag"},
        ]
        issues = ai._check_installed_packages_for_rpm_delivery("foo", "foo-1.2.3-1", rpm_packages)
        self.assertEqual(len(issues), 1)

        # Test a stream "release" with a package not tagged into my-ship-ok-tag
        ai.assembly_type = AssemblyTypes.STREAM
        issues = ai._check_installed_packages_for_rpm_delivery("foo", "foo-1.2.3-1", rpm_packages)
        self.assertEqual(len(issues), 0)

        # Test a stream "release" with a package not tagged into my-stop-ship-tag
        brew_session.listTags.return_value = [
            {"name": "tag-a"},
            {"name": "my-integration-tag"},
            {"name": "my-ship-ok-tag"},
            {"name": "my-stop-ship-tag"},
        ]
        issues = ai._check_installed_packages_for_rpm_delivery("foo", "foo-1.2.3-1", rpm_packages)
        self.assertEqual(len(issues), 1)

    def test_check_installed_rpms_in_image(self):
        rt = MagicMock(mode="both", group_config=Model({
            "rpm_deliveries": [
                {
                    "packages": ["kernel", "kernel-rt"],
                    "integration_tag": "my-integration-tag",
                    "ship_ok_tag": "my-ship-ok-tag",
                    "stop_ship_tag": "my-stop-ship-tag",
                    "target_tag": "my-target-tag",
                }
            ]
        }))
        brew_session = MagicMock()
        brew_session.listTags.return_value = [
            {"name": "tag-a"},
            {"name": "my-integration-tag"},
            {"name": "my-ship-ok-tag"},
        ]
        ai = AssemblyInspector(rt, brew_session)
        ai.assembly_type = AssemblyTypes.STANDARD
        build_inspector = MagicMock()
        build_inspector.get_all_installed_package_build_dicts.return_value = {
            "kernel": {"nvr": "kernel-1.2.3-1", "id": 1}
        }
        issues = ai.check_installed_rpms_in_image("foo", build_inspector)
        self.assertEqual(issues, [])
    pass
