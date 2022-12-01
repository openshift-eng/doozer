from unittest import TestCase
from unittest.mock import MagicMock, Mock, patch

from flexmock import flexmock

import doozerlib
from doozerlib.assembly import AssemblyIssueCode, AssemblyTypes, AssemblyIssue
from doozerlib.cli.release_gen_assembly import GenAssemblyCli


class TestGenPayloadCli(TestCase):

    def test_initialize_assembly_type(self):
        runtime = MagicMock()

        gacli = GenAssemblyCli(runtime=runtime, custom=True)
        self.assertEqual(gacli.assembly_type, AssemblyTypes.CUSTOM)

        gacli = GenAssemblyCli(runtime=runtime, gen_assembly_name='rc.0')
        self.assertEqual(gacli.assembly_type, AssemblyTypes.CANDIDATE)

        gacli = GenAssemblyCli(runtime=runtime, gen_assembly_name='fc.0')
        self.assertEqual(gacli.assembly_type, AssemblyTypes.CANDIDATE)

        gacli = GenAssemblyCli(runtime=runtime, gen_assembly_name='ec.0')
        self.assertEqual(gacli.assembly_type, AssemblyTypes.PREVIEW)

        gacli = GenAssemblyCli(runtime=runtime, gen_assembly_name='just-a-name')
        self.assertEqual(gacli.assembly_type, AssemblyTypes.STANDARD)

    @patch('doozerlib.cli.release_gen_assembly.GenAssemblyCli._exit_with_error', MagicMock(return_value=None))
    def test_non_stream_assembly_name(self):
        gacli = flexmock(GenAssemblyCli(
            runtime=MagicMock(assembly='custom', arches=['amd64']),
            nightlies=['4.13.0-0.nightly-2022-12-01-153811']
        ))
        gacli.should_receive('_exit_with_error').once()
        gacli._validate_params()

    @patch('doozerlib.cli.release_gen_assembly.GenAssemblyCli._exit_with_error', MagicMock(return_value=None))
    def test_no_nightlies_nor_standards(self):
        gacli = flexmock(GenAssemblyCli(
            runtime=MagicMock(assembly='stream')
        ))
        gacli.should_receive('_exit_with_error').once()
        gacli._validate_params()

    @patch('doozerlib.cli.release_gen_assembly.GenAssemblyCli._exit_with_error', MagicMock(return_value=None))
    def test_arches_nightlies_mismatchs(self):
        gacli = flexmock(GenAssemblyCli(
            runtime=MagicMock(assembly='stream', arches=['amd64', 's390x']),
            nightlies=['4.13.0-0.nightly-2022-12-01-153811']
        ))
        gacli.should_receive('_exit_with_error').once()
        gacli._validate_params()

    @patch('doozerlib.cli.release_gen_assembly.GenAssemblyCli._exit_with_error', MagicMock(return_value=None))
    def test_previous_and_auto_previous(self):
        gacli = flexmock(GenAssemblyCli(
            runtime=MagicMock(assembly='stream', arches=['amd64']),
            nightlies=['4.13.0-0.nightly-2022-12-01-153811'],
            auto_previous=True,
            previous_list='4.y.z'
        ))
        gacli.should_receive('_exit_with_error').once()
        gacli._validate_params()

    @patch('doozerlib.cli.release_gen_assembly.GenAssemblyCli._exit_with_error', MagicMock(return_value=None))
    def test_custom_assembly(self):
        gacli = flexmock(GenAssemblyCli(
            runtime=MagicMock(assembly='stream', arches=['amd64']),
            nightlies=['4.13.0-0.nightly-2022-12-01-153811'],
            custom=True,
            auto_previous=True
        ))
        gacli.should_receive('_exit_with_error').once()
        gacli._validate_params()

        gacli = flexmock(GenAssemblyCli(
            runtime=MagicMock(assembly='stream', arches=['amd64']),
            nightlies=['4.13.0-0.nightly-2022-12-01-153811'],
            custom=True,
            previous_list='4.y.z'
        ))
        gacli.should_receive('_exit_with_error').once()
        gacli._validate_params()

        gacli = flexmock(GenAssemblyCli(
            runtime=MagicMock(assembly='stream', arches=['amd64']),
            nightlies=['4.13.0-0.nightly-2022-12-01-153811'],
            custom=True,
            in_flight=True
        ))
        gacli.should_receive('_exit_with_error').once()
        gacli._validate_params()

    @patch('doozerlib.cli.release_gen_assembly.GenAssemblyCli._exit_with_error', MagicMock(return_value=None))
    def test_group_nightly_mismatch(self):
        runtime = MagicMock()
        runtime.get_minor_version.return_value = '4.12'
        gacli = flexmock(GenAssemblyCli(
            runtime=runtime,
            nightlies=['4.13.0-0.nightly-2022-12-01-153811'],
        ))
        gacli.should_receive('_exit_with_error').once()
        gacli._get_release_pullspecs()

    def test_nightly_release_pullspecs(self):
        runtime = MagicMock()
        runtime.get_minor_version.return_value = '4.13'
        gacli = GenAssemblyCli(
            runtime=runtime,
            nightlies=['4.13.0-0.nightly-2022-12-01-153811'],
        )
        gacli._get_release_pullspecs()
        self.assertEqual(
            gacli.release_pullspecs,
            {'x86_64': 'registry.ci.openshift.org/ocp/release:4.13.0-0.nightly-2022-12-01-153811'}
        )
        self.assertEqual(
            gacli.reference_releases_by_arch['x86_64'],
            '4.13.0-0.nightly-2022-12-01-153811'
        )

    def test_multi_nighly_arch(self):
        runtime = MagicMock()
        runtime.get_minor_version.return_value = '4.13'
        gacli = GenAssemblyCli(
            runtime=runtime,
            nightlies=[
                '4.13.0-0.nightly-2022-12-01-153811',
                '4.13.0-0.nightly-2022-12-01-140621'
            ],
        )
        with self.assertRaises(ValueError):
            gacli._get_release_pullspecs()

    @patch('doozerlib.cli.release_gen_assembly.GenAssemblyCli._exit_with_error', MagicMock(return_value=None))
    def test_group_standard_mismatch(self):
        runtime = MagicMock()
        runtime.get_minor_version.return_value = '4.12'
        gacli = flexmock(GenAssemblyCli(
            runtime=runtime,
            standards=['4.11.18-x86_64'],
        ))
        gacli.should_receive('_exit_with_error').once()
        gacli._get_release_pullspecs()

    def test_standard_release_pullspecs(self):
        runtime = MagicMock()
        runtime.get_minor_version.return_value = '4.11'
        gacli = GenAssemblyCli(
            runtime=runtime,
            standards=['4.11.18-x86_64'],
        )
        gacli._get_release_pullspecs()
        self.assertEqual(
            gacli.release_pullspecs,
            {'x86_64': 'quay.io/openshift-release-dev/ocp-release:4.11.18-x86_64'}
        )
        self.assertEqual(
            gacli.reference_releases_by_arch,
            {}
        )

    def test_multi_standard_arch(self):
        runtime = MagicMock()
        runtime.get_minor_version.return_value = '4.11'
        gacli = GenAssemblyCli(
            runtime=runtime,
            standards=[
                '4.11.18-x86_64',
                '4.11.19-x86_64'
            ],
        )
        with self.assertRaises(ValueError):
            gacli._get_release_pullspecs()
