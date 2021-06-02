from unittest import TestCase

import re
import datetime

from mock import MagicMock, Mock

from doozerlib.metadata import Metadata, CgitAtomFeedEntry, RebuildHintCode
from doozerlib.brew import BuildStates
from doozerlib.model import Model, Missing
from doozerlib import exectools


class TestMetadata(TestCase):

    def setUp(self) -> None:
        data_obj = MagicMock(key="foo", filename="foo.yml", data={"name": "foo"})
        runtime = MagicMock()
        runtime.group_config.urls.cgit = "http://distgit.example.com/cgit"

        koji_mock = Mock()
        koji_mock.__enter__ = Mock()
        koji_mock.__enter__.return_value = koji_mock
        koji_mock.__exit__ = Mock()

        runtime.pooled_koji_client_session = Mock()
        runtime.pooled_koji_client_session.return_value = koji_mock

        self.package_id = 5
        koji_mock.getPackage = Mock(return_value={'name': 'foo-container', 'id': self.package_id})

        runtime.assembly = 'hotfix_a'
        meta = Metadata("image", runtime, data_obj)

        meta.get_component_name = Mock(return_value='foo-container')
        meta.branch_major_minor = Mock(return_value='4.7')

        self.runtime = runtime
        self.meta = meta
        self.koji_mock = koji_mock

    def test_cgit_url(self):
        data_obj = MagicMock(key="foo", filename="foo.yml", data={"name": "foo"})
        runtime = MagicMock()
        runtime.group_config.urls.cgit = "http://distgit.example.com/cgit"
        meta = Metadata("image", runtime, data_obj)
        url = meta.cgit_file_url("some_path/some_file.txt", "abcdefg", "some-branch")
        self.assertEqual(url, "http://distgit.example.com/cgit/containers/foo/plain/some_path/some_file.txt?h=some-branch&id=abcdefg")

    def build_record(self, creation_dt: datetime.datetime, assembly, name='foo-container',
                     version='4.7.0', p='p0', epoch=None, git_commit='4c0ed6d',
                     release_prefix=None, release_suffix='',
                     build_state: BuildStates = BuildStates.COMPLETE):
        """
        :return: Returns an artificial brew build record.
        """
        if not release_prefix:
            release_prefix = creation_dt.strftime('%Y%m%d%H%M%S')

        release = release_prefix

        if p:
            release += f'.{p}'

        if git_commit:
            release += f'.git.{git_commit[:7]}'

        if assembly is not None:
            release += f'.assembly.{assembly}{release_suffix}'

        return {
            'name': name,
            'package_name': name,
            'version': version,
            'release': release,
            'epoch': epoch,
            'nvr': f'{name}-v{version}-{release}',
            'build_id': creation_dt.timestamp(),
            'creation_event_id': creation_dt.timestamp(),
            'creation_ts': creation_dt.timestamp(),
            'creation_time': creation_dt.isoformat(),
            'state': build_state.value,
            'package_id': self.package_id,
        }

    def _list_builds(self, builds, packageID=None, state=None, pattern=None, queryOpts=None):
        """
        A simple replacement of koji's listBuilds API. The vital input to this
        the `builds` variable. It will be filtered based on
        some of the parameters passed to this method.
        """
        pattern_regex = re.compile(r'.*')
        if pattern:
            regex = pattern.replace('.', "\\.")
            regex = regex.replace('*', '.*')
            pattern_regex = re.compile(regex)

        refined = list(builds)
        refined = [build for build in refined if pattern_regex.match(build['nvr'])]

        if packageID is not None:
            refined = [build for build in refined if build['package_id'] == packageID]

        if state is not None:
            refined = [build for build in refined if build['state'] == state]

        refined.sort(key=lambda e: e['creation_ts'], reverse=True)
        return refined

    def test_get_latest_build(self):
        runtime = self.runtime
        meta = self.meta
        koji_mock = self.koji_mock
        now = datetime.datetime.utcnow()

        def list_builds(packageID=None, state=None, pattern=None, queryOpts=None):
            return self._list_builds(builds, packageID=packageID, state=state, pattern=pattern, queryOpts=queryOpts)

        koji_mock.listBuilds.side_effect = list_builds

        # If listBuilds returns nothing, no build should be returned
        builds = []
        self.assertIsNone(meta.get_latest_build(default=None))

        # If listBuilds returns a build from an assembly that is not ours
        # get_latest_builds should not return it.
        builds = [
            self.build_record(now, assembly='not_ours')
        ]
        self.assertIsNone(meta.get_latest_build(default=None))

        # If there is a build from the 'stream' assembly, it shold be
        # returned.
        builds = [
            self.build_record(now, assembly='not_ours'),
            self.build_record(now, assembly='stream')
        ]
        self.assertEqual(meta.get_latest_build(default=None), builds[1])

        # If there is a build for our assembly, it should be returned
        builds = [
            self.build_record(now, assembly=runtime.assembly)
        ]
        self.assertEqual(meta.get_latest_build(default=None), builds[0])

        # If there is a build for our assembly and stream, our assembly
        # should be preferred even if stream is more recent.
        builds = [
            self.build_record(now - datetime.timedelta(hours=5), assembly='stream'),
            self.build_record(now, assembly='not_ours'),
            self.build_record(now, assembly=runtime.assembly)
        ]
        self.assertEqual(meta.get_latest_build(default=None), builds[2])

        # The most recent assembly build should be preferred.
        builds = [
            self.build_record(now - datetime.timedelta(hours=5), assembly='stream'),
            self.build_record(now - datetime.timedelta(hours=5), assembly=runtime.assembly),
            self.build_record(now, assembly='not_ours'),
            self.build_record(now, assembly=runtime.assembly)
        ]
        self.assertEqual(meta.get_latest_build(default=None), builds[3])

        # Make sure that just matching the prefix of an assembly is not sufficient.
        builds = [
            self.build_record(now - datetime.timedelta(hours=5), assembly='stream'),
            self.build_record(now - datetime.timedelta(hours=5), assembly=runtime.assembly),
            self.build_record(now, assembly='not_ours'),
            self.build_record(now, assembly=f'{runtime.assembly}b')
        ]
        self.assertEqual(meta.get_latest_build(default=None), builds[1])

        # But, a proper suffix like '.el8' should still match.
        builds = [
            self.build_record(now - datetime.timedelta(hours=5), assembly='stream'),
            self.build_record(now - datetime.timedelta(hours=5), assembly=runtime.assembly),
            self.build_record(now, assembly='not_ours'),
            self.build_record(now, assembly=f'{runtime.assembly}', release_suffix='.el8')
        ]
        self.assertEqual(meta.get_latest_build(default=None), builds[3])

        # By default, we should only be finding COMPLETE builds
        builds = [
            self.build_record(now - datetime.timedelta(hours=5), assembly='stream', build_state=BuildStates.COMPLETE),
            self.build_record(now, assembly='stream', build_state=BuildStates.FAILED),
        ]
        self.assertEqual(meta.get_latest_build(default=None), builds[0])

        # By default, we should only be finding COMPLETE builds
        builds = [
            self.build_record(now - datetime.timedelta(hours=5), assembly=None, build_state=BuildStates.COMPLETE),
            self.build_record(now, assembly=None, build_state=BuildStates.FAILED),
            self.build_record(now, assembly=None, build_state=BuildStates.COMPLETE),
        ]
        self.assertEqual(meta.get_latest_build(default=None, assembly=''), builds[2])

        # Check whether extra pattern matching works
        builds = [
            self.build_record(now - datetime.timedelta(hours=5), assembly='stream'),
            self.build_record(now - datetime.timedelta(hours=25), assembly='stream', release_prefix='99999.git.1234567', release_suffix='.el8'),
            self.build_record(now - datetime.timedelta(hours=5), assembly=runtime.assembly),
            self.build_record(now, assembly='not_ours'),
            self.build_record(now - datetime.timedelta(hours=8), assembly=f'{runtime.assembly}')
        ]
        self.assertEqual(meta.get_latest_build(default=None, extra_pattern='*.git.1234567.*'), builds[1])

    def test_needs_rebuild_disgit_only(self):
        runtime = self.runtime
        meta = self.meta
        koji_mock = self.koji_mock
        now = datetime.datetime.utcnow()
        then = now - datetime.timedelta(hours=5)

        def list_builds(packageID=None, state=None, pattern=None, queryOpts=None):
            return self._list_builds(builds, packageID=packageID, state=state, pattern=pattern, queryOpts=queryOpts)

        runtime.downstream_commitish_overrides = {}
        koji_mock.listBuilds.side_effect = list_builds
        meta.has_source = Mock(return_value=False)  # Emulate a distgit-only repo

        # If listBuilds returns nothing, we want to trigger a rebuild
        builds = []
        self.assertEqual(meta.needs_rebuild().code, RebuildHintCode.NO_LATEST_BUILD)

        meta.cgit_atom_feed = Mock()

        # If we find that that the most recent distgit commit is newer than the build,
        # we must rebuild.
        meta.cgit_atom_feed.return_value = [
            CgitAtomFeedEntry(title='', content='', updated=now, id='1234567')
        ]
        builds = [
            self.build_record(then, assembly=runtime.assembly, git_commit=None)
        ]
        self.assertEqual(meta.needs_rebuild().code, RebuildHintCode.DISTGIT_ONLY_COMMIT_NEWER)

        # If we find that that the most recent distgit commit is newer than the build,
        # we must rebuild. If there was a recent failed build, delay retrying to not
        # constantly flood brew.
        meta.cgit_atom_feed.return_value = [
            CgitAtomFeedEntry(title='', content='', updated=now, id='1234567')
        ]
        builds = [
            self.build_record(now - datetime.timedelta(minutes=5), assembly=runtime.assembly,
                              git_commit=None, build_state=BuildStates.FAILED),
            self.build_record(then, assembly=runtime.assembly,
                              git_commit=None)
        ]
        self.assertEqual(meta.needs_rebuild().code, RebuildHintCode.DELAYING_NEXT_ATTEMPT)

        # If we find that that the most recent distgit commit is OLDER than the
        # current build, no rebuild is necessary
        meta.cgit_atom_feed.return_value = [
            CgitAtomFeedEntry(title='', content='', updated=then, id='1234567')
        ]
        builds = [
            self.build_record(now, assembly=runtime.assembly, git_commit=None)
        ]
        self.assertEqual(meta.needs_rebuild().code, RebuildHintCode.DISTGIT_ONLY_COMMIT_OLDER)

        # Sanity check that failed builds and unrelated assemblies do not affect our queries
        meta.cgit_atom_feed.return_value = [
            CgitAtomFeedEntry(title='', content='', updated=then, id='1234567')
        ]
        builds = [
            self.build_record(now, assembly=runtime.assembly, git_commit=None, build_state=BuildStates.FAILED),
            self.build_record(now, assembly='not_ours', git_commit=None)
        ]
        self.assertEqual(meta.needs_rebuild().code, RebuildHintCode.NO_LATEST_BUILD)

    def test_needs_rebuild_with_upstream(self):
        runtime = self.runtime
        meta = self.meta
        koji_mock = self.koji_mock
        now = datetime.datetime.utcnow()
        then = now - datetime.timedelta(hours=5)

        def list_builds(packageID=None, state=None, pattern=None, queryOpts=None):
            return self._list_builds(builds, packageID=packageID, state=state, pattern=pattern, queryOpts=queryOpts)

        runtime.downstream_commitish_overrides = {}
        koji_mock.listBuilds.side_effect = list_builds
        ls_remote_commit = '296ac244f3e7fd2d937316639892f90f158718b0'

        meta.config.content = Model(dict_to_model={
            'source': {
                'git': {
                    'url': 'git@github.com:openshift/release.git',
                }
            }
        })
        exectools.cmd_assert = Mock(return_value=(ls_remote_commit, ''))  # emulate response to ls-remote of openshift/release

        # If listBuilds returns nothing, we want to trigger a rebuild
        builds = []
        self.assertEqual(meta.needs_rebuild().code, RebuildHintCode.NO_LATEST_BUILD)

        # Make sure irrelevant builds are ignored
        builds = [
            self.build_record(now, assembly='not_ours'),
            self.build_record(now, assembly=runtime.assembly, build_state=BuildStates.FAILED),
            self.build_record(now, assembly=runtime.assembly, git_commit=ls_remote_commit, build_state=BuildStates.FAILED),
            self.build_record(now, assembly=f'{runtime.assembly}extra', git_commit=ls_remote_commit)  # Close but not quite our assembly
        ]
        self.assertEqual(meta.needs_rebuild().code, RebuildHintCode.NO_LATEST_BUILD)

        meta.cgit_atom_feed = Mock()
        meta.cgit_atom_feed.return_value = [
            CgitAtomFeedEntry(title='', content='', updated=then, id='1234567')
        ]

        # In this scenario, we have a build newer than distgit's commit, but it's git.<> release
        # component does not match the current upstream ls-remote commit. This means there is
        # a new build required.
        builds = [
            self.build_record(now, assembly=runtime.assembly, git_commit='abcdefg')
        ]
        self.assertEqual(meta.needs_rebuild().code, RebuildHintCode.NEW_UPSTREAM_COMMIT)

        # In this scenario, we have a build newer than distgit's commit. It's git.<> release
        # component matches the ls-remote value. No new build required.
        builds = [
            self.build_record(now, assembly='not_ours'),
            self.build_record(now, assembly='not_ours', git_commit=ls_remote_commit),
            self.build_record(now, assembly=runtime.assembly, build_state=BuildStates.FAILED),
            self.build_record(now, assembly=runtime.assembly, git_commit=ls_remote_commit, build_state=BuildStates.FAILED),
            self.build_record(now, assembly=runtime.assembly, git_commit=ls_remote_commit)  # This one should match perfectly
        ]
        self.assertEqual(meta.needs_rebuild().code, RebuildHintCode.BUILD_IS_UP_TO_DATE)

        # We should also accept the 'stream' assembly
        builds = [
            self.build_record(now, assembly='not_ours'),
            self.build_record(now, assembly=runtime.assembly, build_state=BuildStates.FAILED),
            self.build_record(now, assembly=runtime.assembly, git_commit=ls_remote_commit, build_state=BuildStates.FAILED),
            self.build_record(now, assembly='stream', git_commit=ls_remote_commit)  # This one should match perfectly
        ]
        self.assertEqual(meta.needs_rebuild().code, RebuildHintCode.BUILD_IS_UP_TO_DATE)

        # If we tried the upstream commit recently and failed, there should be a delay before the next attempt
        builds = [
            self.build_record(now, assembly='not_ours'),
            self.build_record(now - datetime.timedelta(days=5), git_commit='1234567', assembly=runtime.assembly, build_state=BuildStates.COMPLETE),
            self.build_record(now, assembly=runtime.assembly, git_commit=ls_remote_commit, build_state=BuildStates.FAILED),
        ]
        self.assertEqual(meta.needs_rebuild().code, RebuildHintCode.DELAYING_NEXT_ATTEMPT)

        # If the failed build attempt is old, try again
        builds = [
            self.build_record(now, assembly='not_ours'),
            self.build_record(now - datetime.timedelta(days=5), git_commit='1234567', assembly=runtime.assembly, build_state=BuildStates.COMPLETE),
            self.build_record(now - datetime.timedelta(days=5), assembly=runtime.assembly, git_commit=ls_remote_commit, build_state=BuildStates.FAILED),
        ]
        self.assertEqual(meta.needs_rebuild().code, RebuildHintCode.LAST_BUILD_FAILED)

        # Scenario where the latest build has a commit that does not agree with current upstream commit
        # but there is an old build that does. Indicates some type of revert. Rebuild.
        builds = [
            self.build_record(now, assembly='not_ours'),
            self.build_record(now, git_commit='1234567', assembly=runtime.assembly, build_state=BuildStates.COMPLETE),
            self.build_record(now - datetime.timedelta(days=5), assembly=runtime.assembly, git_commit=ls_remote_commit),
        ]
        self.assertEqual(meta.needs_rebuild().code, RebuildHintCode.UPSTREAM_COMMIT_MISMATCH)

        # The preceding revert scenario only applies if the assemblies match
        builds = [
            self.build_record(now, assembly='not_ours'),
            self.build_record(now, git_commit='1234567', assembly='stream', build_state=BuildStates.COMPLETE),
            self.build_record(now - datetime.timedelta(days=5), assembly=runtime.assembly, git_commit=ls_remote_commit),
        ]
        self.assertEqual(meta.needs_rebuild().code, RebuildHintCode.BUILD_IS_UP_TO_DATE)

        # If both builds are 'stream' assembly, the upstream commit revert DOES affect our assembly
        builds = [
            self.build_record(now, assembly='not_ours'),
            self.build_record(now, git_commit='1234567', assembly='stream', build_state=BuildStates.COMPLETE),
            self.build_record(now - datetime.timedelta(days=5), assembly='stream', git_commit=ls_remote_commit),
        ]
        self.assertEqual(meta.needs_rebuild().code, RebuildHintCode.UPSTREAM_COMMIT_MISMATCH)

        # If there is any build of our assembly, it does not matter if there is one from stream; only use
        # one specific to our assembly. In this case, our last assembly specific build does not have the
        # right upstream commit.
        builds = [
            self.build_record(now, assembly='not_ours'),
            self.build_record(now - datetime.timedelta(days=5), git_commit=ls_remote_commit, assembly='stream', build_state=BuildStates.COMPLETE),
            self.build_record(now, assembly=runtime.assembly, git_commit='123457'),
        ]
        self.assertEqual(meta.needs_rebuild().code, RebuildHintCode.UPSTREAM_COMMIT_MISMATCH)
