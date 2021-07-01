import glob
import io
import os
import re
import shutil
import threading
import traceback
from typing import List, Optional

import rpm

from doozerlib import brew, constants, util
from doozerlib.exceptions import DoozerFatalError
from doozerlib.source_modifications import SourceModifierFactory

from . import exectools
from .brew import watch_tasks
from .metadata import Metadata
from .model import Missing
from .pushd import Dir

RELEASERS_CONF = """
[{target}]
releaser = tito.release.DistGitReleaser
branches = {branch}
build_targets = {branch}:{brew_target}
srpm_disttag = .el7aos
builder.test = 1
remote_git_name = {name}
"""

# note; appended to, does not replace existing props
TITO_PROPS = """

[{target}]
remote_git_name = {name}
"""


class RPMMetadata(Metadata):

    def __init__(self, runtime, data_obj, commitish: Optional[str] = None, clone_source=True,
                 source_modifier_factory=SourceModifierFactory(), prevent_cloning: Optional[bool] = False):
        super(RPMMetadata, self).__init__('rpm', runtime, data_obj, commitish, prevent_cloning=prevent_cloning)

        self.source = self.config.content.source
        if self.source is Missing:
            raise ValueError('RPM config must contain source entry.')
        self.source_modifier_factory = source_modifier_factory
        self.rpm_name = self.config.name
        self.version = None
        self.release = None
        self.tag = None
        self.build_status = False
        self.pre_init_sha = None
        # This will be set to True if the source contains embargoed (private) CVE fixes. Defaulting to None means this should be auto-determined.
        self.private_fix = None

        # If populated, extra variables that will added as os_git_vars
        self.extra_os_git_vars = {}

        self.source_path = None
        self.source_head = None
        # path to the specfile
        self.specfile = None
        if clone_source:
            self.clone_source()

    def clone_source(self):
        self.source_path = self.runtime.resolve_source(self)
        self.source_head = self.runtime.resolve_source_head(self)
        with Dir(self.source_path):
            # gather source repo short sha for audit trail
            out, _ = exectools.cmd_assert(["git", "rev-parse", "HEAD"])
            self.pre_init_sha = source_full_sha = out.strip()

            # Determine if the source contains private fixes by checking if the private org branch commit exists in the public org
            if self.private_fix is None and self.public_upstream_branch:
                self.private_fix = not util.is_commit_in_public_upstream(source_full_sha, self.public_upstream_branch, self.source_path)

            self.extra_os_git_vars.update(self.extract_kube_env_vars())

        if self.source.specfile:
            self.specfile = os.path.join(self.source_path, self.source.specfile)
            if not os.path.isfile(self.specfile):
                raise ValueError('{} config specified a spec file that does not exist: {}'.format(
                    self.config_filename, self.specfile
                ))
        else:
            with Dir(self.source_path):
                specs = []
                for spec in glob.glob('*.spec'):
                    specs.append(spec)
                if len(specs) > 1:
                    raise ValueError('More than one spec file found. Specify correct file in config yaml')
                elif len(specs) == 0:
                    raise ValueError('Unable to find any spec files in {}'.format(self.source_path))
                else:
                    self.specfile = os.path.join(self.source_path, specs[0])
        return self.source_path

    def set_nvr(self, version, release):
        self.version = version
        self.release = release
        self.tag = '{}-{}-{}'.format(self.config.name, self.version, self.release)

    def _run_modifications(self, specfile: Optional[os.PathLike] = None, cwd: Optional[os.PathLike] = None):
        """
        Interprets and applies content.source.modify steps in the image metadata.

        :param specfile: Path to an alternative specfile. None means use the specfile in the source dir
        :param cwd: If set, change current working directory. None means use the source dir
        """
        with io.open(specfile or self.specfile, 'r', encoding='utf-8') as df:
            specfile_data = df.read()

        self.logger.debug(
            "About to start modifying spec file [{}]:\n{}\n".
            format(self.name, specfile_data))

        # add build data modifications dir to path; we *could* add more
        # specific paths for the group and the individual config but
        # expect most scripts to apply across multiple groups.
        metadata_scripts_path = self.runtime.data_dir + "/modifications"
        path = os.pathsep.join([os.environ['PATH'], metadata_scripts_path])
        new_specfile_data = specfile_data

        for modification in self.config.content.source.modifications:
            if self.source_modifier_factory.supports(modification.action):
                # run additional modifications supported by source_modifier_factory
                modifier = self.source_modifier_factory.create(**modification)
                # pass context as a dict so that the act function can modify its content
                context = {
                    "component_name": self.name,
                    "kind": "spec",
                    "content": new_specfile_data,
                    "set_env": {"PATH": path},
                }
                modifier.act(context=context, ceiling_dir=cwd or Dir.getcwd())
                new_specfile_data = context.get("result")
            else:
                raise IOError("%s: Don't know how to perform modification action: %s" % (self.distgit_key, modification.action))

        if new_specfile_data is not None and new_specfile_data != specfile_data:
            with io.open(specfile or self.specfile, 'w', encoding='utf-8') as df:
                df.write(new_specfile_data)

    target_golangs = {}  # a dict of cached target -> golang_version mappings
    target_golangs_lock = threading.Lock()

    def assert_golang_versions(self):
        """ Assert all buildroots have consistent versions of golang compilers
        """
        # no: do not check; x.y: only major and minor version; exact: the z-version must be the same
        check_mode = self.runtime.group_config.check_golang_versions
        if check_mode is Missing:
            check_mode = "x.y"

        if check_mode == "no" or check_mode is False:
            # if 'no' is not wrapped in quotes in the yaml, it is interpreted as False; so check both
            return

        # populate target_golangs with information from Brew
        with RPMMetadata.target_golangs_lock:
            uncached_targets = set(self.targets) - RPMMetadata.target_golangs.keys()
        if uncached_targets:
            uncached_targets = list(uncached_targets)
            self.logger.debug(f"Querying golang compiler versions for targets {uncached_targets}...")
            brew_session = self.runtime.build_retrying_koji_client()
            # get buildroots for uncached targets
            with brew_session.multicall(strict=True) as m:
                tasks = [m.getBuildTarget(target) for target in uncached_targets]
            buildroots = [task.result["build_tag_name"] for task in tasks]
            # get latest build of golang compiler for each buildroot
            golang_components = ["golang", "golang-scl-shim"]
            for target, buildroot in zip(uncached_targets, buildroots):
                latest_builds = brew.get_latest_builds([(buildroot, component) for component in golang_components], "rpm", None, brew_session)
                latest_builds = [builds[0] for builds in latest_builds if builds]  # flatten latest_builds
                # It is possible that a buildroot has multiple golang compiler packages (golang and golang-scl-shim) tagged in.
                # We need to find the maximum version in each buildroot.
                max_golang_nevr = None
                for build in latest_builds:
                    nevr = (build["name"], build["epoch"], build["version"], build["release"])
                    if max_golang_nevr is None or rpm.labelCompare(nevr[1:], max_golang_nevr[1:]) > 0:
                        max_golang_nevr = nevr
                if max_golang_nevr is None:
                    raise DoozerFatalError(f"Buildroot {buildroot} doesn't contain any golang compiler packages.")
                if max_golang_nevr[0] == "golang-scl-shim":
                    # golang-scl-shim is not an actual compiler but an adaptor to make go-toolset look like golang for an RPM build.
                    # We need to check the actual go-toolset build it requires.
                    # See https://source.redhat.com/groups/public/atomicopenshift/atomicopenshift_wiki/what_art_needs_to_know_about_golang#jive_content_id_golangsclshim
                    major, minor = max_golang_nevr[2].split(".")[:2]
                    go_toolset_builds = brew_session.getLatestBuilds(buildroot, package=f"go-toolset-{major}.{minor}", type="rpm")
                    if not go_toolset_builds:
                        raise DoozerFatalError(f"Buildroot {buildroot} doesn't have go-toolset-{major}.{minor} tagged in.")
                    max_golang_nevr = (go_toolset_builds[0]["name"], go_toolset_builds[0]["epoch"], go_toolset_builds[0]["version"], go_toolset_builds[0]["release"])
                with RPMMetadata.target_golangs_lock:
                    RPMMetadata.target_golangs[target] = max_golang_nevr

        # assert all buildroots have the same version of golang compilers
        it = iter(self.targets)
        first_target = next(it)
        with RPMMetadata.target_golangs_lock:
            first_nevr = RPMMetadata.target_golangs[first_target]
            for target in it:
                nevr = RPMMetadata.target_golangs[target]
                if (check_mode == "exact" and nevr[2] != first_nevr[2]) or (check_mode == "x.y" and nevr[2].split(".")[:2] != first_nevr[2].split(".")[:2]):
                    raise DoozerFatalError(f"Buildroot for target {target} has inconsistent golang compiler version {nevr[2]} while target {first_target} has {first_nevr[2]}.")

    def get_package_name_from_spec(self):
        """
        Returns the brew package name for the distgit repository. Method requires
        a local clone. This differs from get_package_name because it will actually
        parse the .spec file in the distgit clone vs trusting the ocp-build-data.
        :return: The package name if detected in the distgit spec file. Otherwise, the specified default.
                If default is not passed in, an exception will be raised if the package name can't be found.
        """
        specs = glob.glob(f'{self.distgit_repo().distgit_dir}/*.spec')
        if len(specs) != 1:
            raise IOError('Unable to find .spec file in RPM distgit: ' + self.qualified_name)

        spec_path = specs[0]
        with open(spec_path, mode='r', encoding='utf-8') as f:
            for line in f.readlines():
                if line.lower().startswith('name:'):
                    return line[5:].strip()  # Exclude "Name:" and then remove whitespace

        raise IOError(f'Unable to find Name: field in rpm spec: {spec_path}')

    def get_package_name(self) -> str:
        """
        Returns the brew package name for the distgit repository. Package names for RPMs
        do not necessarily match the distgit component name. The package name is controlled
        by the RPM's spec file. The 'name' field in the doozer metadata should match what is
        in the RPM's spec file.
        :return: The package name - Otherwise, the specified default.
                If default is not passed in, an exception will be raised if the package name can't be found.
        """
        return self.get_component_name()

    def candidate_brew_tag(self):
        return self.targets[0]

    def candidate_brew_tags(self):
        return self.targets.copy()

    def default_brew_target(self):
        if self.runtime.hotfix:
            target = f"{self.branch()}-hotfix"
        else:
            target = f"{self.branch()}-candidate"
        return target
