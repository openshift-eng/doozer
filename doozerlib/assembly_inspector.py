from typing import List, Dict, Optional

from koji import ClientSession
from kobo.rpmlib import parse_nvr

from doozerlib import util, Runtime
from doozerlib.image import BrewBuildImageInspector
from doozerlib.rpmcfg import RPMMetadata
from doozerlib.assembly import assembly_rhcos_config, AssemblyTypes, assembly_permits, AssemblyIssue, \
    AssemblyIssueCode, assembly_type
from doozerlib.rhcos import RHCOSBuildInspector, RHCOSBuildFinder


class AssemblyInspector:
    """ It inspects an assembly """
    def __init__(self, runtime: Runtime, brew_session: ClientSession = None, lite: bool = False):
        """
        :param runtime: Doozer runtime
        :param brew_session: Brew session object to use for communicating with Brew
        :param lite: Create a lite version without the ability to inspect Images; can be used to check AssemblyIssues,
        fetch rhcos_builds and other defined methods
        """
        self.runtime = runtime
        self.brew_session = brew_session
        if not lite and runtime.mode != 'both':
            raise ValueError('Runtime must be initialized with "both"')

        self.assembly_rhcos_config = assembly_rhcos_config(self.runtime.releases_config, self.runtime.assembly)
        self.assembly_type: AssemblyTypes = assembly_type(self.runtime.releases_config, self.runtime.assembly)
        self._rpm_build_cache: Dict[int, Dict[str, Optional[Dict]]] = {}  # Dict[rhel_ver] -> Dict[distgit_key] -> Optional[BuildDict]
        self._permits = assembly_permits(self.runtime.releases_config, self.runtime.assembly)

        if lite:
            return
        # If an image component has a latest build, an ImageInspector associated with the image.
        self._release_image_inspectors: Dict[str, Optional[BrewBuildImageInspector]] = dict()
        for image_meta in runtime.get_for_release_image_metas():
            latest_build_obj = image_meta.get_latest_build(default=None)
            if latest_build_obj:
                self._release_image_inspectors[image_meta.distgit_key] = BrewBuildImageInspector(self.runtime, latest_build_obj['nvr'])
            else:
                self._release_image_inspectors[image_meta.distgit_key] = None

    def get_type(self) -> AssemblyTypes:
        return self.assembly_type

    def does_permit(self, issue: AssemblyIssue) -> bool:
        """
        Checks all permits for this assembly definition to see if a given issue
        is actually permitted when it is time to construct a payload.
        :return: Returns True if the issue is permitted to exist in the assembly payload.
        """
        for permit in self._permits:
            if issue.code == AssemblyIssueCode.IMPERMISSIBLE:
                # permitting '*' still doesn't permit impermissible
                return False
            if permit.code == '*' or issue.code.name == permit.code:
                if permit.component == '*' or issue.component == permit.component:
                    return True
        return False

    def check_rhcos_issues(self, rhcos_build: RHCOSBuildInspector) -> List[AssemblyIssue]:
        """
        Analyzes an RHCOS build to check whether the installed packages are consistent with:
        1. package NVRs defined at the group dependency level
        2. package NVRs defiend at the rhcos dependency level
        3. package NVRs of any RPMs built in this assembly/group
        :param rhcos_build: The RHCOS build to analyze.
        :return: Returns a (potentially empty) list of inconsistencies in the build.
        """
        self.runtime.logger.info(f'Checking RHCOS build for consistency: {str(rhcos_build)}...')

        issues: List[AssemblyIssue] = []
        required_packages: Dict[str, str] = dict()  # Dict[package_name] -> nvr  # Dependency specified in 'rhcos' in assembly definition
        desired_packages: Dict[str, str] = dict()  # Dict[package_name] -> nvr  # Dependency specified at group level
        el_tag = f'el{rhcos_build.get_rhel_base_version()}'
        for package_entry in (self.runtime.get_group_config().dependencies or []):
            if el_tag in package_entry:
                nvr = package_entry[el_tag]
                package_name = parse_nvr(nvr)['name']
                desired_packages[package_name] = nvr

        for package_entry in (self.assembly_rhcos_config.dependencies or []):
            if el_tag in package_entry:
                nvr = package_entry[el_tag]
                package_name = parse_nvr(nvr)['name']
                required_packages[package_name] = nvr
                desired_packages[package_name] = nvr  # Override if something else was at the group level

        installed_packages = rhcos_build.get_package_build_objects()
        for package_name, desired_nvr in desired_packages.items():

            if package_name in required_packages and package_name not in installed_packages:
                # If the dependency is specified in the 'rhcos' section of the assembly, we must find it or raise an issue.
                # This is impermissible because it can simply be fixed in the assembly definition.
                issues.append(AssemblyIssue(f'Expected assembly defined rhcos dependency {desired_nvr} to be installed in {rhcos_build.build_id} but that package was not installed', component='rhcos'))

            if package_name in installed_packages:
                installed_build_dict = installed_packages[package_name]
                installed_nvr = installed_build_dict['nvr']
                if installed_nvr != desired_nvr:
                    # We could consider permitting this in AssemblyTypes.CUSTOM, but it means that the RHCOS build
                    # could not be effectively reproduced by the rebuild job.
                    issues.append(AssemblyIssue(f'Expected {desired_nvr} to be installed in RHCOS build {rhcos_build.build_id} but found {installed_nvr}', component='rhcos', code=AssemblyIssueCode.CONFLICTING_INHERITED_DEPENDENCY))

        """
        If the rhcos build has RPMs from this group installed, make sure they match the NVRs associated with this assembly.
        """
        for dgk, assembly_rpm_build in self.get_group_rpm_build_dicts(el_ver=rhcos_build.get_rhel_base_version()).items():
            if not assembly_rpm_build:
                continue
            package_name = assembly_rpm_build['package_name']
            assembly_nvr = assembly_rpm_build['nvr']
            if package_name in installed_packages:
                installed_nvr = installed_packages[package_name]['nvr']
                if assembly_nvr != installed_nvr:
                    # We could consider permitting this in AssemblyTypes.CUSTOM, but it means that the RHCOS build
                    # could not be effectively reproduced by the rebuild job.
                    issues.append(AssemblyIssue(f'Expected {rhcos_build.build_id}/{rhcos_build.brew_arch} image to contain assembly selected RPM build {assembly_nvr} but found {installed_nvr} installed', component='rhcos', code=AssemblyIssueCode.CONFLICTING_GROUP_RPM_INSTALLED))

        return issues

    def check_group_rpm_package_consistency(self, rpm_meta: RPMMetadata) -> List[AssemblyIssue]:
        """
        Evaluate the current assembly builds of RPMs in the group and check whether they are consistent with
        the assembly definition.
        :param rpm_meta: The rpm metadata to evaluate
        :return: Returns a (potentially empty) list of reasons the rpm should be rebuilt.
        """
        self.runtime.logger.info(f'Checking group RPM for consistency: {rpm_meta.distgit_key}...')
        issues: List[AssemblyIssue] = []

        for rpm_meta in self.runtime.rpm_metas():
            dgk = rpm_meta.distgit_key
            for el_ver in rpm_meta.determine_rhel_targets():
                brew_build_dict = self.get_group_rpm_build_dicts(el_ver=el_ver)[dgk]
                if not brew_build_dict:
                    # Impermissible. The RPM should be built for each target.
                    issues.append(AssemblyIssue(f'Did not find rhel-{el_ver} build for {dgk}', component=dgk))
                    continue

                """
                Assess whether the image build has the upstream
                source git repo and git commit that may have been declared/
                overridden in an assembly definition.
                """
                content_git_url = rpm_meta.config.content.source.git.url
                if content_git_url:
                    # Make sure things are in https form so we can compare
                    # content_git_url = util.convert_remote_git_to_https(content_git_url)

                    # TODO: The commit in which this comment is introduced also introduces
                    # machine parsable yaml documents into distgit commits. Once this code
                    # has been running for our active 4.x releases for some time,
                    # we should check the distgit commit info against the git.url
                    # in our metadata.

                    try:
                        target_branch = rpm_meta.config.content.source.git.branch.target
                        if target_branch:
                            _ = int(target_branch, 16)  # parse the name as a git commit
                            # if we reach here, a git commit hash was declared as the
                            # upstream source of the rpm package's content. We should verify
                            # it perfectly matches what we find in the assembly build.
                            # Each package build gets git commits encoded into the
                            # release field of the NVR. So the NVR should contain
                            # the desired commit.
                            build_nvr = brew_build_dict['nvr']
                            if target_branch[:7] not in build_nvr:
                                # Impermissible because the assembly definition can simply be changed.
                                issues.append(AssemblyIssue(f'{dgk} build for rhel-{el_ver} did not find git commit {target_branch[:7]} in package RPM NVR {build_nvr}', component=dgk))
                    except ValueError:
                        # The meta's target branch a normal branch name
                        # and not a git commit. When this is the case,
                        # we don't try to assert anything about the build's
                        # git commit.
                        pass

        return issues

    def check_group_image_consistency(self, build_inspector: BrewBuildImageInspector) -> List[AssemblyIssue]:
        """
        Evaluate the current assembly build and an image in the group and check whether they are consistent with
        :param build_inspector: The brew build to check
        :return: Returns a (potentially empty) list of reasons the image should be rebuilt.
        """
        image_meta = build_inspector.get_image_meta()
        self.runtime.logger.info(f'Checking group image for consistency: {image_meta.distgit_key}...')
        issues: List[AssemblyIssue] = []

        installed_packages = build_inspector.get_all_installed_package_build_dicts()
        dgk = build_inspector.get_image_meta().distgit_key

        """
        If the assembly defined any RPM package dependencies at the group or image
        member level, we want to check to make sure that installed RPMs in the
        build image match the override package.
        If reading this, keep in mind that a single package/build may create several
        RPMs. Both assemblies and this method deal with the package level - not
        individual RPMs.
        """
        member_package_overrides, all_package_overrides = image_meta.get_assembly_rpm_package_dependencies(el_ver=image_meta.branch_el_target())
        if member_package_overrides or all_package_overrides:
            for package_name, required_nvr in all_package_overrides.items():
                if package_name in member_package_overrides and package_name not in installed_packages:
                    # A dependency was defined explicitly in an assembly member, but it is not installed.
                    # i.e. the artists expected something to be installed, but it wasn't. Raise an issue.
                    # Tis is impermissible because the assembly definition can easily be changed to remove the explicit dep.
                    issues.append(AssemblyIssue(f'Expected image to contain assembly member override dependencies NVR {required_nvr} but it was not installed', component=dgk))

                if package_name in installed_packages:
                    installed_build_dict: Dict = installed_packages[package_name]
                    installed_nvr = installed_build_dict['nvr']
                    if required_nvr != installed_nvr:
                        issues.append(AssemblyIssue(f'Expected image to contain assembly override dependencies NVR {required_nvr} but found {installed_nvr} installed', component=dgk, code=AssemblyIssueCode.CONFLICTING_INHERITED_DEPENDENCY))

        """
        If an image contains an RPM from the doozer group, make sure it is the current
        RPM for the assembly.
        """
        el_ver = build_inspector.get_rhel_base_version()
        if el_ver:  # We might not find an el_ver for an image (e.g. FROM scratch)
            for dgk, assembly_rpm_build in self.get_group_rpm_build_dicts(el_ver).items():
                if not assembly_rpm_build:
                    # The RPM doesn't claim to build for this image's RHEL base, so ignore it.
                    continue
                package_name = assembly_rpm_build['package_name']
                assembly_nvr = assembly_rpm_build['nvr']
                if package_name in installed_packages:
                    installed_nvr = installed_packages[package_name]['nvr']
                    if installed_nvr != assembly_nvr:
                        issues.append(AssemblyIssue(f'Expected image to contain assembly RPM build {assembly_nvr} but found {installed_nvr} installed', component=dgk, code=AssemblyIssueCode.CONFLICTING_GROUP_RPM_INSTALLED))

        """
        Assess whether the image build has the upstream
        source git repo and git commit that may have been declared/
        overridden in an assembly definition.
        """
        content_git_url = image_meta.config.content.source.git.url
        if content_git_url:
            # Make sure things are in https form so we can compare
            content_git_url, _ = self.runtime.get_public_upstream(util.convert_remote_git_to_https(content_git_url))
            build_git_url = util.convert_remote_git_to_https(build_inspector.get_source_git_url())
            if content_git_url != build_git_url:
                # Impermissible as artist can just fix upstream git source in assembly definition
                issues.append(AssemblyIssue(f'Expected image git source from metadata {content_git_url} but found {build_git_url} as the upstream source of the brew build', component=dgk))

            try:
                target_branch = image_meta.config.content.source.git.branch.target
                if target_branch:
                    _ = int(target_branch, 16)  # parse the name as a git commit
                    # if we reach here, a git commit hash was declared as the
                    # upstream source of the image's content. We should verify
                    # it perfectly matches what we find in the assembly build.
                    build_commit = build_inspector.get_source_git_commit()
                    if target_branch != build_commit:
                        # Impermissible as artist can just fix the assembly definition.
                        issues.append(AssemblyIssue(f'Expected image build git commit {target_branch} but {build_commit} was found in the build', component=dgk))
            except ValueError:
                # The meta's target branch a normal branch name
                # and not a git commit. When this is the case,
                # we don't try to assert anything about the build's
                # git commit.
                pass

        return issues

    def get_assembly_name(self):
        return self.runtime.assembly

    def get_group_release_images(self) -> Dict[str, Optional[BrewBuildImageInspector]]:
        """
        :return: Returns a map of distgit_key -> BrewImageInspector for each image built in this group. The value will be None if no build was found.
        """
        return self._release_image_inspectors

    def get_group_rpm_build_dicts(self, el_ver: int) -> Dict[str, Optional[Dict]]:
        """
        :param el_ver: The version of RHEL to check for builds of the RPMs
        :return: Returns Dict[distgit_key] -> brew_build_dict or None if the RPM does not build for the specified el target.
        """
        assembly_rpm_dicts: Dict[str, Optional[Dict]] = dict()
        if el_ver not in self._rpm_build_cache:
            # Maps of component names to the latest brew build dicts. If there is no latest build, the value will be None
            for rpm_meta in self.runtime.rpm_metas():
                if el_ver in rpm_meta.determine_rhel_targets():
                    latest_build = rpm_meta.get_latest_build(default=None, el_target=el_ver)
                    if not latest_build:
                        raise IOError(f'RPM {rpm_meta.distgit_key} claims to have a rhel-{el_ver} build target, but not build was detected')
                    assembly_rpm_dicts[rpm_meta.distgit_key] = latest_build
                else:
                    # The RPM does not claim to build for this rhel version, so return None as a value.
                    assembly_rpm_dicts[rpm_meta.distgit_key] = None
            self._rpm_build_cache[el_ver] = assembly_rpm_dicts

        return self._rpm_build_cache[el_ver]

    def get_rhcos_build(self, arch: str, private: bool = False, custom: bool = False) -> RHCOSBuildInspector:
        """
        :param arch: The CPU architecture of the build to retrieve.
        :param private: If this should be a private build (NOT CURRENTLY SUPPORTED)
        :param custom: If this is a custom RHCOS build (see https://gitlab.cee.redhat.com/openshift-art/rhcos-upshift/-/blob/fdad7917ebdd9c8b47d952010e56e511394ed348/Jenkinsfile#L30).
        :return: Returns an RHCOSBuildInspector for the specified arch. For non-STREAM assemblies, this will be the RHCOS builds
                 pinned in the assembly definition. For STREAM assemblies, it will be the latest RHCOS build in the latest
                 in the app.ci imagestream for ART's release/arch (e.g. ocp-s390x:is/4.7-art-latest-s390x).
        """
        runtime = self.runtime
        brew_arch = util.brew_arch_for_go_arch(arch)
        runtime.logger.info(f"Getting latest RHCOS source for {brew_arch}...")

        # See if this assembly has assembly.rhcos.machine-os-content.images populated for this architecture.
        assembly_rhcos_arch_pullspec = self.assembly_rhcos_config['machine-os-content'].images[brew_arch]

        if self.runtime.assembly_type != AssemblyTypes.STREAM and not assembly_rhcos_arch_pullspec:
            raise Exception(f'Assembly {runtime.assembly} has is not a STREAM but no assembly.rhcos MOSC image data for {brew_arch}; all MOSC image data must be populated for this assembly to be valid')

        version = self.runtime.get_minor_version()
        if assembly_rhcos_arch_pullspec:
            return RHCOSBuildInspector(runtime, assembly_rhcos_arch_pullspec, brew_arch)
        else:
            _, pullspec = RHCOSBuildFinder(runtime, version, brew_arch, private, custom=custom).latest_machine_os_content()
            if not pullspec:
                raise IOError(f"No RHCOS latest found for {version} / {brew_arch}")
            return RHCOSBuildInspector(runtime, pullspec, brew_arch)
