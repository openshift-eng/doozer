from typing import Any, List, Dict, Optional

from koji import ClientSession
from doozerlib.model import Model
from doozerlib.rpm_delivery import RPMDeliveries, RPMDelivery
from doozerlib.rpm_utils import parse_nvr

from doozerlib import brew, util, Runtime
from doozerlib.image import BrewBuildImageInspector
from doozerlib.rpmcfg import RPMMetadata
from doozerlib.assembly import assembly_rhcos_config, AssemblyTypes, assembly_permits, AssemblyIssue, \
    AssemblyIssueCode, assembly_type
from doozerlib.rhcos import RHCOSBuildInspector, RHCOSBuildFinder, get_container_configs, RhcosMissingContainerException


class AssemblyInspector:
    """ It inspects an assembly """
    def __init__(self, runtime: Runtime, brew_session: ClientSession = None, lookup_mode: str = "both"):
        """
        :param runtime: Doozer runtime
        :param brew_session: Brew session object to use for communicating with Brew
        :param lookup_mode:
            None: Create a lite version without the ability to inspect Images; can be used to check
                  AssemblyIssues, fetch rhcos_builds and other defined methods
            "images": Do the lookups to enable image inspection, but expect code touching group RPMs
                      to fail (limited use case)
            "both": Do the lookups for a full inspection
        """
        self.runtime = runtime
        self.brew_session = brew_session
        if lookup_mode and runtime.mode != lookup_mode:
            raise ValueError(f'Runtime must be initialized with "{lookup_mode}"')

        self.assembly_rhcos_config = assembly_rhcos_config(self.runtime.releases_config, self.runtime.assembly)
        self.assembly_type: AssemblyTypes = assembly_type(self.runtime.releases_config, self.runtime.assembly)
        self._rpm_build_cache: Dict[int, Dict[str, Optional[Dict]]] = {}  # Dict[rhel_ver] -> Dict[distgit_key] -> Optional[BuildDict]
        self._permits = assembly_permits(self.runtime.releases_config, self.runtime.assembly)

        if not lookup_mode:  # do no lookups
            return
        # If an image component has a latest build, an ImageInspector associated with the image.
        self._release_image_inspectors: Dict[str, Optional[BrewBuildImageInspector]] = dict()
        for image_meta in runtime.get_for_release_image_metas():
            latest_build_obj = image_meta.get_latest_build(default=None)
            if latest_build_obj:
                self._release_image_inspectors[image_meta.distgit_key] = BrewBuildImageInspector(self.runtime, latest_build_obj['nvr'])
            else:
                self._release_image_inspectors[image_meta.distgit_key] = None

        # Preprocess rpm_deliveries group config
        # This is mainly to support weekly kernel delivery
        self._rpm_deliveries: Dict[str, RPMDelivery] = {}  # Dict[package_name] => per package RpmDelivery config
        if self.runtime.group_config.rpm_deliveries:
            # parse and validate rpm_deliveries config
            rpm_deliveries = RPMDeliveries.parse_obj(self.runtime.group_config.rpm_deliveries.primitive())
            for entry in rpm_deliveries:
                packages = entry.packages
                for package in packages:
                    if package in self._rpm_deliveries:
                        raise ValueError(f"Duplicate package {package} defined in rpm_deliveries config")
                    self._rpm_deliveries[package] = entry

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

    def _check_installed_packages_for_rpm_delivery(self, component: str, component_description: str, rpm_packages: Dict[str, Dict[str, Any]]):
        """
        If rpm_deliveries is configured, this checks installed packages in a component
        follow the stop-ship/ship-ok tagging requirement.
        See https://issues.redhat.com/browse/ART-6100 for more information.

        :param component: Component name, like `rhcos` or image distgit key
        :param component_description: Component description, used to include more details for the component in the AssemblyIssue message
        :param rpm_packages: A dict; key is rpm package name, value is brew build dict
        """
        self.runtime.logger.info("Checking installed rpms in %s", component_description)
        issues: List[AssemblyIssue] = []
        for package_name, rpm_build in rpm_packages.items():
            if package_name in self._rpm_deliveries:
                rpm_delivery_config = self._rpm_deliveries[package_name]
                self.runtime.logger.info("Getting tags for rpm build %s...", rpm_build['nvr'])
                tag_names = {tag["name"] for tag in self.brew_session.listTags(brew.KojiWrapperOpts(caching=True), build=rpm_build["id"])}
                # If the rpm is tagged into the stop-ship tag, it is never permissible
                if rpm_delivery_config.stop_ship_tag and rpm_delivery_config.stop_ship_tag in tag_names:
                    issues.append(AssemblyIssue(f'{component_description} has {rpm_build["nvr"]}, which has been tagged into the stop-ship tag: {rpm_delivery_config.stop_ship_tag}', component=component, code=AssemblyIssueCode.IMPERMISSIBLE))
                    continue
                # Check if the rpm has an explicit "ship-ok" tag
                if rpm_delivery_config.ship_ok_tag and rpm_delivery_config.ship_ok_tag not in tag_names:
                    # If the rpm is not tagged into the integration tag, it might be obtained from a released repo
                    if rpm_delivery_config.integration_tag and rpm_delivery_config.integration_tag not in tag_names:
                        release_tag = next(filter(lambda tag: tag.endswith("-released"), tag_names), None)
                        if release_tag:
                            self.runtime.logger.warning(f"Permit {rpm_build['nvr']} because it was already released through {release_tag}.")
                            continue
                    # For GA releases, the rpm must have an explicit "ship-ok" tag
                    if self.assembly_type is AssemblyTypes.STANDARD:
                        issues.append(AssemblyIssue(f'{component_description} has {rpm_build["nvr"]}, which is missing the ship-ok tag: {rpm_delivery_config.ship_ok_tag}', component=component, code=AssemblyIssueCode.IMPERMISSIBLE))
                    # For pre-GA releases, MISSING_SHIP_OK_TAG issue will be reported if the rpm doesn't have a "ship-ok" tag
                    if self.assembly_type is AssemblyTypes.CANDIDATE:
                        issues.append(AssemblyIssue(f'{component_description} has {rpm_build["nvr"]}, which is missing the ship-ok tag: {rpm_delivery_config.ship_ok_tag}', component=component, code=AssemblyIssueCode.MISSING_SHIP_OK_TAG))
        return issues

    def check_installed_rpms_in_image(self, dg_key: str, build_inspector: BrewBuildImageInspector):
        """ Analyzes an image build to check if installed packages are allowed to assemble.
        """
        issues: List[AssemblyIssue] = []
        self.runtime.logger.info("Getting rpms in image build %s...", build_inspector.get_nvr())
        installed_packages = build_inspector.get_all_installed_package_build_dicts()
        # If rpm_deliveries is configured, check if the image build has rpms respecting the stop-ship/ship-ok tag
        issues.extend(self._check_installed_packages_for_rpm_delivery(dg_key, f'Image build {build_inspector.get_nvr()}', installed_packages))
        return issues

    def check_rhcos_issues(self, rhcos_build: RHCOSBuildInspector) -> List[AssemblyIssue]:
        """
        Analyzes an RHCOS build to check whether the installed packages are consistent with:
        1. package NVRs defined at the group dependency level
        2. package NVRs defined at the rhcos dependency level
        3. package NVRs of any RPMs built in this assembly/group

        If rpm_deliveries is defined in group config, this will also check
        whether the installed packages meet the tagging requirements like "stop-ship" and "ship-ok".

        :param rhcos_build: The RHCOS build to analyze.
        :return: Returns a (potentially empty) list of inconsistencies in the build.
        """
        self.runtime.logger.info(f'Checking RHCOS build for consistency: {str(rhcos_build)}...')

        issues: List[AssemblyIssue] = []
        required_packages: Dict[str, str] = dict()  # Dict[package_name] -> nvr  # Dependency specified in 'rhcos' in assembly definition
        desired_packages: Dict[str, str] = dict()  # Dict[package_name] -> nvr  # Dependency specified at group level
        installed_packages: Dict[str, Dict[str, Any]] = dict()  # Dict[package_name] -> build dict  # rpms installed in the rhcos image

        self.runtime.logger.info("Getting rpms in RHCOS build %s...", rhcos_build.build_id)
        installed_packages = rhcos_build.get_package_build_objects()

        # If rpm_deliveries is configured, check if the RHCOS build has rpms with the stop-ship/ship-ok tag
        issues.extend(self._check_installed_packages_for_rpm_delivery('rhcos', f'RHCOS build {rhcos_build.build_id} ({rhcos_build.brew_arch})', installed_packages))

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
                    # i.e. the artists expected something to be installed, but it wasn't found in the final image.
                    # Raise an issue. In rare circumstances the RPM may be used by early stage of the Dockerfile
                    # and not in the final. In this case, it should be permitted in the assembly definition.
                    issues.append(AssemblyIssue(f'Expected image to contain assembly member override dependencies NVR {required_nvr} but it was not installed', component=dgk, code=AssemblyIssueCode.MISSING_INHERITED_DEPENDENCY))

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
            for rpm_dgk, assembly_rpm_build in self.get_group_rpm_build_dicts(el_ver).items():
                if not assembly_rpm_build:
                    # The RPM doesn't claim to build for this image's RHEL base, so ignore it.
                    continue
                package_name = assembly_rpm_build['package_name']
                assembly_nvr = assembly_rpm_build['nvr']
                if package_name in installed_packages:
                    installed_nvr = installed_packages[package_name]['nvr']
                    if installed_nvr != assembly_nvr:
                        issues.append(AssemblyIssue(f'Expected image to contain assembly RPM build {assembly_nvr} but found {installed_nvr} installed', component=rpm_dgk, code=AssemblyIssueCode.CONFLICTING_GROUP_RPM_INSTALLED))

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
                        raise IOError(f'RPM {rpm_meta.distgit_key} claims to have a rhel-{el_ver} build target, but no build was detected')
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
        build_id = None
        runtime.logger.info(f"Getting latest RHCOS source for {brew_arch}...")

        # See if this assembly has assembly.rhcos.*.images populated for this architecture.
        pullspec_for_tag = dict()
        for container_conf in get_container_configs(runtime):
            # first we look at the assembly definition as the source of truth for RHCOS containers
            assembly_rhcos_arch_pullspec = self.assembly_rhcos_config[container_conf.name].images[brew_arch]
            if assembly_rhcos_arch_pullspec:
                pullspec_for_tag[container_conf.name] = assembly_rhcos_arch_pullspec
                continue

            # for non-stream assemblies we expect explicit config for RHCOS
            if self.runtime.assembly_type != AssemblyTypes.STREAM:
                if container_conf.primary:
                    raise Exception(f'Assembly {runtime.assembly} is not type STREAM but no assembly.rhcos.{container_conf.name} image data for {brew_arch}; all RHCOS image data must be populated for this assembly to be valid')
                # require the primary container at least to be specified, but
                # allow the edge case where we add an RHCOS container type and
                # previous assemblies don't specify it
                continue

            try:
                version = self.runtime.get_minor_version()
                build_id, pullspec = RHCOSBuildFinder(runtime, version, brew_arch, private,
                                                      custom=custom).latest_container(container_conf)
                if not pullspec:
                    raise IOError(f"No RHCOS latest found for {version} / {brew_arch}")
                pullspec_for_tag[container_conf.name] = pullspec
            except RhcosMissingContainerException:
                if container_conf.primary:
                    # accommodate RHCOS build metadata not specifying all expected containers, but require primary.
                    # their absence will be noted when generating payloads anyway.
                    raise

        return RHCOSBuildInspector(runtime, pullspec_for_tag, brew_arch, build_id)
