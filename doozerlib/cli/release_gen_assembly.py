import asyncio

import click
import json
import re
from semver import VersionInfo
import sys
from typing import Dict, List, Optional, Set, Tuple
import yaml

from doozerlib import util
from doozerlib.assembly import AssemblyTypes
from doozerlib.cli import cli, pass_runtime, click_coroutine
from doozerlib import exectools
from doozerlib.model import Model
from doozerlib import brew
from doozerlib import rhcos
from doozerlib.rpmcfg import RPMMetadata
from doozerlib.image import BrewBuildImageInspector
from doozerlib.runtime import Runtime


@cli.group("release:gen-assembly", short_help="Output assembly metadata based on inputs")
@click.option('--name', metavar='ASSEMBLY_NAME', required=True,
              help='The name of the assembly (e.g. "4.9.99", "art1234") to scaffold')
@click.pass_context
def releases_gen_assembly(ctx, name):
    ctx.ensure_object(dict)
    ctx.obj['ASSEMBLY_NAME'] = name
    pass


@releases_gen_assembly.command('from-releases',
                               short_help='Outputs assembly metadata based on a set of specified releases')
@click.option('--nightly', 'nightlies', metavar='NIGHTLY_NAME', default=[], multiple=True,
              help='A nightly release name for each architecture (e.g. 4.7.0-0.nightly-2021-07-07-214918)')
@click.option('--standard', 'standards', metavar='4.y.z-ARCH', default=[], multiple=True,
              help='The name and arch of an official release (e.g. 4.8.3-x86_64) '
                   'where ARCH in [x86_64, s390x, ppc64le, aarch64].')
@click.option("--custom", default=False, is_flag=True,
              help="If specified, weaker conformance criteria are applied "
                   "(e.g. a nightly is not required for every arch).")
@click.option('--in-flight', 'in_flight', metavar='EDGE', help='An in-flight release that can upgrade to this release')
@click.option('--previous', 'previous_list', metavar='EDGES', default=[], multiple=True,
              help='A list of releases that can upgrade to this release')
@click.option('--auto-previous', 'auto_previous', is_flag=True,
              help='If specified, previous list is calculated from Cincinnati graph')
@click.option("--graph-url", metavar='GRAPH_URL', required=False,
              default='https://api.openshift.com/api/upgrades_info/v1/graph',
              help="When using --auto-previous, set custom Cincinnati graph URL to query")
@click.option("--graph-content-stable", metavar='JSON_FILE', required=False,
              help="When using --auto-previous, override content from stable channel - primarily for testing")
@click.option("--graph-content-candidate", metavar='JSON_FILE', required=False,
              help="When using --auto-previous, override content from candidate channel - primarily for testing")
@click.option("--suggestions-url", metavar='SUGGESTIONS_URL', required=False,
              default="https://raw.githubusercontent.com/openshift/cincinnati-graph-data/master/build-suggestions/",
              help="When using --auto-previous, set custom suggestions URL, load from {major}-{minor}-{arch}.yaml")
@click.option('--output-file', '-o', required=False,
              help='Specify a file path to write the generated assembly definition to')
@pass_runtime
@click_coroutine
@click.pass_context
async def gen_assembly_from_releases(ctx, runtime: Runtime, nightlies: Tuple[str, ...], standards: Tuple[str, ...],
                                     custom: bool, in_flight: Optional[str], previous_list: Tuple[str, ...],
                                     auto_previous: bool, graph_url: Optional[str], graph_content_stable: Optional[str],
                                     graph_content_candidate: Optional[str], suggestions_url: Optional[str],
                                     output_file: Optional[str]):

    runtime.initialize(mode='both', clone_distgits=False, clone_source=False, prevent_cloning=True)

    assembly_def = await GenAssemblyCli(
        runtime=runtime,
        gen_assembly_name=ctx.obj['ASSEMBLY_NAME'],
        nightlies=nightlies,
        standards=standards,
        custom=custom,
        in_flight=in_flight,
        previous_list=previous_list,
        auto_previous=auto_previous,
        graph_url=graph_url,
        graph_content_stable=graph_content_stable,
        graph_content_candidate=graph_content_candidate,
        suggestions_url=suggestions_url,
    ).run()

    print(yaml.dump(assembly_def))
    if output_file:
        with open(output_file, 'w') as file:
            yaml.safe_dump(assembly_def, file)


class GenAssemblyCli:
    def __init__(
            self,
            # leave these all optional to make testing easier
            runtime: Runtime = None,
            gen_assembly_name: str = '',
            nightlies: Tuple[str, ...] = [],
            standards: Tuple[str, ...] = [],
            custom: bool = False,
            in_flight: Optional[str] = None,
            previous_list: Tuple[str, ...] = None,
            auto_previous: bool = False,
            graph_url: Optional[str] = None,
            graph_content_stable: Optional[str] = None,
            graph_content_candidate: Optional[str] = None,
            suggestions_url: Optional[str] = None):

        self.runtime = runtime
        # The name of the assembly we are going to output
        self.gen_assembly_name = gen_assembly_name
        self.nightlies = nightlies
        self.standards = standards
        self.custom = custom
        self.in_flight = in_flight
        self.previous_list = previous_list
        self.auto_previous = auto_previous
        self.graph_url = graph_url
        self.graph_content_stable = graph_content_stable
        self.graph_content_candidate = graph_content_candidate
        self.suggestions_url = suggestions_url
        self.logger = self.runtime.logger
        self.release_pullspecs: Dict[str, str] = dict()
        # Maps brew arch name to nightly name
        self.reference_releases_by_arch: Dict[str, str] = dict()
        # Maps RHCOS container name(s) to brew arch name to pullspec(s) from nightly
        self.rhcos_by_tag: Dict[str, Dict[str, str]] = dict()
        # Maps component package_name to brew build dict found for nightly
        self.component_image_builds: Dict[str, BrewBuildImageInspector] = dict()
        # Dict[ package_name ] -> Dict[ el? ] -> brew build dict
        self.component_rpm_builds: Dict[str, Dict[int, Dict]] = dict()
        self.basis_event: int = 0
        self.basis_event_ts: float = 0.0
        # A set of package_names whose NVRs are not correctly sourced by the estimated basis_event
        self.force_is: Set[str] = set()
        self.primary_rhcos_tag: str = ''
        self.final_previous_list: List[VersionInfo] = []

        # Infer assembly type
        if self.custom:
            self.assembly_type = AssemblyTypes.CUSTOM
        elif re.search(r'^[fr]c\.[0-9]+$', self.gen_assembly_name):
            self.assembly_type = AssemblyTypes.CANDIDATE
        elif re.search(r'^ec\.[0-9]+$', self.gen_assembly_name):
            self.assembly_type = AssemblyTypes.PREVIEW
        else:
            self.assembly_type = AssemblyTypes.STANDARD

        # Create a map of package_name to RPMMetadata
        self.package_rpm_meta: Dict[str, RPMMetadata] = \
            {rpm_meta.get_package_name(): rpm_meta for rpm_meta in self.runtime.rpm_metas()}

    async def run(self):
        self._validate_params()
        self._get_release_pullspecs()
        await self._select_images()
        self._get_rhcos_container()
        self._select_rpms()
        self._calculate_previous_list()
        return self._generate_assembly_definition()

    @staticmethod
    def _exit_with_error(msg):
        print(msg, file=sys.stderr)
        exit(1)

    def _validate_params(self):
        if self.runtime.assembly != 'stream':
            self._exit_with_error(
                '--assembly must be "stream" in order to populate an assembly definition from nightlies')

        if not self.nightlies and not self.standards:
            self._exit_with_error('At least one release (--nightly or --standard) must be specified')

        if len(self.runtime.arches) != len(self.nightlies) + len(self.standards) and not self.custom:
            self._exit_with_error(f'Expected at least {len(self.runtime.arches)} nightlies; '
                                  f'one for each group arch: {self.runtime.arches}')

        if self.auto_previous and self.previous_list:
            self._exit_with_error('Cannot use `--previous` and `--auto-previous` at the same time.')

        if self.assembly_type in [AssemblyTypes.CUSTOM]:
            if self.auto_previous or self.previous_list or self.in_flight:
                self._exit_with_error("Custom releases don't have previous list.")

    def _get_release_pullspecs(self):
        for nightly_name in self.nightlies:
            major_minor, brew_cpu_arch, priv = util.isolate_nightly_name_components(nightly_name)
            if major_minor != self.runtime.get_minor_version():
                self._exit_with_error(f'Specified nightly {nightly_name} does not match group major.minor')
            self.reference_releases_by_arch[brew_cpu_arch] = nightly_name
            rc_suffix = util.go_suffix_for_arch(brew_cpu_arch, priv)
            nightly_pullspec = f'registry.ci.openshift.org/ocp{rc_suffix}/release{rc_suffix}:{nightly_name}'
            if brew_cpu_arch in self.release_pullspecs:
                raise ValueError(
                    f'Cannot process {nightly_name} since {self.release_pullspecs[brew_cpu_arch]} is already included')
            self.release_pullspecs[brew_cpu_arch] = nightly_pullspec

        for standard_release_name in self.standards:
            version, brew_cpu_arch = standard_release_name.split('-')  # 4.7.22-s390x => ['4.7.22', 's390x']
            major_minor = '.'.join(
                version.split('.')[:2])  # isolate just x.y from version names like '4.77.22' and '4.8.0-rc.3'
            if major_minor != self.runtime.get_minor_version():
                self._exit_with_error(f'Specified release {standard_release_name} does not match group major.minor')
            standard_pullspec = f'quay.io/openshift-release-dev/ocp-release:{standard_release_name}'
            if brew_cpu_arch in self.release_pullspecs:
                raise ValueError(f'Cannot process {standard_release_name} since '
                                 f'{self.release_pullspecs[brew_cpu_arch]} is already included')
            self.release_pullspecs[brew_cpu_arch] = standard_pullspec

    async def _select_images(self):
        await self._determine_basis_brew_event()
        self._collect_outliers()

    @exectools.limit_concurrency(500)
    async def _process_release(self, brew_cpu_arch, pullspec, rhcos_tag_names):
        self.runtime.logger.info(f'Processing release: {pullspec}')

        release_json_str, _ = await exectools.cmd_assert_async(f'oc adm release info {pullspec} -o=json', retries=3)
        release_info = Model(dict_to_model=json.loads(release_json_str))

        if not release_info.references.spec.tags:
            self._exit_with_error(f'Could not find any imagestream tags in release: {pullspec}')

        for component_tag in release_info.references.spec.tags:
            payload_tag_name = component_tag.name  # e.g. "aws-ebs-csi-driver"
            payload_tag_pullspec = component_tag['from'].name  # quay pullspec

            if payload_tag_name in rhcos_tag_names:
                self.rhcos_by_tag.setdefault(payload_tag_name, {})[brew_cpu_arch] = payload_tag_pullspec
                continue

            # The brew_build_inspector will take this archive image and find the actual
            # brew build which created it.
            brew_build_inspector = BrewBuildImageInspector(self.runtime, payload_tag_pullspec)
            package_name = brew_build_inspector.get_package_name()
            build_nvr = brew_build_inspector.get_nvr()
            if package_name in self.component_image_builds:
                # If we have already encountered this package once in the list of releases we are
                # processing, then make sure that the original NVR we found matches the new NVR.
                # We want the releases to be populated with identical builds.
                existing_nvr = self.component_image_builds[package_name].get_nvr()
                if build_nvr != existing_nvr:
                    self._exit_with_error(f'Found disparate nvrs between releases; '
                                          f'{existing_nvr} in processed and {build_nvr} in {pullspec}')
            else:
                # Otherwise, record the build as the first time we've seen an NVR for this
                # package.
                self.component_image_builds[package_name] = brew_build_inspector

            # We now try to determine a basis brew event that will
            # find this image during get_latest_build-like operations
            # for the assembly. At the time of this writing, metadata.get_latest_build
            # will only look for builds *completed* before the basis event. This could
            # be changed to *created* before the basis event in the future. However,
            # other logic that is used to find latest builds requires the build to be
            # tagged into an rhaos tag before the basis brew event.
            # To choose a safe / reliable basis brew event, we first find the
            # time at which a build was completed, then add 5 minutes.
            # That extra 5 minutes ensures brew will have had time to tag the
            # build appropriately for its build target. The 5 minutes is also
            # short enough to ensure that no other build of this image could have
            # completed before the basis event.

            completion_ts: float = brew_build_inspector.get_brew_build_dict()['completion_ts']
            # If the basis event for this image is > the basis_event capable of
            # sweeping images we've already analyzed, increase the basis_event_ts.
            self.basis_event_ts = max(self.basis_event_ts, completion_ts + (60.0 * 5))

    async def _determine_basis_brew_event(self):
        rhcos_tag_names = rhcos.get_container_names(self.runtime)
        await asyncio.gather(*[
            self._process_release(brew_cpu_arch, pullspec, rhcos_tag_names)
            for brew_cpu_arch, pullspec in self.release_pullspecs.items()
        ])

        # basis_event_ts should now be greater than the build completion / target tagging operation
        # for any (non RHCOS) image in the nightlies. Because images are built after RPMs,
        # it must also hold that the basis_event_ts is also greater than build completion & tagging
        # of any member RPM.

        # Let's now turn the approximate basis_event_ts into a brew event number
        with self.runtime.shared_koji_client_session() as koji_api:
            self.basis_event = koji_api.getLastEvent(before=self.basis_event_ts)['id']
        self.logger.info(f'Estimated basis brew event: {self.basis_event}')
        self.logger.info('The following image package_names were detected in the specified releases: %s',
                         self.component_image_builds.keys())

    def _collect_outliers(self):
        """
        Things happen. Let's say image component X was built in build X1 and X2.
        Image component Y was build in Y1. Let's say that the ordering was X1, X2, Y1 and, for
        whatever reason, we find X1 and Y1 in the user specified nightly. This means the basis_event_ts
        we find for Y1 is going to find X2 instead of X1 if we used it as part of an assembly's basis event.

        To avoid that, we now evaluate whether any images or RPMs defy our assumption that the nightly
        corresponds to the basis_event_ts we have calculated. If we find something that will not be swept
        correctly by the estimated basis event, we collect up the outliers (hopefully few in number) into
        a list of packages which must be included in the assembly as 'is:'. This might happen if, for example,
        an artist accidentally builds an image on the command line for the stream assembly; without this logic,
        that build might be found by our basis event, but we will explicitly pin to the image in the nightly
        component's NVR as an override in the assembly definition.
        """

        for image_meta in self.runtime.image_metas():

            if image_meta.base_only or not image_meta.for_release:
                continue

            dgk = image_meta.distgit_key
            package_name = image_meta.get_component_name()
            basis_event_dict = image_meta.get_latest_build(default=None, complete_before_event=self.basis_event)
            if not basis_event_dict:
                self._exit_with_error(f'No image was found for assembly {self.runtime.assembly} for component {dgk} '
                                      f'at estimated brew event {self.basis_event}. No normal reason for this to '
                                      f'happen so exiting out of caution.')

            basis_event_build_dict: BrewBuildImageInspector = BrewBuildImageInspector(
                self.runtime, basis_event_dict['id'])
            basis_event_build_nvr = basis_event_build_dict.get_nvr()

            if not image_meta.is_payload:
                # If this is not for the payload, the nightlies cannot have informed our NVR decision; just
                # pick whatever the estimated basis will pull and let the user know. If they want to change
                # it, they will need to pin it.
                self.logger.info(
                    f'{dgk} non-payload build {basis_event_build_nvr} will be swept by estimated assembly basis event')
                self.component_image_builds[package_name] = basis_event_build_dict
                continue

            # Otherwise, the image_meta is destined for the payload and analyzing the nightlies should
            # have given us an NVR which is expected to be selected by the assembly.

            if package_name not in self.component_image_builds:
                if self.custom:
                    self.logger.warning('Unable to find %s in releases despite it being marked as is_payload '
                                        'in ART metadata; this may be because the image is not built for every arch '
                                        'or it is not labeled appropriately for the payload. '
                                        'Choosing what was in the estimated basis event sweep: %s',
                                        dgk, basis_event_build_nvr)
                else:
                    self.logger.error('Unable to find %s in releases despite it being marked as is_payload '
                                      'in ART metadata; this may mean the image does not have the proper labeling for '
                                      'being in the payload. Choosing what was in the estimated basis event sweep: %s',
                                      dgk, basis_event_build_nvr)
                self.component_image_builds[package_name] = basis_event_build_dict
                continue

            ref_releases_component_build = self.component_image_builds[package_name]
            ref_nightlies_component_build_nvr = ref_releases_component_build.get_nvr()

            if basis_event_build_nvr != ref_nightlies_component_build_nvr:
                self.logger.info('%s build %s was selected by estimated basis event. That is not what is in the '
                                 'specified releases, so this image will be pinned.', dgk, basis_event_build_nvr)
                self.force_is.add(package_name)
                continue

            # Otherwise, the estimated basis event resolved the image nvr we found in the nightlies. The
            # image NVR does not need to be pinned. Yeah!

    def _get_rhcos_container(self):
        # We should have found an RHCOS container for each architecture in the group for a standard assembly
        self.primary_rhcos_tag = rhcos.get_primary_container_name(self.runtime)

        for arch in self.runtime.arches:
            if arch in self.rhcos_by_tag[self.primary_rhcos_tag]:
                continue

            if self.custom:
                # This is permitted for custom assemblies which do not need to be assembled for every
                # architecture. The customer may just need x86_64.
                self.logger.info('Did not find RHCOS "%s" image for active group architecture: %s; '
                                 'ignoring for custom assembly type.', self.primary_rhcos_tag, arch)
            else:
                self._exit_with_error(
                    f'Did not find RHCOS "{self.primary_rhcos_tag}" image for active group architecture: {arch}')

    def _select_rpms(self):
        """
        We now have a list of image builds that should be selected by the assembly basis event
        and those that will need to be forced with 'is'. We now need to perform a similar step
        for RPMs. Look at the image contents, see which RPMs are in use. If we build them,
        then the NVRs in the image must be selected by the estimated basis event. If they are
        not, then we must pin the NVRs in the assembly definition.
        """

        with self.runtime.shared_koji_client_session() as koji_api:

            archive_lists = brew.list_archives_by_builds(
                build_ids=[b.get_brew_build_id() for b in self.component_image_builds.values()],
                build_type="image",
                session=koji_api
            )
            rpm_build_ids = {rpm["build_id"] for archives in archive_lists for ar in archives for rpm in ar["rpms"]}

            self.logger.info("Querying Brew build information for %s RPM builds...", len(rpm_build_ids))
            # We now have a list of all RPM builds which have been installed into the various images which
            # ART builds. Specifically the ART builds which went into composing the nightlies.
            ref_releases_rpm_builds: List[Dict] = brew.get_build_objects(rpm_build_ids, koji_api)

            for ref_releases_rpm_build in ref_releases_rpm_builds:
                package_name = ref_releases_rpm_build['package_name']
                if package_name not in self.package_rpm_meta:  # Does ART build this package?
                    continue

                rpm_meta = self.package_rpm_meta[package_name]
                dgk = rpm_meta.distgit_key
                rpm_build_nvr = ref_releases_rpm_build['nvr']

                # If so, what RHEL version is this build for?
                el_ver = util.isolate_el_version_in_release(ref_releases_rpm_build['release'])
                if not el_ver:
                    self._exit_with_error(f'Unable to isolate el? version in {rpm_build_nvr}')

                if package_name not in self.component_rpm_builds:
                    # If this is the first time we've seen this ART package, bootstrap a dict for its
                    # potentially different builds for different RHEL versions.
                    self.component_rpm_builds[package_name]: Dict[int, Dict] = dict()

                if el_ver in self.component_rpm_builds[package_name]:
                    # We've already captured the build in our results
                    continue

                # Now it is time to see whether a query for the RPM from the basis event
                # estimate comes up with this RPM NVR.
                basis_event_build_dict = rpm_meta.get_latest_build(
                    el_target=el_ver, complete_before_event=self.basis_event)

                if not basis_event_build_dict:
                    self._exit_with_error(f'No RPM was found for assembly {self.runtime.assembly} for component {dgk} '
                                          f'at estimated brew event {self.basis_event}. No normal reason for this to '
                                          f'happen so exiting out of caution.')

                if el_ver in self.component_rpm_builds[package_name]:
                    # We've already logged a build for this el version before
                    continue

                self.component_rpm_builds[package_name][el_ver] = ref_releases_rpm_build
                basis_event_build_nvr = basis_event_build_dict['nvr']
                self.logger.info(f'{dgk} build {basis_event_build_nvr} selected by scan against estimated basis event')

                if basis_event_build_nvr != ref_releases_rpm_build['nvr']:
                    # The basis event estimate did not find the RPM from the nightlies. We have to pin the package.
                    self.logger.info('%s build %s was selected by estimated basis event. '
                                     'That is not what is in the specified releases, so this RPM will be pinned.',
                                     dgk, basis_event_build_nvr)
                    self.force_is.add(package_name)

    def _calculate_previous_list(self):
        final_previous_list: Set[VersionInfo] = set()
        if self.in_flight:
            final_previous_list.add(VersionInfo.parse(self.in_flight))
        if self.previous_list:
            final_previous_list |= set(map(VersionInfo.parse, self.previous_list))
        elif self.auto_previous:
            # gen_assembly_name should be in the form of `ec.0`, `fc.0`, `rc.1`, or `4.10.1`
            if self.assembly_type == AssemblyTypes.CANDIDATE or self.assembly_type == AssemblyTypes.PREVIEW:
                major_minor = self.runtime.get_minor_version()  # x.y
                version = f"{major_minor}.0-{self.gen_assembly_name}"
            else:
                version = self.gen_assembly_name
            for arch in self.runtime.arches:
                self.logger.info("Calculating previous list for %s", arch)
                previous_list = util.get_release_calc_previous(
                    version, arch, self.graph_url, self.graph_content_stable,
                    self.graph_content_candidate, self.suggestions_url)
                final_previous_list |= set(map(VersionInfo.parse, previous_list))
        self.final_previous_list = sorted(final_previous_list)

    def _get_advisories_release_jira(self) -> Tuple[Dict[str, int], str]:
        # Add placeholder advisory numbers and JIRA key.
        # Those values will be replaced with real values by pyartcd when preparing a release.
        advisories = {
            'image': -1,
            'rpm': -1,
            'extras': -1,
            'metadata': -1,
        }

        # For OCP >= 4.12, also microshift advisory placeholder must be created
        major, minor = self.runtime.get_major_minor_fields()
        if major > 4 or minor >= 12:  # exclude 3.11, include any 5.y and 4.12+
            advisories['microshift'] = -1

        release_jira = "ART-0"

        if self.assembly_type == AssemblyTypes.CANDIDATE:
            # if this assembly is rc.X, then check if there is a previously defined rc.X-1
            # pick advisories and release ticket from there
            split = re.split(r'(\d+)', self.gen_assembly_name)
            # ['rc.', '2', '']
            current_v = int(split[1])
            if current_v != 0:
                previous_assembly = f"{split[0]}{current_v - 1}"
                releases_config = self.runtime.get_releases_config()
                if previous_assembly in releases_config.releases:
                    previous_group = releases_config.releases[previous_assembly].assembly.group
                    advisories = previous_group.advisories.primitive()
                    release_jira = previous_group.release_jira
                    self.logger.info(f"Reusing advisories and release ticket from previous candidate assembly {previous_assembly}, {previous_group.advisories}, {previous_group.release_jira}")
                else:
                    self.logger.info("No matching previous candidate assembly found")

        return advisories, release_jira

    def _generate_assembly_definition(self) -> dict:
        image_member_overrides, rpm_member_overrides = self._get_member_overrides()

        group_info = {}
        if self.custom:
            # Custom payloads don't require advisories.
            # If the user has specified fewer nightlies than is required by this
            # group, then we need to override the group arches.
            group_info = {
                'arches!': list(self.rhcos_by_tag[self.primary_rhcos_tag].keys())
            }

        if self.assembly_type not in [AssemblyTypes.CUSTOM, AssemblyTypes.PREVIEW]:
            group_info['advisories'], group_info["release_jira"] = self._get_advisories_release_jira()

        if self.final_previous_list:
            group_info['upgrades'] = ','.join(map(str, self.final_previous_list))

        return {
            'releases': {
                self.gen_assembly_name: {
                    "assembly": {
                        'type': self.assembly_type.value,
                        'basis': {
                            'brew_event': self.basis_event,
                            'reference_releases': self.reference_releases_by_arch,
                        },
                        'group': group_info,
                        'rhcos': {
                            tag: dict(images={arch: pullspec for arch, pullspec in specs_by_arch.items()})
                            for tag, specs_by_arch in self.rhcos_by_tag.items()
                        },
                        'members': {
                            'rpms': rpm_member_overrides,
                            'images': image_member_overrides,
                        }
                    }
                }
            }
        }

    def _get_member_overrides(self):
        """
        self.component_image_builds now contains a mapping of package_name -> BrewBuildImageInspector
        for all images that should be included in the assembly.

        self.component_rpm_builds now contains a mapping of package_name to different RHEL versions
        that should be included in the assembly.

        self.force_is is a set of package_names which were not successfully selected by the estimated basis event.
        """

        image_member_overrides: List[Dict] = []
        rpm_member_overrides: List[Dict] = []

        for package_name in self.force_is:
            if package_name in self.component_image_builds:
                build_inspector: BrewBuildImageInspector = self.component_image_builds[package_name]
                dgk = build_inspector.get_image_meta().distgit_key
                image_member_overrides.append({
                    'distgit_key': dgk,
                    'why': 'Query from assembly basis event failed to replicate '
                           'referenced nightly content exactly. Pinning to replicate.',
                    'metadata': {
                        'is': {
                            'nvr': build_inspector.get_nvr()
                        }
                    }
                })
            elif package_name in self.component_rpm_builds:
                dgk = self.package_rpm_meta[package_name].distgit_key
                rpm_member_overrides.append({
                    'distgit_key': dgk,
                    'why': 'Query from assembly basis event failed to replicate '
                           'referenced nightly content exactly. Pinning to replicate.',
                    'metadata': {
                        'is': {
                            f'el{el_ver}': self.component_rpm_builds[package_name][el_ver]['nvr'] for el_ver in
                            self.component_rpm_builds[package_name]
                        }
                    }
                })

        return image_member_overrides, rpm_member_overrides
