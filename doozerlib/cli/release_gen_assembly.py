import click
import sys
import json
import yaml
from typing import Dict, List, Set

from doozerlib import util
from doozerlib.cli import cli, pass_runtime
from doozerlib import exectools
from doozerlib.model import Model
from doozerlib import brew
from doozerlib.rpmcfg import RPMMetadata
from doozerlib.image import BrewBuildImageInspector


@cli.group("release:gen-assembly", short_help="Output assembly metadata based on inputs")
@click.option('--name', metavar='ASSEMBLY_NAME', required=True, help='The name of the assembly (e.g. "4.9.99", "art1234") to scaffold')
@click.pass_context
def releases_gen_assembly(ctx, name):
    ctx.ensure_object(dict)
    ctx.obj['ASSEMBLY_NAME'] = name
    pass


@releases_gen_assembly.command('from-releases', short_help='Outputs assembly metadata based on a set of specified releases')
@click.option('--nightly', 'nightlies', metavar='NIGHTLY_NAME', default=[], multiple=True, help='A nightly release name for each architecture (e.g. 4.7.0-0.nightly-2021-07-07-214918)')
@click.option('--standard', 'standards', metavar='4.y.z-ARCH', default=[], multiple=True, help='The name and arch of an official release (e.g. 4.8.3-x86_64) where ARCH in [x86_64, s390x, ppc64le, aarch64].')
@click.option("--custom", default=False, is_flag=True,
              help="If specified, weaker conformance criteria are applied (e.g. a nightly is not required for every arch).")
@pass_runtime
@click.pass_context
def gen_assembly_from_releases(ctx, runtime, nightlies, standards, custom):
    runtime.initialize(mode='both', clone_distgits=False, clone_source=False, prevent_cloning=True)
    logger = runtime.logger
    gen_assembly_name = ctx.obj['ASSEMBLY_NAME']  # The name of the assembly we are going to output

    # Create a map of package_name to RPMMetadata
    package_rpm_meta: Dict[str, RPMMetadata] = {rpm_meta.get_package_name(): rpm_meta for rpm_meta in runtime.rpm_metas()}

    def exit_with_error(msg):
        print(msg, file=sys.stderr)
        exit(1)

    if runtime.assembly != 'stream':
        exit_with_error('--assembly must be "stream" in order to populate an assembly definition from nightlies')

    if not nightlies and not standards:
        exit_with_error('At least one release (--nightly or --standard) must be specified')

    if len(runtime.arches) != len(nightlies) + len(standards) and not custom:
        exit_with_error(f'Expected at least {len(runtime.arches)} nightlies; one for each group arch: {runtime.arches}')

    reference_releases_by_arch: Dict[str, str] = dict()  # Maps brew arch name to nightly name
    mosc_by_arch: Dict[str, str] = dict()  # Maps brew arch name to machine-os-content pullspec from nightly
    component_image_builds: Dict[str, BrewBuildImageInspector] = dict()  # Maps component package_name to brew build dict found for nightly
    component_rpm_builds: Dict[str, Dict[int, Dict]] = dict()  # Dict[ package_name ] -> Dict[ el? ] -> brew build dict
    basis_event_ts: float = 0.0

    release_pullspecs: Dict[str, str] = dict()
    for nightly_name in nightlies:
        major_minor, brew_cpu_arch, priv = util.isolate_nightly_name_components(nightly_name)
        if major_minor != runtime.get_minor_version():
            exit_with_error(f'Specified nightly {nightly_name} does not match group major.minor')
        reference_releases_by_arch[brew_cpu_arch] = nightly_name
        rc_suffix = util.go_suffix_for_arch(brew_cpu_arch, priv)
        nightly_pullspec = f'registry.ci.openshift.org/ocp{rc_suffix}/release{rc_suffix}:{nightly_name}'
        if brew_cpu_arch in release_pullspecs:
            raise ValueError(f'Cannot process {nightly_name} since {release_pullspecs[brew_cpu_arch]} is already included')
        release_pullspecs[brew_cpu_arch] = nightly_pullspec

    for standard_release_name in standards:
        version, brew_cpu_arch = standard_release_name.split('-')  # 4.7.22-s390x => ['4.7.22', 's390x']
        major_minor = '.'.join(version.split('.')[:2])  # isolate just x.y from version names like '4.77.22' and '4.8.0-rc.3'
        if major_minor != runtime.get_minor_version():
            exit_with_error(f'Specified release {standard_release_name} does not match group major.minor')
        standard_pullspec = f'quay.io/openshift-release-dev/ocp-release:{standard_release_name}'
        if brew_cpu_arch in release_pullspecs:
            raise ValueError(f'Cannot process {standard_release_name} since {release_pullspecs[brew_cpu_arch]} is already included')
        release_pullspecs[brew_cpu_arch] = standard_pullspec

    for brew_cpu_arch, pullspec in release_pullspecs.items():
        runtime.logger.info(f'Processing release: {pullspec}')

        release_json_str, _ = exectools.cmd_assert(f'oc adm release info {pullspec} -o=json', retries=3)
        release_info = Model(dict_to_model=json.loads(release_json_str))

        if not release_info.references.spec.tags:
            exit_with_error(f'Could not find any imagestream tags in release: {pullspec}')

        for component_tag in release_info.references.spec.tags:
            payload_tag_name = component_tag.name  # e.g. "aws-ebs-csi-driver"
            payload_tag_pullspec = component_tag['from'].name  # quay pullspec

            if payload_tag_name == 'machine-os-content':
                mosc_by_arch[brew_cpu_arch] = payload_tag_pullspec
                continue

            # The brew_build_inspector will take this archive image and find the actual
            # brew build which created it.
            brew_build_inspector = BrewBuildImageInspector(runtime, payload_tag_pullspec)
            package_name = brew_build_inspector.get_package_name()
            build_nvr = brew_build_inspector.get_nvr()
            if package_name in component_image_builds:
                # If we have already encountered this package once in the list of releases we are
                # processing, then make sure that the original NVR we found matches the new NVR.
                # We want the releases to be populated with identical builds.
                existing_nvr = component_image_builds[package_name].get_nvr()
                if build_nvr != existing_nvr:
                    exit_with_error(f'Found disparate nvrs between releases; {existing_nvr} in processed and {build_nvr} in {pullspec}')
            else:
                # Otherwise, record the build as the first time we've seen an NVR for this
                # package.
                component_image_builds[package_name] = brew_build_inspector

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
            basis_event_ts = max(basis_event_ts, completion_ts + (60.0 * 5))

    # basis_event_ts should now be greater than the build completion / target tagging operation
    # for any (non machine-os-content) image in the nightlies. Because images are built after RPMs,
    # it must also hold that the basis_event_ts is also greater than build completion & tagging
    # of any member RPM.

    # Let's now turn the approximate basis_event_ts into a brew event number
    with runtime.shared_koji_client_session() as koji_api:
        basis_event = koji_api.getLastEvent(before=basis_event_ts)['id']

    logger.info(f'Estimated basis brew event: {basis_event}')
    logger.info(f'The following image package_names were detected in the specified releases: {component_image_builds.keys()}')

    # That said, things happen. Let's say image component X was built in build X1 and X2.
    # Image component Y was build in Y1. Let's say that the ordering was X1, X2, Y1 and, for
    # whatever reason, we find X1 and Y1 in the user specified nightly. This means the basis_event_ts
    # we find for Y1 is going to find X2 instead of X1 if we used it as part of an assembly's basis event.

    # To avoid that, we now evaluate whether any images or RPMs defy our assumption that the nightly
    # corresponds to the basis_event_ts we have calculated. If we find something that will not be swept
    # correctly by the estimated basis event, we collect up the outliers (hopefully few in number) into
    # a list of packages which must be included in the assembly as 'is:'. This might happen if, for example,
    # an artist accidentally builds an image on the command line for the stream assembly; without this logic,
    # that build might be found by our basis event, but we will explicitly pin to the image in the nightly
    # component's NVR as an override in the assembly definition.
    force_is: Set[str] = set()  # A set of package_names whose NVRs are not correctly sourced by the estimated basis_event
    for image_meta in runtime.image_metas():

        if image_meta.base_only or not image_meta.for_release:
            continue

        dgk = image_meta.distgit_key
        package_name = image_meta.get_component_name()
        basis_event_dict = image_meta.get_latest_build(default=None, complete_before_event=basis_event)
        if not basis_event_dict:
            exit_with_error(f'No image was found for assembly {runtime.assembly} for component {dgk} at estimated brew event {basis_event}. No normal reason for this to happen so exiting out of caution.')

        basis_event_build_dict: BrewBuildImageInspector = BrewBuildImageInspector(runtime, basis_event_dict['id'])
        basis_event_build_nvr = basis_event_build_dict.get_nvr()

        if not image_meta.is_payload:
            # If this is not for the payload, the nightlies cannot have informed our NVR decision; just
            # pick whatever the estimated basis will pull and let the user know. If they want to change
            # it, they will need to pin it.
            logger.info(f'{dgk} non-payload build {basis_event_build_nvr} will be swept by estimated assembly basis event')
            component_image_builds[package_name] = basis_event_build_dict
            continue

        # Otherwise, the image_meta is destined for the payload and analyzing the nightlies should
        # have given us an NVR which is expected to be selected by the assembly.

        if package_name not in component_image_builds:
            if custom:
                logger.warning(f'Unable to find {dgk} in releases despite it being marked as is_payload in ART metadata; this may be because the image is not built for every arch or it is not labeled appropriately for the payload. Choosing what was in the estimated basis event sweep: {basis_event_build_nvr}')
            else:
                logger.error(f'Unable to find {dgk} in releases despite it being marked as is_payload in ART metadata; this may mean the image does not have the proper labeling for being in the payload. Choosing what was in the estimated basis event sweep: {basis_event_build_nvr}')
            component_image_builds[package_name] = basis_event_build_dict
            continue

        ref_releases_component_build = component_image_builds[package_name]
        ref_nightlies_component_build_nvr = ref_releases_component_build.get_nvr()

        if basis_event_build_nvr != ref_nightlies_component_build_nvr:
            logger.info(f'{dgk} build {basis_event_build_nvr} was selected by estimated basis event. That is not what is in the specified releases, so this image will be pinned.')
            force_is.add(package_name)
            continue

        # Otherwise, the estimated basis event resolved the image nvr we found in the nightlies. The
        # image NVR does not need to be pinned. Yeah!
        pass

    # We should have found a machine-os-content for each architecture in the group for a standard assembly
    for arch in runtime.arches:
        if arch not in mosc_by_arch:
            if custom:
                # This is permitted for custom assemblies which do not need to be assembled for every
                # architecture. The customer may just need x86_64.
                logger.info(f'Did not find machine-os-content image for active group architecture: {arch}; ignoring since this is custom.')
            else:
                exit_with_error(f'Did not find machine-os-content image for active group architecture: {arch}')

    # We now have a list of image builds that should be selected by the assembly basis event
    # and those that will need to be forced with 'is'. We now need to perform a similar step
    # for RPMs. Look at the image contents, see which RPMs are in use. If we build them,
    # then the NVRs in the image must be selected by the estimated basis event. If they are
    # not, then we must pin the NVRs in the assembly definition.

    with runtime.shared_koji_client_session() as koji_api:

        archive_lists = brew.list_archives_by_builds([b.get_brew_build_id() for b in component_image_builds.values()], "image", koji_api)
        rpm_build_ids = {rpm["build_id"] for archives in archive_lists for ar in archives for rpm in ar["rpms"]}
        logger.info("Querying Brew build information for %s RPM builds...", len(rpm_build_ids))
        # We now have a list of all RPM builds which have been installed into the various images which
        # ART builds. Specifically the ART builds which went into composing the nightlies.
        ref_releases_rpm_builds: List[Dict] = brew.get_build_objects(rpm_build_ids, koji_api)

        for ref_releases_rpm_build in ref_releases_rpm_builds:
            package_name = ref_releases_rpm_build['package_name']
            if package_name in package_rpm_meta:  # Does ART build this package?
                rpm_meta = package_rpm_meta[package_name]
                dgk = rpm_meta.distgit_key
                rpm_build_nvr = ref_releases_rpm_build['nvr']
                # If so, what RHEL version is this build for?
                el_ver = util.isolate_el_version_in_release(ref_releases_rpm_build['release'])
                if not el_ver:
                    exit_with_error(f'Unable to isolate el? version in {rpm_build_nvr}')

                if package_name not in component_rpm_builds:
                    # If this is the first time we've seen this ART package, bootstrap a dict for its
                    # potentially different builds for different RHEL versions.
                    component_rpm_builds[package_name]: Dict[int, Dict] = dict()

                if el_ver in component_rpm_builds[package_name]:
                    # We've already captured the build in our results
                    continue

                # Now it is time to see whether a query for the RPM from the basis event
                # estimate comes up with this RPM NVR.
                basis_event_build_dict = rpm_meta.get_latest_build(el_target=el_ver, complete_before_event=basis_event)
                if not basis_event_build_dict:
                    exit_with_error(f'No RPM was found for assembly {runtime.assembly} for component {dgk} at estimated brew event {basis_event}. No normal reason for this to happen so exiting out of caution.')

                if el_ver in component_rpm_builds[package_name]:
                    # We've already logged a build for this el version before
                    continue

                component_rpm_builds[package_name][el_ver] = ref_releases_rpm_build
                basis_event_build_nvr = basis_event_build_dict['nvr']
                logger.info(f'{dgk} build {basis_event_build_nvr} selected by scan against estimated basis event')
                if basis_event_build_nvr != ref_releases_rpm_build['nvr']:
                    # The basis event estimate did not find the RPM from the nightlies. We have to pin the package.
                    logger.info(f'{dgk} build {basis_event_build_nvr} was selected by estimated basis event. That is not what is in the specified releases, so this RPM will be pinned.')
                    force_is.add(package_name)

    # component_image_builds now contains a mapping of package_name -> BrewBuildImageInspector for all images that should be included
    # in the assembly.
    # component_rpm_builds now contains a mapping of package_name to different RHEL versions that should be included
    # in the assembly.
    # force_is is a set of package_names which were not successfully selected by the estimated basis event.

    image_member_overrides: List[Dict] = []
    rpm_member_overrides: List[Dict] = []
    for package_name in force_is:
        if package_name in component_image_builds:
            build_inspector: BrewBuildImageInspector = component_image_builds[package_name]
            dgk = build_inspector.get_image_meta().distgit_key
            image_member_overrides.append({
                'distgit_key': dgk,
                'why': 'Query from assembly basis event failed to replicate referenced nightly content exactly. Pinning to replicate.',
                'metadata': {
                    'is': {
                        'nvr': build_inspector.get_nvr()
                    }
                }
            })
        elif package_name in component_rpm_builds:
            dgk = package_rpm_meta[package_name].distgit_key
            rpm_member_overrides.append({
                'distgit_key': dgk,
                'why': 'Query from assembly basis event failed to replicate referenced nightly content exactly. Pinning to replicate.',
                'metadata': {
                    'is': {
                        f'el{el_ver}': component_rpm_builds[package_name][el_ver]['nvr'] for el_ver in component_rpm_builds[package_name]
                    }
                }
            })

    group_info = {}
    if not custom:
        group_info['advisories'] = {
            'image': -1,
            'rpm': -1,
            'extras': -1,
            'metadata': -1,
        }
    else:
        # Custom payloads don't require advisories.
        # If the user has specified fewer nightlies than is required by this
        # group, then we need to override the group arches.
        group_info = {
            'arches!': list(mosc_by_arch.keys())
        }

    assembly_def = {
        'releases': {
            gen_assembly_name: {
                "assembly": {
                    'type': 'custom' if custom else 'standard',
                    'basis': {
                        'brew_event': basis_event,
                        'reference_releases': reference_releases_by_arch,
                    },
                    'group': group_info,
                    'rhcos': {
                        'machine-os-content': {
                            "images": mosc_by_arch,
                        }
                    },
                    'members': {
                        'rpms': rpm_member_overrides,
                        'images': image_member_overrides,
                    }
                }
            }
        }
    }

    print(yaml.dump(assembly_def))
