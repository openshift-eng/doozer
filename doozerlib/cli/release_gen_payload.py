import datetime
import hashlib
import traceback
import sys
import json
from pathlib import Path
from typing import List, Optional, Tuple, Dict, NamedTuple, Iterable, Set, Any, Callable

import click
import yaml
import openshift as oc
from kobo.rpmlib import parse_nvr

from doozerlib.brew import KojiWrapper
from doozerlib.rhcos import RHCOSBuildInspector
from doozerlib.cli import cli, pass_runtime
from doozerlib.image import ImageMetadata, BrewBuildImageInspector, ArchiveImageInspector
from doozerlib.assembly_inspector import AssemblyInspector
from doozerlib.runtime import Runtime
from doozerlib.util import red_print, go_suffix_for_arch, brew_arch_for_go_arch, isolate_nightly_name_components, convert_remote_git_to_https, go_arch_for_brew_arch
from doozerlib.assembly import AssemblyTypes, assembly_basis, AssemblyIssue, AssemblyIssueCode
from doozerlib import exectools
from doozerlib.model import Model
from doozerlib.exceptions import DoozerFatalError
from doozerlib.util import find_manifest_list_sha


def default_imagestream_base_name(version: str) -> str:
    return f'{version}-art-latest'


def assembly_imagestream_base_name(runtime: Runtime) -> str:
    version = runtime.get_minor_version()
    if runtime.assembly is None or runtime.assembly == 'stream':
        return default_imagestream_base_name(version)
    else:
        return f'{version}-art-assembly-{runtime.assembly}'


def default_imagestream_namespace_base_name() -> str:
    return "ocp"


def payload_imagestream_name_and_namespace(base_imagestream_name: str, base_namespace: str, brew_arch: str, private: bool) -> Tuple[str, str]:
    """
    :return: Returns the imagestream name and namespace to which images for the specified CPU arch and privacy mode should be synced.
    """
    arch_suffix = go_suffix_for_arch(brew_arch)
    priv_suffix = "-priv" if private else ""
    name = f"{base_imagestream_name}{arch_suffix}{priv_suffix}"
    namespace = f"{base_namespace}{arch_suffix}{priv_suffix}"
    return name, namespace


def modify_and_replace_api_object(api_obj: oc.APIObject, modifier_func: Callable[[oc.APIObject], Any], backup_file_path: Path, dry_run: bool):
    """
    Receives an APIObject, archives the current state of that object, runs a modifying method on it,
    archives the new state of the object, and then tries to replace the object on the
    cluster API server.
    :param api_obj: The openshift client APIObject to work with.
    :param modifier_func: A function that will accept the api_obj as its first parameter and make any desired change
                            to that object.
    :param backup_file_path: A Path object that can be used to archive pre & post modification states of the object
                                before triggering the update.
    :param dry_run: Write archive files but do not actually update the imagestream.
    """
    with backup_file_path.joinpath(f'replacing-{api_obj.kind()}.{api_obj.namespace()}.{api_obj.name()}.before-modify.json').open(mode='w+') as backup_file:
        backup_file.write(api_obj.as_json(indent=4))

    modifier_func(api_obj)
    api_obj_model = api_obj.model

    # Before replacing api objects on the server, make sure to remove aspects that can
    # confuse subsequent CLI interactions with the object.
    if api_obj_model.metadata.annotations['kubectl.kubernetes.io/last-applied-configuration']:
        api_obj_model.metadata.annotations.pop('kubectl.kubernetes.io/last-applied-configuration')

    # If server-side metadata is being passed in, remove it before we try to replace the object.
    if api_obj_model.metadata:
        for md in ['creationTimestamp', 'generation', 'uid']:
            api_obj_model.metadata.pop(md)

    api_obj_model.pop('status')

    with backup_file_path.joinpath(f'replacing-{api_obj.kind()}.{api_obj.namespace()}.{api_obj.name()}.after-modify.json').open(mode='w+') as backup_file:
        backup_file.write(api_obj.as_json(indent=4))

    if not dry_run:
        api_obj.replace()


@cli.command("release:gen-payload", short_help="Generate input files for release mirroring")
@click.option("--is-name", metavar='NAME', required=False,
              help="ImageStream .metadata.name value. For example '4.2-art-latest'")
@click.option("--is-namespace", metavar='NAMESPACE', required=False,
              help="ImageStream .metadata.namespace value. For example 'ocp'")
@click.option("--organization", metavar='ORGANIZATION', required=False, default='openshift-release-dev',
              help="Quay ORGANIZATION to mirror into.\ndefault=openshift-release-dev")
@click.option("--repository", metavar='REPO', required=False, default='ocp-v4.0-art-dev',
              help="Quay REPOSITORY in ORGANIZATION to mirror into.\ndefault=ocp-v4.0-art-dev")
@click.option("--release-repository", metavar='REPO', required=False, default='ocp-release',
              help="Quay REPOSITORY in ORGANIZATION to push release payloads (used for multi-arch)\ndefault=ocp-release")
@click.option("--output-dir", metavar='DIR', required=False, default='.',
              help="Directory into which the mirroring/imagestream artifacts should be written")
@click.option("--skip-gc-tagging", default=False, is_flag=True,
              help="By default, for a named assembly, images will be tagged to prevent garbage collection")
@click.option("--exclude-arch", metavar='ARCH', required=False, multiple=True,
              help="Architecture (brew nomenclature) to exclude from payload generation")
@click.option("--emergency-ignore-issues", default=False, is_flag=True,
              help="If you must get this command to permit an assembly despite issues. Do not use without approval.")
@click.option("--apply", default=False, is_flag=True,
              help="If doozer should perform the mirroring and imagestream updates.")
@click.option("--apply-multi-arch", default=False, is_flag=True,
              help="If doozer should also create a release payload for multi-arch/heterogeneous clusters.")
@click.option("--moist-run", default=False, is_flag=True,
              help="Performing mirroring/etc but to not actually update imagestreams.")
@pass_runtime
def release_gen_payload(runtime: Runtime, is_name: str, is_namespace: str, organization: str,
                        repository: str, release_repository: str, output_dir: str, exclude_arch: Tuple[str, ...],
                        skip_gc_tagging: bool, emergency_ignore_issues: bool,
                        apply: bool, apply_multi_arch: bool, moist_run: bool):
    """Computes a set of imagestream tags which can be assembled
into an OpenShift release for this assembly. The tags will not be
valid unless --apply and is supplied.

Applying the change will cause the OSBS images to be mirrored into the OpenShift release
repositories on quay.

Applying will also directly update the imagestreams relevant to assembly (e.g.
updating 4.9-art-latest for 4.9's stream assembly).

You may provide the namespace and base name for the image streams, or defaults
will be used.

The ORGANIZATION and REPOSITORY options are combined into
ORGANIZATION/REPOSITORY when preparing for mirroring.

Generate files for mirroring from registry-proxy (OSBS storage) to our
quay registry:

\b
    $ doozer --group=openshift-4.2 release:gen-payload \\
        --is-name=4.2-art-latest

Note that if you use -i to include specific images, you should also include
openshift-enterprise-cli to satisfy any need for the 'cli' tag. The cli image
is used automatically as a stand-in for images when an arch does not build
that particular tag.

## Validation ##

Additionally we want to check that the following conditions are true for each
imagestream being updated:

* For all architectures built, RHCOS builds must have matching versions of any
  unshipped RPM they include (per-entry os metadata - the set of RPMs may differ
  between arches, but versions should not).
* Any RPMs present in images (including machine-os-content) from unshipped RPM
  builds included in one of our candidate tags must exactly version-match the
  latest RPM builds in those candidate tags (ONLY; we never flag what we don't
  directly ship.)

These checks (and likely more in the future) should run and any failures should
be listed in brief via a "release.openshift.io/inconsistency" annotation on the
relevant image istag (these are publicly visible; ref. https://bit.ly/37cseC1)
and in more detail in state.yaml. The release-controller, per ART-2195, will
read and propagate/expose this annotation in its display of the release image.
    """
    runtime.initialize(mode='both', clone_distgits=False, clone_source=False, prevent_cloning=True)

    if runtime.assembly not in {None, "stream", "test"} and runtime.assembly not in runtime.releases_config.releases:
        raise DoozerFatalError(f"Assembly '{runtime.assembly}' is not explicitly defined.")

    logger = runtime.logger
    brew_session = runtime.build_retrying_koji_client()

    base_imagestream_name: str = is_name if is_name else assembly_imagestream_base_name(runtime)
    base_istream_namespace: str = is_namespace if is_namespace else default_imagestream_namespace_base_name()

    if runtime.assembly and runtime.assembly != 'stream' and 'art-latest' in base_imagestream_name:
        raise ValueError('The art-latest imagestreams should not be used for an assembly other than "stream"')

    logger.info(f'Collecting latest information associated with the assembly: {runtime.assembly}')
    assembly_inspector = AssemblyInspector(runtime, brew_session)
    logger.info('Checking for mismatched siblings...')
    mismatched_siblings = PayloadGenerator.find_mismatched_siblings(assembly_inspector.get_group_release_images().values())

    # A list of strings that denote inconsistencies across all payloads generated
    assembly_issues: List[AssemblyIssue] = list()

    for mismatched_bbii, sibling_bbi in mismatched_siblings:
        mismatch_issue = AssemblyIssue(f'{mismatched_bbii.get_nvr()} was built from a different upstream source commit ({mismatched_bbii.get_source_git_commit()[:7]}) than one of its siblings {sibling_bbi.get_nvr()} from {sibling_bbi.get_source_git_commit()[:7]}',
                                       component=mismatched_bbii.get_image_meta().distgit_key,
                                       code=AssemblyIssueCode.MISMATCHED_SIBLINGS)
        assembly_issues.append(mismatch_issue)

    report = dict()
    report['non_release_images'] = [image_meta.distgit_key for image_meta in runtime.get_non_release_image_metas()]
    report['release_images'] = [image_meta.distgit_key for image_meta in runtime.get_for_release_image_metas()]
    report['missing_image_builds'] = [dgk for (dgk, ii) in assembly_inspector.get_group_release_images().items() if ii is None]  # A list of metas where the assembly did not find a build

    if runtime.assembly_type is AssemblyTypes.STREAM:
        # Only nightlies have the concept of private and public payloads
        privacy_modes = [False, True]
    else:
        privacy_modes = [False]

    # Structure to record rhcos builds we use so that they can be analyzed for inconsistencies
    targeted_rhcos_builds: Dict[bool, List[RHCOSBuildInspector]] = {
        False: [],
        True: []
    }

    """
    Collect a list of builds we to tag in order to prevent garbage collection.
    Note: we also use this list to warm up caches, so don't wrap this section
    with `if not skip_gc_tagging`.

    To prevent garbage collection for custom
    assemblies (which won't normally be released via errata tool, triggering
    the traditional garbage collection prevention), we must tag these builds
    explicitly to prevent their GC. It is necessary to prevent GC, because
    we want to be able to build custom releases off of custom releases, and
    so on. If we loose images and builds for custom releases in brew due
    to garbage collection, we will not be able to construct derivative
    release payloads.
    """
    assembly_build_ids: Set[int] = set()  # This list of builds associated with the group/assembly will be used to warm up caches

    list_tags_tasks: Dict[Tuple[int, str], Any] = dict()  # Maps (build_id, tag) tuple to multicall task to list tags
    with runtime.pooled_koji_client_session() as pcs:
        with pcs.multicall(strict=True) as m:
            for bbii in assembly_inspector.get_group_release_images().values():
                if bbii:
                    build_id = bbii.get_brew_build_id()
                    assembly_build_ids.add(build_id)  # Collect up build ids for cache warm up
                    hotfix_tag = bbii.get_image_meta().hotfix_brew_tag()
                    list_tags_tasks[(build_id, hotfix_tag)] = m.listTags(build=build_id)

            # RPMs can build for multiple versions of RHEL. For example, a single RPM
            # metadata can target 7 & 8.
            # For each rhel version targeted by our RPMs, build a list of RPMs
            # appropriate for the RHEL version with respect to the group/assembly.
            rhel_version_scanned_for_rpms: Dict[int, bool] = dict()  # Maps rhel version -> bool indicating whether we have processed that rhel version
            for rpm_meta in runtime.rpm_metas():
                for el_ver in rpm_meta.determine_rhel_targets():
                    if el_ver in rhel_version_scanned_for_rpms:
                        # We've already processed this RHEL version.
                        continue
                    hotfix_tag = runtime.get_default_hotfix_brew_tag(el_target=el_ver)
                    # Otherwise, query the assembly for this rhel version now.
                    for dgk, rpm_build_dict in assembly_inspector.get_group_rpm_build_dicts(el_ver=el_ver).items():
                        if not rpm_build_dict:
                            # RPM not built for this rhel version
                            continue
                        build_id = rpm_build_dict['id']
                        assembly_build_ids.add(build_id)  # For cache warm up later.
                        list_tags_tasks[(build_id, hotfix_tag)] = m.listTags(build=build_id)
                    # Record that we are done for this rhel version.
                    rhel_version_scanned_for_rpms[el_ver] = True

    # Tasks should now contain tag list information for all builds associated with this assembly.
    # and assembly_build_ids should contain ids for builds that should be cached.

    # We have a list of image and RPM builds associated with this assembly.
    # Tag them unless we have been told not to from the command line.
    if runtime.assembly_type != AssemblyTypes.STREAM and not skip_gc_tagging:
        with runtime.shared_koji_client_session() as koji_api:
            koji_api.gssapi_login()  # Tagging requires authentication
            with koji_api.multicall() as m:
                for tup, list_tag_task in list_tags_tasks.items():
                    build_id = tup[0]
                    desired_tag = tup[1]
                    current_tags = [tag_entry['name'] for tag_entry in list_tag_task.result]
                    if desired_tag not in current_tags:
                        # The hotfix tag is missing, so apply it.
                        runtime.logger.info(f'Adding tag {desired_tag} to build: {build_id} to prevent garbage collection.')
                        m.tagBuild(desired_tag, build_id)

    with runtime.shared_build_status_detector() as bsd:
        bsd.populate_archive_lists(assembly_build_ids)
        bsd.find_shipped_builds(assembly_build_ids)

    """
    Make sure that RPMs belonging to this assembly/group are consistent with the assembly definition.
    """
    for rpm_meta in runtime.rpm_metas():
        issues = assembly_inspector.check_group_rpm_package_consistency(rpm_meta)
        assembly_issues.extend(issues)

    """
    If this is a stream assembly, images which are not using the latest rpm builds should not reach
    the release controller. Other assemblies are meant to be constructed from non-latest.
    """
    if runtime.assembly == 'stream':
        for dgk, build_inspector in assembly_inspector.get_group_release_images().items():
            if build_inspector:
                non_latest_rpm_nvrs = build_inspector.find_non_latest_rpms()
                dgk = build_inspector.get_image_meta().distgit_key
                for installed_nvr, newest_nvr in non_latest_rpm_nvrs:
                    # This indicates an issue with scan-sources or that an image is no longer successfully building.
                    # Impermissible as this speaks to a potentially deeper issue of images not being rebuilt
                    outdated_issue = AssemblyIssue(f'Found outdated RPM ({installed_nvr}) installed in {build_inspector.get_nvr()} when {newest_nvr} was available', component=dgk, code=AssemblyIssueCode.OUTDATED_RPMS_IN_STREAM_BUILD)
                    assembly_issues.append(outdated_issue)  # Add to overall issues

    """
    Make sure image builds selected by this assembly/group are consistent with the assembly definition.
    """
    for dgk, bbii in assembly_inspector.get_group_release_images().items():
        if bbii:
            issues = assembly_inspector.check_group_image_consistency(bbii)
            assembly_issues.extend(issues)

    output_path = Path(output_dir).absolute()
    output_path.mkdir(parents=True, exist_ok=True)
    entries_by_arch: Dict[str, Dict[str, PayloadGenerator.PayloadEntry]] = dict()
    for arch in runtime.arches:
        if arch in exclude_arch:
            logger.info(f'Excluding payload files architecture: {arch}')
            continue

        # Whether private or public, the assembly's canonical payload content is the same.
        entries: Dict[str, PayloadGenerator.PayloadEntry] = PayloadGenerator.find_payload_entries(assembly_inspector, arch, f'quay.io/{organization}/{repository}')  # Key of this dict is release payload tag name
        entries_by_arch[arch] = entries

        for tag, payload_entry in entries.items():
            if payload_entry.image_meta:
                # We already stored inconsistencies for each image_meta; look them up if there are any.
                payload_entry.issues.extend(filter(lambda ai: ai.component == payload_entry.image_meta.distgit_key, assembly_issues))
            elif payload_entry.rhcos_build:
                # Record the build so that we can later evaluate consistency between all RHCOS builds. There are presently
                # no private RHCOS builds, so add only to private_mode=False.
                targeted_rhcos_builds[False].append(payload_entry.rhcos_build)
                assembly_issues.extend(assembly_inspector.check_rhcos_issues(payload_entry.rhcos_build))
                payload_entry.issues.extend(filter(lambda ai: ai.component == 'rhcos', assembly_issues))
                if runtime.assembly == 'stream':
                    # For stream alone, we want to enforce that the very latest RPMs are installed.
                    non_latest_rpm_nvrs = payload_entry.rhcos_build.find_non_latest_rpms()
                    for installed_nvr, newest_nvr in non_latest_rpm_nvrs:
                        assembly_issues.append(AssemblyIssue(f'Found outdated RPM ({installed_nvr}) installed in {payload_entry.rhcos_build} when {newest_nvr} is available',
                                                             component='rhcos',
                                                             code=AssemblyIssueCode.OUTDATED_RPMS_IN_STREAM_BUILD))
            else:
                raise IOError(f'Unsupported PayloadEntry: {payload_entry}')

    # Now make sure that all of the RHCOS builds contain consistent RPMs
    for private_mode in privacy_modes:
        rhcos_builds = targeted_rhcos_builds[private_mode]
        rhcos_inconsistencies: Dict[str, List[str]] = PayloadGenerator.find_rhcos_build_rpm_inconsistencies(rhcos_builds)
        if rhcos_inconsistencies:
            assembly_issues.append(AssemblyIssue(f'Found RHCOS inconsistencies in builds {rhcos_builds} (private={private_mode}): {rhcos_inconsistencies}', component='rhcos', code=AssemblyIssueCode.INCONSISTENT_RHCOS_RPMS))

    # If the assembly claims to have reference nightlies, assert that our payload
    # matches them exactly.
    nightly_match_issues = PayloadGenerator.check_nightlies_consistency(assembly_inspector)
    if nightly_match_issues:
        assembly_issues.extend(nightly_match_issues)

    assembly_issues_report: Dict[str, List[Dict]] = dict()
    report['assembly_issues'] = assembly_issues_report

    payload_permitted = True
    for ai in assembly_issues:
        permitted = assembly_inspector.does_permit(ai)
        payload_permitted &= permitted  # If anything is not permitted, exit with an error
        assembly_issues_report.setdefault(ai.component, []).append({
            'code': ai.code.name,
            'msg': ai.msg,
            'permitted': permitted
        })

    report['viable'] = payload_permitted

    print(yaml.dump(report, default_flow_style=False, indent=2))
    overall_permitted = payload_permitted
    if not overall_permitted:
        if emergency_ignore_issues:
            logger.warning('Permitting issues because --emergency-ignore-issues was specified')
            overall_permitted = True
        else:
            logger.warning('Assembly is not permitted. Disabling apply.')
            apply = False
            apply_multi_arch = False

    # In case we are building a heterogeneous / multiarch payload, we need to keep track of images that are
    # going into the each single-arch imagestream. Maps [is_private] -> [tag_name] -> [arch] -> PayloadEntry
    multi_specs: Dict[bool, Dict[str, Dict[str, PayloadGenerator.PayloadEntry]]] = {
        True: dict(),
        False: dict()
    }

    for arch, entries in entries_by_arch.items():
        # Save the default SRC=DEST input to a file for syncing by 'oc image mirror'. Why is
        # there no '-priv'? The true images for the assembly are what we are syncing -
        # it is what we update in the imagestreams that defines whether the image will be
        # part of a public vs private release.
        dests: Set[str] = set()  # Prevents writing the same destination twice (not supported by oc)
        src_dest_path = output_path.joinpath(f"src_dest.{arch}")
        with src_dest_path.open("w+", encoding="utf-8") as out_file:
            for payload_entry in entries.values():
                if not payload_entry.archive_inspector:
                    # Nothing to mirror (e.g. machine-os-content)
                    continue
                if payload_entry.dest_pullspec in dests:
                    # Don't write the same destination twice.
                    continue
                out_file.write(f"{payload_entry.archive_inspector.get_archive_pullspec()}={payload_entry.dest_pullspec}\n")
                dests.add(payload_entry.dest_pullspec)

        if apply or apply_multi_arch:
            logger.info(f'Mirroring images from {str(src_dest_path)}')
            exectools.cmd_assert(f'oc image mirror --filename={str(src_dest_path)}', retries=3)

        for private_mode in privacy_modes:
            logger.info(f'Building payload files for architecture: {arch}; private: {private_mode}')

            imagestream_name, imagestream_namespace = payload_imagestream_name_and_namespace(
                base_imagestream_name,
                base_istream_namespace,
                arch, private_mode)

            # Compute a list of imagestream tags which we want to update in imagestream.
            new_tag_names: Set[str] = set()
            istags: List[Dict] = []
            incomplete_payload_update: bool = False

            if runtime.images or runtime.exclude:
                # If images are being explicitly included or excluded, assume we will not be
                # performing a full replacement of the imagestream content. This flag
                # instructs the update to not remove existing tags from the imagestream.
                incomplete_payload_update = True

            for payload_tag_name, payload_entry in entries.items():
                new_tag_names.add(payload_tag_name)

                if payload_tag_name not in multi_specs[private_mode]:
                    multi_specs[private_mode][payload_tag_name] = dict()

                if payload_entry.build_inspector and payload_entry.build_inspector.is_under_embargo() and private_mode is False:
                    # No embargoed images should go to the public release controller. Setting this boolean signals
                    # the applier logic that it should try to preserve any old tag names in the imagestream.
                    incomplete_payload_update = True
                else:
                    istags.append(PayloadGenerator.build_payload_istag(payload_tag_name, payload_entry))
                    multi_specs[private_mode][payload_tag_name][arch] = payload_entry

                with output_path.joinpath(f"updated-tags-for.{imagestream_namespace}.{imagestream_name}{'-partial' if incomplete_payload_update else ''}.yaml").open("w+", encoding="utf-8") as out_file:
                    istream_spec = PayloadGenerator.build_payload_imagestream(imagestream_name, imagestream_namespace, istags, assembly_issues)
                    yaml.safe_dump(istream_spec, out_file, indent=2, default_flow_style=False)

            if apply:
                with oc.project(imagestream_namespace):
                    is_apiobj = oc.selector(f'imagestream/{imagestream_name}').object(ignore_not_found=True)
                    if not is_apiobj:
                        # If the stream has not been bootstrapped, create it.
                        oc.create({
                            'apiVersion': 'image.openshift.io/v1',
                            'kind': 'ImageStream',
                            'metadata': {
                                'name': imagestream_name,
                                'namespace': imagestream_namespace
                            }
                        })
                        is_apiobj = oc.selector(f'imagestream/{imagestream_name}').object()

                    pruning_tags = []
                    adding_tags = []

                    def update_single_arch_istags(apiobj: oc.APIObject):
                        nonlocal pruning_tags
                        nonlocal adding_tags

                        incoming_tag_names = set([istag['name'] for istag in istags])
                        existing_tag_names = set([istag['name'] for istag in apiobj.model.spec.tags])

                        if incomplete_payload_update:
                            # If our `istags` don't necessarily include everything in the release,
                            # we need to preserve old tag values.
                            for istag in apiobj.model.spec.tags:
                                if istag.name not in incoming_tag_names:
                                    istags.append(istag)
                        else:
                            # Else, we believe the assembled tags are canonical. Compute
                            # old tags and new tags.
                            pruning_tags = existing_tag_names.difference(incoming_tag_names)
                            adding_tags = incoming_tag_names.difference(existing_tag_names)

                        apiobj.model.spec.tags = istags

                    modify_and_replace_api_object(is_apiobj, update_single_arch_istags, output_path, moist_run)

                    if pruning_tags:
                        logger.warning(f'The following tag names are no longer part of the release and will be pruned in {imagestream_namespace}:{imagestream_name}: {pruning_tags}')
                        # Even though we have replaced the .spec on the imagestream, the old tag will still be reflected in .status.
                        # The release controller considers this a legit declaration, so we must remove it explicitly using `oc delete istag`
                        if not moist_run:
                            for old_tag in pruning_tags:
                                try:
                                    oc.selector(f'istag/{imagestream_name}:{old_tag}').delete()
                                except:
                                    # This is not a fatal error, but may result in issues being displayed on the release controller page.
                                    logger.error(f'Unable to delete {old_tag} tag fully from {imagestream_name} imagestream in {imagestream_namespace}:\n{traceback.format_exc()}')

                    if adding_tags:
                        logger.warning(f'The following tag names are net new to {imagestream_namespace}:{imagestream_name}: {adding_tags}')

    # We now generate the artifacts to create heterogeneous release payloads. A heterogeneous or 'multi' release
    # payload is a manifest list (i.e. it consists of N release payload manifests, one for each arch). The release
    # payload manifests included in the multi-release payload manifest list are themselves composed of references to
    # manifest list based component images. For example, the `cli` istag in the release imagestream will point to
    # a manifest list composed of cli image manifests for each architecture.
    # In short, the release payload is a manifest list, and each component image referenced by each release manifest
    # is a manifest list.
    for private_mode in privacy_modes:

        if private_mode:
            # The CI image registry does not support manifest lists. Thus, we need to publish our nightly release
            # payloads to quay.io. As of this writing, we don't have a private quay repository into which we could
            # push embargoed release heterogeneous release payloads.
            red_print('PRIVATE MODE MULTI PAYLOADS ARE CURRENTLY DISABLED. WE NEED A PRIVATE QUAY REPO FOR PRIVATE MULTI RELEASE PAYLOADS')
            continue

        if not apply_multi_arch:
            break

        imagestream_name, imagestream_namespace = payload_imagestream_name_and_namespace(
            base_imagestream_name,
            base_istream_namespace,
            'multi', private_mode)

        now = datetime.datetime.now()
        multi_nightly_ts = now.strftime('%Y-%m-%d-%H%M%S')
        multi_nightly_name = f'{runtime.get_minor_version()}.0-0.nightly{go_suffix_for_arch("multi", private_mode)}-{multi_nightly_ts}'

        multi_istags: List[Dict] = list()
        for tag_name, arch_to_payload_entry in multi_specs[private_mode].items():
            # podman on rhel7.9 (like buildvm) does not support manifest lists. Instead we use a tool named
            # manifest-list which is available through epel for rhel7 can be installed directly on fedora.
            # The format for input is https://github.com/estesp/manifest-tool . Let's create some yaml input files.
            component_manifest_path = output_path.joinpath(f'{imagestream_namespace}.{tag_name}.manifest-list.yaml')
            manifests = []
            aggregate_issues = list()
            overall_manifest_hash = hashlib.sha256()
            # Ensure we create a new tag for each manifest list. Unlike images, if we push a manifest list
            # that seems to contain the same content (i.e. references the exact same manifest), it will still
            # have a different digest. This means pushing a seemingly identical manifest list to the same
            # tag will cause the original to lose the tag and be garbage collected.
            overall_manifest_hash.update(runtime.uuid.encode('utf-8'))
            for arch, payload_entry in arch_to_payload_entry.items():
                manifests.append({
                    'image': payload_entry.dest_pullspec,
                    'platform': {
                        'os': 'linux',
                        'architecture': go_arch_for_brew_arch(arch)
                    }
                })
                overall_manifest_hash.update(payload_entry.dest_pullspec.encode('utf-8'))
                if payload_entry.issues:
                    aggregate_issues.extend(payload_entry.issues)

            ml_dict = {
                # We need a unique tag for the manifest list image so that it does not get garbage collected.
                # To calculate a tag that will vary depending on the individual manifests being added,
                # we've calculated a sha256 of all the manifests being added.
                'image': f'quay.io/{organization}/{repository}:sha256-{overall_manifest_hash.hexdigest()}',
                'manifests': manifests
            }

            with component_manifest_path.open(mode='w+') as ml:
                yaml.safe_dump(ml_dict, stream=ml, default_flow_style=False)

            output_pullspec = ml_dict['image']
            # Give the push a try
            exectools.cmd_assert(f'manifest-tool push from-spec {str(component_manifest_path)}', retries=3)
            # if we are actually pushing a manifest list, then we should derive a sha256 based pullspec
            output_registry_org_repo = output_pullspec.rsplit(':')[0]  # e.g. quay.io/openshift-release-dev/ocp-v4.0-art-dev:sha256-b056..84b-ml -> quay.io/openshift-release-dev/ocp-v4.0-art-dev
            output_pullspec = output_registry_org_repo + '@' + find_manifest_list_sha(output_pullspec)  # create a sha based pullspec for the new manifest list

            multi_istags.append(PayloadGenerator.build_payload_istag(tag_name, PayloadGenerator.PayloadEntry(
                dest_pullspec=output_pullspec,
                issues=aggregate_issues
            )))

        # For multi-arch, we do not rely on the 4.x-art-latest update to trigger a nightly, we must create
        # it ourselves.
        # multi_release_is contains istags which all point to manifest lists. We must run oc adm release new
        # on this is once for each arch, and then stitch those images together into a release payload manifest
        # list.
        multi_release_is = PayloadGenerator.build_payload_imagestream(multi_nightly_name, imagestream_namespace, multi_istags, assembly_wide_inconsistencies=assembly_issues)
        multi_release_is_path = output_path.joinpath(f'{multi_nightly_name}-release-imagestream.yaml')
        with multi_release_is_path.open(mode='w+') as mf:
            yaml.safe_dump(multi_release_is, mf)

        multi_release_dest = f'quay.io/{organization}/{release_repository}:{multi_nightly_name}'
        arch_release_dests: Dict[str, str] = dict()  # Maps arch names to a release payload definition specific for that arch (i.e. based on the arch's CVO image)
        for arch, payload_entry in multi_specs[private_mode]['cluster-version-operator'].items():
            cvo_pullspec = payload_entry.dest_pullspec
            arch_release_dest = f'{multi_release_dest}-{arch}'
            arch_release_dests[arch] = arch_release_dest
            if apply_multi_arch:
                # If we are applying, actually create the arch specific release payload containing tags pointing to manifest list component images.
                exectools.cmd_assert(f'oc adm release new --name={multi_nightly_name} --reference-mode=source --from-image-stream-file={str(multi_release_is_path)} --to-image-base={cvo_pullspec} --to-image={arch_release_dest}')

        # Create manifest list spec containing references to all the arch specific release payloads we've created
        manifests = []
        ml_dict = {
            'image': f'{multi_release_dest}',
            'manifests': manifests
        }
        for arch, arch_release_payload in arch_release_dests.items():
            manifests.append({
                'image': arch_release_payload,
                'platform': {
                    'os': 'linux',
                    'architecture': go_arch_for_brew_arch(arch)
                }
            })

        release_payload_ml_path = output_path.joinpath(f'{multi_nightly_name}.manifest-list.yaml')
        with release_payload_ml_path.open(mode='w+') as ml:
            yaml.safe_dump(ml_dict, stream=ml, default_flow_style=False)

        # Give the push a try
        exectools.cmd_assert(f'manifest-tool push from-spec {str(release_payload_ml_path)}', retries=3)
        # if we are actually pushing a manifest list, then we should derive a sha256 based pullspec
        output_registry_org_repo = multi_release_dest.rsplit(':')[0]   # e.g. quay.io/openshift-release-dev/ocp-release-nightly
        final_multi_pullspec = output_registry_org_repo + '@' + find_manifest_list_sha(multi_release_dest)  # create a sha based pullspec for the new manifest list
        logger.info(f'The final pull_spec for the multi release payload is: {final_multi_pullspec}')

        with oc.project(imagestream_namespace):
            multi_art_latest_is = oc.selector(f'imagestream/{imagestream_name}').object(ignore_not_found=True)
            if not multi_art_latest_is:
                # If the stream has not been bootstrapped, create it.
                oc.create({
                    'apiVersion': 'image.openshift.io/v1',
                    'kind': 'ImageStream',
                    'metadata': {
                        'name': imagestream_name,
                        'namespace': imagestream_namespace
                    }
                })
                multi_art_latest_is = oc.selector(f'imagestream/{imagestream_name}').object()

            def add_multi_nightly_release(obj: oc.APIObject):
                m = obj.model
                if m.spec.tags is oc.Missing:
                    m.spec['tags'] = []

                # For normal 4.x-art-latest, we update the imagestream with individual component images and the release
                # controller formulates the nightly. For multi-arch, this is not possible (notably, the CI internal
                # registry does not support manifest lists). Instead, in the ocp-multi namespace, the 4.x-art-latest
                # imagestreams are configured `as: Stable`: https://github.com/openshift/release/pull/24130 .
                # This means the release controller treats entries in these imagestreams the same way it treats
                # it when ART tags into is/release; i.e. it treats it as an official release.
                # With this comes the responsibility to prune nightlies ourselves.
                release_tags: List = m.spec['tags']
                while len(release_tags) > 5:
                    release_tags.pop(0)

                # Now append a tag for our new nightly.
                release_tags.append({
                    'from': {
                        'kind': 'DockerImage',
                        'name': final_multi_pullspec,
                    },
                    'referencePolicy': {
                        'type': 'Source'
                    },
                    'name': multi_nightly_name,
                    'annotations': {
                        'release.openshift.io/rewrite': 'false',  # Prevents the release controller from trying to create a local registry release payload with oc adm release new.
                        # 'release.openshift.io/name': f'{runtime.get_minor_version()}.0-0.nightly',
                    }
                })
                return True

            modify_and_replace_api_object(multi_art_latest_is, add_multi_nightly_release, output_path, moist_run)

    if not overall_permitted:
        red_print('DO NOT PROCEED WITH THIS ASSEMBLY PAYLOAD -- not all detected issues are permitted.', file=sys.stderr)
        exit(1)

    exit(0)


class PayloadGenerator:

    class PayloadEntry(NamedTuple):

        # The destination pullspec
        dest_pullspec: str

        # Append any issues for the assembly
        issues: List[AssemblyIssue]

        """
        If the entry is for an image in this doozer group, these values will be set.
        """
        # The image metadata which associated with the payload
        image_meta: Optional[ImageMetadata] = None
        # An inspector associated with the overall brew build (manifest list) found for the release
        build_inspector: Optional[BrewBuildImageInspector] = None
        # The brew build archive (arch specific image) that should be tagged into the payload
        archive_inspector: Optional[ArchiveImageInspector] = None

        """
        If the entry is for machine-os-content, this value will be set
        """
        rhcos_build: Optional[RHCOSBuildInspector] = None

    @staticmethod
    def find_mismatched_siblings(build_image_inspectors: Iterable[Optional[BrewBuildImageInspector]]) -> List[Tuple[BrewBuildImageInspector, BrewBuildImageInspector]]:
        """
        Sibling images are those built from the same repository. We need to throw an error
        if there are sibling built from different commits.
        :return: Returns a list of (BrewBuildImageInspector,BrewBuildImageInspector) where the first item is a mismatched sibling of the second
        """
        class RepoBuildRecord(NamedTuple):
            build_image_inspector: BrewBuildImageInspector
            source_git_commit: str

        # Maps SOURCE_GIT_URL -> RepoBuildRecord(SOURCE_GIT_COMMIT, DISTGIT_KEY, NVR). Where the Tuple is the first build
        # encountered claiming it is sourced from the SOURCE_GIT_URL
        repo_builds: Dict[str, RepoBuildRecord] = dict()

        mismatched_siblings: List[Tuple[BrewBuildImageInspector, BrewBuildImageInspector]] = []
        for build_image_inspector in build_image_inspectors:

            if not build_image_inspector:
                # No build for this component at present.
                continue

            # Here we check the raw config - before it is affected by assembly overrides. Why?
            # If an artist overrides one sibling's git url, but not another, the following
            # scan would not be able to detect that they were siblings. Instead, we rely on the
            # original image metadata to determine sibling-ness.
            source_url = build_image_inspector.get_image_meta().raw_config.content.source.git.url

            source_git_commit = build_image_inspector.get_source_git_commit()
            if not source_url or not source_git_commit:
                # This is true for distgit only components.
                continue

            # Make sure URLs are comparable regardless of git: or https:
            source_url = convert_remote_git_to_https(source_url)

            potential_conflict: RepoBuildRecord = repo_builds.get(source_url, None)
            if potential_conflict:
                # Another component has build from this repo before. Make
                # sure it built from the same commit.
                if potential_conflict.source_git_commit != source_git_commit:
                    mismatched_siblings.append((build_image_inspector, potential_conflict.build_image_inspector))
                    red_print(f"The following NVRs are siblings but built from different commits: {potential_conflict.build_image_inspector.get_nvr()} and {build_image_inspector.get_nvr()}", file=sys.stderr)
            else:
                # No conflict, so this is our first encounter for this repo; add it to our tracking dict.
                repo_builds[source_url] = RepoBuildRecord(build_image_inspector=build_image_inspector, source_git_commit=source_git_commit)

        return mismatched_siblings

    @staticmethod
    def find_rhcos_build_rpm_inconsistencies(rhcos_builds: List[RHCOSBuildInspector]) -> Dict[str, List[str]]:
        """
        Looks through a set of RHCOS builds and finds if any of those builds contains a package version that
        is inconsistent with the same package in another RHCOS build.
        :return: Returns Dict[inconsistent_rpm_name] -> [inconsistent_nvrs, ...]. The Dictionary will be empty
                 if there are no inconsistencies detected.
        """
        rpm_uses: Dict[str, Set[str]] = {}

        for rhcos_build in rhcos_builds:
            for nvr in rhcos_build.get_rpm_nvrs():
                rpm_name = parse_nvr(nvr)['name']
                if rpm_name not in rpm_uses:
                    rpm_uses[rpm_name] = set()
                rpm_uses[rpm_name].add(nvr)

        # Report back rpm name keys which were associated with more than one NVR in the set of RHCOS builds.
        return {rpm_name: nvr_list for rpm_name, nvr_list in rpm_uses.items() if len(nvr_list) > 1}

    @staticmethod
    def get_mirroring_destination(archive_inspector: ArchiveImageInspector, dest_repo: str) -> str:
        """
        :param archive_inspector: The archive to analyze for mirroring.
        :param dest_repo: A pullspec to mirror to, except for the tag. This include registry, organization, and repo.
        :return: Returns the external (quay) image location to which this image should be mirrored in order
                 to be included in an nightly release payload. These tags are meant to leak no information
                 to users watching the quay repo. The image must have a tag or it will be garbage collected.
        """
        tag = archive_inspector.get_archive_digest().replace(":", "-")  # sha256:abcdef -> sha256-abcdef
        return f"{dest_repo}:{tag}"

    @staticmethod
    def find_payload_entries(assembly_inspector: AssemblyInspector, arch: str, dest_repo: str) -> Dict[str, PayloadEntry]:
        """
        Returns a list of images which should be included in the architecture specific release payload.
        This includes images for our group's image metadata as well as machine-os-content.
        :param assembly_inspector: An analyzer for the assembly to generate entries for.
        :param arch: The brew architecture name to create the list for.
        :param dest_repo: The registry/org/repo into which the image should be mirrored.
        :return: Map[payload_tag_name] -> PayloadEntry.
        """
        members: Dict[str, Optional[PayloadGenerator.PayloadEntry]] = dict()  # Maps release payload tag name to the PayloadEntry for the image.
        for payload_tag, archive_inspector in PayloadGenerator.get_group_payload_tag_mapping(assembly_inspector, arch).items():
            if not archive_inspector:
                # There is no build for this payload tag for this CPU arch. This
                # will be filled in later in this method for the final list.
                members[payload_tag] = None
                continue

            members[payload_tag] = PayloadGenerator.PayloadEntry(
                image_meta=archive_inspector.get_image_meta(),
                build_inspector=archive_inspector.get_brew_build_inspector(),
                archive_inspector=archive_inspector,
                dest_pullspec=PayloadGenerator.get_mirroring_destination(archive_inspector, dest_repo),
                issues=list(),
            )

        # members now contains a complete map of payload tag keys, but some values may be None. This is an
        # indication that the architecture did not have a build of one of our group images.
        # The tricky bit is that all architecture specific release payloads contain the same set of tags
        # or 'oc adm release new' will have trouble assembling it. i.e. an imagestream tag 'X' may not be
        # necessary on s390x, bit we need to populate that tag with something.

        # To do this, we replace missing images with the 'pod' image for the architecture. This should
        # be available for every CPU architecture. As such, we must find pod to proceed.

        pod_entry = members.get('pod', None)
        if not pod_entry:
            raise IOError(f'Unable to find pod image archive for architecture: {arch}; unable to construct payload')

        final_members: Dict[str, PayloadGenerator.PayloadEntry] = dict()
        for tag_name, entry in members.items():
            if entry:
                final_members[tag_name] = entry
            else:
                final_members[tag_name] = pod_entry

        rhcos_build: RHCOSBuildInspector = assembly_inspector.get_rhcos_build(arch)
        final_members['machine-os-content'] = PayloadGenerator.PayloadEntry(
            dest_pullspec=rhcos_build.get_image_pullspec(),
            rhcos_build=rhcos_build,
            issues=list(),
        )

        # Final members should have all tags populated.
        return final_members

    @staticmethod
    def build_payload_istag(payload_tag_name: str, payload_entry: PayloadEntry) -> Dict:
        """
        :param payload_tag_name: The name of the payload tag for which to create an istag.
        :param payload_entry: The payload entry to serialize into an imagestreamtag.
        :return: Returns a imagestreamtag dict for a release payload imagestream.
        """
        return {
            'annotations': PayloadGenerator._build_inconsistency_annotation(payload_entry.issues),
            'name': payload_tag_name,
            'from': {
                'kind': 'DockerImage',
                'name': payload_entry.dest_pullspec,
            }
        }

    @staticmethod
    def build_payload_imagestream(imagestream_name: str, imagestream_namespace: str, payload_istags: Iterable[Dict], assembly_wide_inconsistencies: Iterable[AssemblyIssue]) -> Dict:
        """
        Builds a definition for a release payload imagestream from a set of payload istags.
        :param imagestream_name: The name of the imagstream to generate.
        :param imagestream_namespace: The nemspace in which the imagestream should be created.
        :param payload_istags: A list of istags generated by build_payload_istag.
        :param assembly_wide_inconsistencies: Any inconsistency information to embed in the imagestream.
        :return: Returns a definition for an imagestream for the release payload.
        """

        istream_obj = {
            'kind': 'ImageStream',
            'apiVersion': 'image.openshift.io/v1',
            'metadata': {
                'name': imagestream_name,
                'namespace': imagestream_namespace,
                'annotations': PayloadGenerator._build_inconsistency_annotation(assembly_wide_inconsistencies)
            },
            'spec': {
                'tags': list(payload_istags),
            }
        }

        return istream_obj

    @staticmethod
    def _build_inconsistency_annotation(inconsistencies: Iterable[AssemblyIssue]):
        """
        :param inconsistencies: A list of strings to report as inconsistencies within an annotation.
        :return: Returns a dict containing an inconsistency annotation out of the specified str.
                 Returns emtpy {} if there are no inconsistencies in the parameter.
        """
        # given a list of strings, build the annotation for inconsistencies
        if not inconsistencies:
            return {}

        msgs = sorted([i.msg for i in inconsistencies])
        if len(msgs) > 5:
            # an exhaustive list of the problems may be too large; that goes in the state file.
            msgs[5:] = ["(...and more)"]
        return {"release.openshift.io/inconsistency": json.dumps(msgs)}

    @staticmethod
    def get_group_payload_tag_mapping(assembly_inspector: AssemblyInspector, arch: str) -> Dict[str, Optional[ArchiveImageInspector]]:
        """
        Each payload tag name used to map exactly to one release imagemeta. With the advent of '-alt' images,
        we need some logic to determine which images map to which payload tags for a given architecture.
        :return: Returns a map[payload_tag_name] -> ArchiveImageInspector containing an image for the payload. The value may be
                 None if there is no arch specific build for the tag. This does not include machine-os-content since that
                 is not a member of the group.
        """
        brew_arch = brew_arch_for_go_arch(arch)  # Make certain this is brew arch nomenclature
        members: Dict[str, Optional[ArchiveImageInspector]] = dict()  # Maps release payload tag name to the archive which should populate it
        for dgk, build_inspector in assembly_inspector.get_group_release_images().items():

            if build_inspector is None:
                # There was no build for this image found associated with the assembly.
                # In this case, don't put the tag_name into the imagestream. This is not good,
                # so be verbose.
                red_print(f'Unable to find build for {dgk} for {assembly_inspector.get_assembly_name()}', file=sys.stderr)
                continue

            image_meta: ImageMetadata = assembly_inspector.runtime.image_map[dgk]

            if not image_meta.is_payload:
                # Nothing to do for images which are not in the payload
                continue

            tag_name, explicit = image_meta.get_payload_tag_info()  # The tag that will be used in the imagestreams and whether it was explicitly declared.

            if arch not in image_meta.get_arches():
                # If this image is not meant for this architecture
                if tag_name not in members:
                    members[tag_name] = None  # We still need a placeholder in the tag mapping
                continue

            if members.get(tag_name, None) and not explicit:
                # If we have already found an entry, there is a precedence we honor for
                # "-alt" images. Specifically, if a imagemeta declares its payload tag
                # name explicitly, it will take precedence over any other entries
                # https://issues.redhat.com/browse/ART-2823
                # This was tag not explicitly declared, so ignore the duplicate image.
                continue

            archive_inspector = build_inspector.get_image_archive_inspector(brew_arch)

            if not archive_inspector:
                # There is no build for this CPU architecture for this image_meta/build. This finding
                # conflicts with the `arch not in image_meta.get_arches()` check above.
                # Best to fail.
                raise IOError(f'{dgk} claims to be built for {image_meta.get_arches()} but did not find {brew_arch} build for {build_inspector.get_brew_build_webpage_url()}')

            members[tag_name] = archive_inspector

        return members

    @staticmethod
    def _check_nightly_consistency(assembly_inspector: AssemblyInspector, nightly: str, arch: str) -> List[AssemblyIssue]:
        runtime = assembly_inspector.runtime

        def terminal_issue(msg: str) -> List[AssemblyIssue]:
            return [AssemblyIssue(msg, component='reference-releases')]

        issues: List[str]
        runtime.logger.info(f'Processing nightly: {nightly}')
        major_minor, brew_cpu_arch, priv = isolate_nightly_name_components(nightly)

        if major_minor != runtime.get_minor_version():
            return terminal_issue(f'Specified nightly {nightly} does not match group major.minor')

        rc_suffix = go_suffix_for_arch(brew_cpu_arch, priv)

        retries: int = 3
        release_json_str = ''
        rc = -1
        pullspec = f'registry.ci.openshift.org/ocp{rc_suffix}/release{rc_suffix}:{nightly}'
        while retries > 0:
            rc, release_json_str, err = exectools.cmd_gather(f'oc adm release info {pullspec} -o=json')
            if rc == 0:
                break
            runtime.logger.warn(f'Error accessing nightly release info for {pullspec}:  {err}')
            retries -= 1

        if rc != 0:
            return terminal_issue(f'Unable to gather nightly release info details: {pullspec}; garbage collected?')

        release_info = Model(dict_to_model=json.loads(release_json_str))
        if not release_info.references.spec.tags:
            return terminal_issue(f'Could not find tags in nightly {nightly}')

        issues: List[AssemblyIssue] = list()
        payload_entries: Dict[str, PayloadGenerator.PayloadEntry] = PayloadGenerator.find_payload_entries(assembly_inspector, arch, '')
        for component_tag in release_info.references.spec.tags:  # For each tag in the imagestream
            payload_tag_name: str = component_tag.name  # e.g. "aws-ebs-csi-driver"
            payload_tag_pullspec: str = component_tag['from'].name  # quay pullspec
            if '@' not in payload_tag_pullspec:
                # This speaks to an invalid nightly, so raise and exception
                raise IOError(f'Expected pullspec in {nightly}:{payload_tag_name} to be sha digest but found invalid: {payload_tag_pullspec}')

            pullspec_sha = payload_tag_pullspec.rsplit('@', 1)[-1]
            entry = payload_entries.get(payload_tag_name, None)

            if not entry:
                raise IOError(f'Did not find {nightly} payload tag {payload_tag_name} in computed assembly payload')

            if entry.archive_inspector:
                if entry.archive_inspector.get_archive_digest() != pullspec_sha:
                    # Impermissible because the artist should remove the reference nightlies from the assembly definition
                    issues.append(AssemblyIssue(f'{nightly} contains {payload_tag_name} sha {pullspec_sha} but assembly computed archive: {entry.archive_inspector.get_archive_id()} and {entry.archive_inspector.get_archive_pullspec()}',
                                                component='reference-releases'))
            elif entry.rhcos_build:
                if entry.rhcos_build.get_machine_os_content_digest() != pullspec_sha:
                    # Impermissible because the artist should remove the reference nightlies from the assembly definition
                    issues.append(AssemblyIssue(f'{nightly} contains {payload_tag_name} sha {pullspec_sha} but assembly computed rhcos: {entry.rhcos_build} and {entry.rhcos_build.get_machine_os_content_digest()}',
                                                component='reference-releases'))
            else:
                raise IOError(f'Unsupported payload entry {entry}')

        return issues

    @staticmethod
    def check_nightlies_consistency(assembly_inspector: AssemblyInspector) -> List[AssemblyIssue]:
        """
        If this assembly has reference-releases, check whether the current images selected by the
        assembly are an exact match for the nightly contents.
        """
        basis = assembly_basis(assembly_inspector.runtime.get_releases_config(), assembly_inspector.runtime.assembly)
        if not basis or not basis.reference_releases:
            return []

        issues: List[AssemblyIssue] = []
        for arch, nightly in basis.reference_releases.primitive().items():
            issues.extend(PayloadGenerator._check_nightly_consistency(assembly_inspector, nightly, arch))

        return issues
