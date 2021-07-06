import io
import json
from typing import List, Optional, Tuple, Set

import click
import yaml
from koji import ClientSession

from doozerlib import brew, build_status_detector, exectools, rhcos, state
from doozerlib import assembly
from doozerlib.cli import cli, pass_runtime
from doozerlib.exceptions import DoozerFatalError
from doozerlib.image import ImageMetadata
from doozerlib.model import Model
from doozerlib.runtime import Runtime
from doozerlib.util import find_latest_build, go_suffix_for_arch, red_print, yellow_print


@cli.command("release:gen-payload", short_help="Generate input files for release mirroring")
@click.option("--is-name", metavar='NAME', required=False,
              help="ImageStream .metadata.name value. For example '4.2-art-latest'")
@click.option("--is-namespace", metavar='NAMESPACE', required=False,
              help="ImageStream .metadata.namespace value. For example 'ocp'")
@click.option("--organization", metavar='ORGANIZATION', required=False, default='openshift-release-dev',
              help="Quay ORGANIZATION to mirror into.\ndefault=openshift-release-dev")
@click.option("--repository", metavar='REPO', required=False, default='ocp-v4.0-art-dev',
              help="Quay REPOSITORY in ORGANIZATION to mirror into.\ndefault=ocp-v4.0-art-dev")
@click.option("--exclude-arch", metavar='ARCH', required=False, multiple=True,
              help="Architecture (brew nomenclature) to exclude from payload generation")
@click.option("--skip-gc-tagging", default=False, is_flag=True,
              help="By default, for a named assembly, images will be tagged to prevent garbage collection")
@pass_runtime
def release_gen_payload(runtime, is_name, is_namespace, organization, repository, exclude_arch, skip_gc_tagging):
    """Generates two sets of input files for `oc` commands to mirror
content and update image streams. Files are generated for each arch
defined in ocp-build-data for a version, as well as a final file for
manifest-lists.

One set of files are SRC=DEST mirroring definitions for 'oc image
mirror'. They define what source images we will sync to which
destination repos, and what the mirrored images will be labeled as.

The other set of files are YAML image stream tags for 'oc
apply'. Those are applied to an openshift cluster to define "release
streams". When they are applied the release controller notices the
update and begins generating a new payload with the images tagged in
the image stream.

For automation purposes this command generates a mirroring yaml files
after the arch-specific files have been generated. The yaml files
include names of generated content.

You may provide the namespace and base name for the image streams, or defaults
will be used. The generated files will append the -arch and -priv suffixes to
the given name and namespace as needed.

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
    runtime.initialize(clone_distgits=False)
    brew_session = runtime.build_retrying_koji_client()
    base_target = SyncTarget(  # where we will mirror and record the tags
        orgrepo=f"{organization}/{repository}",
        istream_name=is_name if is_name else default_is_base_name(runtime),
        istream_namespace=is_namespace if is_namespace else default_is_base_namespace()
    )

    if runtime.assembly and runtime.assembly != 'stream' and 'art-latest' in base_target.istream_name:
        raise ValueError('The art-latest imagestreams should not be used for non-stream assemblies')

    gen = PayloadGenerator(runtime, brew_session, base_target, exclude_arch, skip_gc_tagging=skip_gc_tagging)
    latest_builds, invalid_name_items, images_missing_builds, mismatched_siblings, non_release_items = gen.load_latest_builds()
    gen.write_mirror_destinations(latest_builds, mismatched_siblings)

    if non_release_items:
        yellow_print("Images skipped due to non_release tag:")
        for img in sorted(non_release_items):
            click.echo("   {}".format(img))

    if invalid_name_items:
        yellow_print("Images skipped due to invalid naming:")
        for img in sorted(invalid_name_items):
            click.echo("   {}".format(img))

    if images_missing_builds:
        yellow_print("No builds found for:")
        for img in sorted(images_missing_builds, key=lambda img: img.image_name_short):
            click.echo("   {}".format(img.image_name_short))

    if mismatched_siblings:
        yellow_print("Images skipped due to siblings mismatch:")
        for img in sorted(mismatched_siblings):
            click.echo("   {}".format(img))


class SyncTarget(object):
    def __init__(self, orgrepo=None, istream_name=None, istream_namespace=None):
        self.orgrepo = orgrepo
        self.istream_name = istream_name
        self.istream_namespace = istream_namespace


class BuildRecord(object):
    def __init__(self, image=None, build=None, archives=None, private=False):
        self.image = image
        self.build = build
        self.archives = archives
        self.private = private


class PayloadGenerator:
    def __init__(self, runtime: Runtime, brew_session: ClientSession, base_target: SyncTarget, exclude_arches: Optional[List[str]] = None, skip_gc_tagging: bool = False):
        self.runtime = runtime
        self.brew_session = brew_session
        self.base_target = base_target
        self.exclude_arches = exclude_arches or []
        self.state = runtime.state[runtime.command] = dict(state.TEMPLATE_IMAGE)
        self.bs_detector = build_status_detector.BuildStatusDetector(brew_session, runtime.logger)
        self.skip_gc_tagging = skip_gc_tagging

    def load_latest_builds(self):
        images = list(self.runtime.image_metas())
        self.state['total_images'] = len(images)

        self.runtime.logger.info("Fetching latest image builds from Brew...")
        payload_images, invalid_name_items = self._get_payload_images(images)
        release_payload_images, non_release_items = self._get_payload_and_non_release_images(payload_images)
        self.state['payload_images'] = len(release_payload_images)

        latest_builds, images_missing_builds = self._get_latest_builds(release_payload_images)
        self.runtime.logger.info("Determining if image builds have embargoed contents...")
        self._designate_privacy(latest_builds, images)

        mismatched_siblings = self._find_mismatched_siblings(latest_builds)
        if mismatched_siblings and self.runtime.assembly_type == assembly.AssemblyTypes.CUSTOM:
            self.runtime.logger.warning(f'There are mismatched siblings in this assembly, but it is "custom"; ignoring: {mismatched_siblings}')
            mismatched_siblings = set()

        return latest_builds, invalid_name_items, images_missing_builds, mismatched_siblings, non_release_items

    def _get_payload_and_non_release_images(self, images):
        payload_images = []
        non_release_items = []
        for image in images:
            if image.for_release:
                payload_images.append(image)
                continue
            non_release_items.append(image.image_name_short)
            red_print(f"NOT adding to IS (non_release: true): {image.image_name_short}")

        return payload_images, non_release_items

    def _get_payload_images(self, images: List[ImageMetadata]) -> Tuple[List[ImageMetadata], List[ImageMetadata]]:
        # Iterates through a list of image metas and finds those destined for
        # the release payload. Images which are marked for the payload but not named
        # appropriately will be captured in a separate list.
        # :param images: The list of metas to scan
        # :return: Returns a tuple containing: (list of images for payload, list of incorrectly named images)

        payload_images = []
        invalid_name_items = []
        for image in images:
            if image.is_payload:
                """
                <Tim Bielawa> note to self: is only for `ose-` prefixed images
                <Clayton Coleman> Yes, Get with the naming system or get out of town
                """
                if image.image_name_short.startswith("ose-"):
                    payload_images.append(image)
                    continue

                invalid_name_items.append(image.image_name_short)
                red_print(f"NOT adding to IS (does not meet name/version conventions): {image.image_name_short}")

        return payload_images, invalid_name_items

    def _get_latest_builds(self, payload_images: List[ImageMetadata]) -> Tuple[List[BuildRecord], List[ImageMetadata]]:
        """
        Find latest brew build (at event, if given) of each payload image.

        If assemblies are disabled, it will return the latest tagged brew build with the candidate brew tag.
        If assemblies are enabled, it will return the latest tagged brew build in the given assembly.
          If no such build, it will fall back to the latest build in "stream" assembly.

        :param payload_images: a list of image metadata for payload images
        :return: list of build records, list of images missing builds
        """
        brew_latest_builds = []
        for image_meta in payload_images:
            latest_build: ImageMetadata = image_meta.get_latest_build()
            if self.runtime.assembly_basis_event and not self.skip_gc_tagging:
                # If we are preparing an assembly with a basis event, let's start getting
                # serious and tag these images so they don't get garbage collected.
                with self.runtime.shared_koji_client_session() as koji_api:
                    build_nvr = latest_build['nvr']
                    tags = {tag['name'] for tag in koji_api.listTags(build=build_nvr)}
                    if image_meta.hotfix_brew_tag() not in tags:
                        self.runtime.logger.info(f'Tagging {image_meta.get_component_name()} build {build_nvr} with {image_meta.hotfix_brew_tag()} to prevent garbage collection')
                        koji_api.tagBuild(image_meta.hotfix_brew_tag(), build_nvr)

            brew_latest_builds.append(latest_build)

        # look up the archives for each image (to get the RPMs that went into them)
        brew_build_ids = [b["id"] if b else 0 for b in brew_latest_builds]
        archives_list = brew.list_archives_by_builds(brew_build_ids, "image", self.brew_session)

        # at this point payload_images, brew_latest_builds, and archives_list should be matching lists;
        # combine them into dict build records.
        latest_builds, missing_images = [], []
        for image, build, archives in zip(payload_images, brew_latest_builds, archives_list):
            if build and archives:
                latest_builds.append(BuildRecord(image, build, archives))
            else:
                missing_images.append(image)
                state.record_image_fail(self.state, image, f"Unable to find build for: {image.image_name_short}", self.runtime.logger)

        self.state["builds_missing"] = len(missing_images)
        self.state["builds_found"] = len(latest_builds)
        return latest_builds, missing_images

    def _designate_privacy(self, latest_builds, images):
        """
        For a list of build records, determine if they have private contents. If
        so, then set "private" to True for that build record. This is done for a
        whole set of builds since we have to look up parents and we would like to
        cache those lookups.
        """
        if not self.runtime.group_config.public_upstreams:
            # when public_upstreams are not configured, we assume there is no private content.
            return

        if self.runtime.assembly_basis_event:
            # If an assembly has a basis event, its content is not going to go out
            # to a release controller. Nothing we write is going to be publicly
            # available.
            return

        # store RPM archives to BuildStatusDetector cache to limit Brew queries
        for r in latest_builds:
            self.bs_detector.archive_lists[r.build["id"]] = r.archives

        # determine if each image build is embargoed (or otherwise "private")
        embargoed_build_ids = self.bs_detector.find_embargoed_builds(
            [r.build for r in latest_builds],
            {image.candidate_brew_tag() for image in images}
        )
        for r in latest_builds:
            if r.build["id"] in embargoed_build_ids:
                r.private = True

    def write_mirror_destinations(self, latest_builds, mismatched_siblings):
        self.runtime.logger.info("Creating mirroring lists...")

        # returns map[(brew_arch, private)] -> map[image_name] -> { version: release: image_src: digest: build_record: }
        mirror_src_for_arch_and_name = self._get_mirror_sources(latest_builds, mismatched_siblings)

        # we need to evaluate rhcos inconsistencies across architectures (separate builds)
        rhcos_source_for_priv_arch = {False: {}, True: {}}  # map[private][brew_arch] -> source
        for brew_arch, private in mirror_src_for_arch_and_name.keys():
            rhcos_source_for_priv_arch[private][brew_arch] = self._latest_mosc_source(brew_arch, private)
        rhcos_inconsistencies = {  # map[private] -> map[annotation] -> description
            private: self._find_rhcos_build_inconsistencies(rhcos_source_for_priv_arch[private]) for private in (True, False)
        }

        for dest, source_for_name in mirror_src_for_arch_and_name.items():
            brew_arch, private = dest

            if self.runtime.assembly_basis_event and private:
                self.runtime.logger.info(f"Skipping private mirroring list / imagestream for asssembly: {self.runtime.assembly}")
                continue

            dest = f"{brew_arch}{'-priv' if private else ''}"

            # Save the default SRC=DEST input to a file for syncing by 'oc image mirror'
            with io.open(f"src_dest.{dest}", "w+", encoding="utf-8") as out_file:
                for source in source_for_name.values():
                    mirror_dest = self._build_dest_name(source, self.base_target.orgrepo)
                    out_file.write(f"{source['image_src']}={mirror_dest}\n")

            # build the local tag target from the base
            name, namespace = is_name_and_space(
                self.base_target.istream_name,
                self.base_target.istream_namespace,
                brew_arch, private)
            target = SyncTarget(self.base_target.orgrepo, name, namespace)
            x86_source_for_name = mirror_src_for_arch_and_name[('x86_64', private)]
            istream_spec = self._get_istream_spec(
                brew_arch, private, target,
                source_for_name, x86_source_for_name,
                rhcos_source_for_priv_arch[private][brew_arch], rhcos_inconsistencies[private]
            )
            with io.open(f"image_stream.{dest}.yaml", "w+", encoding="utf-8") as out_file:
                yaml.safe_dump(istream_spec, out_file, indent=2, default_flow_style=False)

    def _find_rhcos_build_inconsistencies(self, rhcos_source_for_arch):
        inconsistencies = {}

        # gather a list of all rpms used in every arch of rhcos build
        nvrs_for_rpm = {}
        for brew_arch, source in rhcos_source_for_arch.items():
            if not source:  # sometimes could be missing e.g. browser outage; just note that here
                annotation = f"Could not retrieve RHCOS for {brew_arch}"
                inconsistencies[annotation] = annotation  # not much more to explain
                continue
            for rpm in source['archive']['rpms']:
                nvrs_for_rpm.setdefault(rpm['name'], set()).add(rpm['nvr'])
        for name, nvrs in nvrs_for_rpm.items():
            if len(nvrs) > 1:
                annotation = f"Multiple versions of RPM {name} used"
                description = f"RPM {name} has multiple versions across arches: {list(nvrs)}"
                inconsistencies[annotation] = description

        return inconsistencies

    def _get_mirror_sources(self, latest_builds, mismatched_siblings):
        """
        Determine the image sources to mirror to each arch-private-specific imagestream,
        excluding mismatched siblings; also record success/failure per state.

        :return: map[(brew_arch, private)] -> map[image_name] -> { version: release: image_src: digest: build_record: }
        """
        mirroring = {}
        for record in latest_builds:
            image = record.image
            error = None
            if image.distgit_key in mismatched_siblings:
                error = "Siblings built from different commits"
            else:
                # The tag that will be used in the imagestreams
                payload_name = image.config.get("payload_name")
                if payload_name:
                    tag_name = payload_name
                else:
                    tag_name = image.image_name_short[4:] if image.image_name_short.startswith("ose-") else image.image_name_short  # it _should_ but... to be safe
                for archive in record.archives:
                    brew_arch = archive["arch"]
                    if brew_arch in self.exclude_arches:
                        continue
                    pullspecs = archive["extra"]["docker"]["repositories"]
                    if not pullspecs or ":" not in pullspecs[-1]:  # in case of no pullspecs or invalid format
                        error = f"Unable to find pullspecs for: {image.image_name_short}"
                        red_print(error)
                        state.record_image_fail(self.state, image, error, self.runtime.logger)
                        continue
                    digest = archive["extra"]['docker']['digests']['application/vnd.docker.distribution.manifest.v2+json']
                    if not digest.startswith("sha256:"):  # It should start with sha256: for now. Let's raise an error if this changes.
                        raise ValueError(f"Received unrecognized digest {digest} for image {pullspecs[-1]}")

                    mirroring_value = dict(
                        version=record.build["version"],
                        release=record.build["release"],
                        image_src=pullspecs[-1],
                        digest=digest,
                        build_record=record,
                        archive=archive,
                    )

                    if record.private:  # exclude embargoed images from the ocp[-arch] imagestreams
                        yellow_print(f"Omitting embargoed image {pullspecs[-1]}")
                    else:
                        mirroring_list = mirroring.setdefault((brew_arch, False), {})
                        if tag_name not in mirroring_list or payload_name:
                            # if multiple images in arch have the same tag, only explicit payload_name overwrites (https://issues.redhat.com/browse/ART-2823
                            self.runtime.logger.info(f"Adding {brew_arch} image {pullspecs[-1]} to the public mirroring list with imagestream tag {tag_name}...")
                            mirroring_list[tag_name] = mirroring_value

                    if self.runtime.group_config.public_upstreams:
                        # when public_upstreams are configured, both embargoed and non-embargoed images should be included in the ocp[-arch]-priv imagestreams
                        mirroring_list = mirroring.setdefault((brew_arch, True), {})
                        if tag_name not in mirroring_list or payload_name:
                            # if multiple images in arch have the same tag, only explicit payload_name overwrites (https://issues.redhat.com/browse/ART-2823
                            self.runtime.logger.info(f"Adding {brew_arch} image {pullspecs[-1]} to the private mirroring list with imagestream tag {tag_name}...")
                            mirroring_list[tag_name] = mirroring_value

            # per build, record in the state whether we can successfully mirror it
            if error:
                red_print(error)
                state.record_image_fail(self.state, image, error, self.runtime.logger)
            else:
                state.record_image_success(self.state, image)

        return mirroring

    @staticmethod
    def _build_dest_name(source, orgrepo):
        tag = source["digest"].replace(":", "-")  # sha256:abcdef -> sha256-abcdef
        return f"quay.io/{orgrepo}:{tag}"

    def _get_istream_spec(self, brew_arch, private, target, source_for_name, x86_source_for_name,
                          rhcos_source, rhcos_inconsistencies):
        # Write tag specs for the image stream. The name of each tag
        # spec does not include the 'ose-' prefix. This keeps them
        # consistent between OKD and OCP.

        # Template Base Image Stream object.
        tag_list = []
        istream_spec = {
            'kind': 'ImageStream',
            'apiVersion': 'image.openshift.io/v1',
            'metadata': {
                'name': target.istream_name,
                'namespace': target.istream_namespace,
            },
            'spec': {
                'tags': tag_list,
            }
        }

        for tag_name, source in source_for_name.items():
            tag_list.append(self._get_istag_spec(tag_name, source, target))

        # add in rhcos tag
        if rhcos_source:  # missing rhcos should not prevent syncing everything else
            mosc_istag = self._get_mosc_istag_spec(rhcos_source, rhcos_inconsistencies)
            tag_list.append(mosc_istag)

        tag_list.extend(self._extra_dummy_tags(brew_arch, private, source_for_name, x86_source_for_name, target))

        return istream_spec

    def _get_istag_spec(self, tag_name, source, target):
        record = source['build_record']
        inconsistencies = self._find_rpm_inconsistencies(source['archive'], record.image.candidate_brew_tag())
        if inconsistencies:  # format {annotation: description}
            inc_state = self.state.setdefault('inconsistencies', {}).setdefault(tag_name, [])
            # de-duplicate -- most will be repeated for each arch
            inc_state.extend([val for val in inconsistencies.values() if val not in inc_state])

        return {
            'annotations': self._inconsistency_annotation(inconsistencies.keys()),
            'name': tag_name,
            'from': {
                'kind': 'DockerImage',
                'name': self._build_dest_name(source, target.orgrepo),
            }
        }

    def _find_rpm_inconsistencies(self, archive, candidate_tag):
        # returns a dict describing latest candidate rpms that are mismatched with build contents

        # N.B. the "rpms" installed in an image archive are individual RPMs, not brew rpm package builds.
        # we compare against the individual RPMs from latest candidate rpm package builds.
        candidate_rpms = {
            # the RPMs are collected by name mainly to de-duplicate (same RPM, multiple arches)
            rpm['name']: rpm for rpm in
            self.bs_detector.find_unshipped_candidate_rpms(candidate_tag, self.runtime.brew_event)
        }

        inconsistencies = {}
        archive_rpms = {rpm['name']: rpm for rpm in archive['rpms']}
        # we expect only a few unshipped candidates most of the the time, so we'll just search for those.
        for name, rpm in candidate_rpms.items():
            archive_rpm = archive_rpms.get(name)
            if archive_rpm and rpm['nvr'] != archive_rpm['nvr']:
                inconsistencies[f"Contains outdated RPM {rpm['name']}"] = (
                    f"RPM {archive_rpm['nvr']} is installed in image build {archive['build_id']} but"
                    f" {rpm['nvr']} from package build {rpm['build_id']} is latest candidate"
                )

        return inconsistencies  # {annotation: description}

    def _inconsistency_annotation(self, inconsistencies):
        # given a list of strings, build the annotation for inconsistencies
        if not inconsistencies:
            return {}

        inconsistencies = sorted(inconsistencies)
        if len(inconsistencies) > 5:
            # an exhaustive list of the problems may be too large; that goes in the state file.
            inconsistencies[5:] = ["(...and more)"]
        return {"release.openshift.io/inconsistency": json.dumps(inconsistencies)}

    def _extra_dummy_tags(self, brew_arch, private, source_for_name, x86_source_for_name, target, stand_in_tag="pod"):
        """
        For non-x86 arches, not all images are built (e.g. kuryr), but they may
        be mentioned in CVO image references. Thus, make sure there is a tag for
        every tag we find in x86_64 and provide a dummy image to stand in if needed.

        :return: a list of tag specs for the payload images not built in this arch.
        """
        tag_list = []
        if stand_in_tag in source_for_name:  # stand_in_tag image serves as the dummy image for the replacement
            extra_tags = x86_source_for_name.keys() - source_for_name.keys()
            for tag_name in extra_tags:
                yellow_print('Unable to find tag {} for arch {} ; substituting {} image'.format(tag_name, brew_arch, stand_in_tag))
                tag_list.append({
                    'name': tag_name,
                    'from': {
                        'kind': 'DockerImage',
                        'name': self._build_dest_name(source_for_name[stand_in_tag], target.orgrepo)
                    }
                })
        elif self.runtime.group_config.public_upstreams and not private:
            # If stand_in_tag is embargoed, it is expected that stand_in_tag is missing in any non *-priv imagestreams.
            self.runtime.logger.warning(f"Unable to find {stand_in_tag} tag from {brew_arch} imagestream. Is {stand_in_tag} image embargoed or out of sync with siblings?")
        else:
            # if CVE embargoes supporting is disabled or the stand_in_tag image is also
            # missing in *-priv namespaces, an error will be raised.
            raise DoozerFatalError(f"A dummy image is required for arch {brew_arch}, but {stand_in_tag} image is not available to stand in")

        return tag_list

    def _latest_mosc_source(self, brew_arch, private):
        image_stream_suffix = f"{brew_arch}{'-priv' if private else ''}"
        runtime = self.runtime
        runtime.logger.info(f"Getting latest RHCOS source for {image_stream_suffix}...")

        assembly_rhcos_config = assembly.assembly_rhcos_config(runtime.releases_config, runtime.assembly)
        # See if this assembly has assembly.rhcos.machine-os-content.images populated for this architecture.
        assembly_rhcos_arch_pullspec = assembly_rhcos_config['machine-os-content'].images[brew_arch]
        if self.runtime.assembly_basis_event and not assembly_rhcos_arch_pullspec:
            raise Exception(f'Assembly {runtime.assembly} has a basis event but no assembly.rhcos MOSC image data for {brew_arch}; all MOSC image data must be populated for this assembly to be valid')

        try:

            version = self.runtime.get_minor_version()

            if assembly_rhcos_arch_pullspec:
                pullspec = assembly_rhcos_arch_pullspec
                image_info_str, _ = exectools.cmd_assert(f'oc image info -o json {pullspec}')
                image_info = Model(dict_to_model=json.loads(image_info_str))
                build_id = image_info.config.config.Labels.version
                if not build_id:
                    raise Exception(f'Unable to determine MOSC build_id from: {pullspec}. Retrieved image info: {image_info_str}')
            else:
                build_id, pullspec = rhcos.latest_machine_os_content(version, brew_arch, private)
                if not pullspec:
                    raise Exception(f"No RHCOS found for {version}")

            commitmeta = rhcos.rhcos_build_meta(build_id, version, brew_arch, private, meta_type="commitmeta")
            rpm_list = commitmeta.get("rpmostree.rpmdb.pkglist")
            if not rpm_list:
                raise Exception(f"no pkglist in {commitmeta}")

        except Exception as ex:
            problem = f"{image_stream_suffix}: {ex}"
            red_print(f"error finding RHCOS {problem}")
            # record when there is a problem; as each arch is a separate build, make an array
            self.state.setdefault("images", {}).setdefault("machine-os-content", []).append(problem)
            return None

        # create fake brew image archive to be analyzed later for rpm inconsistencies
        archive = dict(
            build_id=f"({brew_arch}){build_id}",
            rpms=[dict(name=r[0], epoch=r[1], nvr=f"{r[0]}-{r[2]}-{r[3]}") for r in rpm_list],
            # nothing else should be needed - if we need more, will have to fake it here
        )

        return dict(
            archive=archive,
            image_src=pullspec,
            # nothing else should be needed - if we need more, will have to fake it here
        )

    def _get_mosc_istag_spec(self, source, inconsistencies):
        inc = dict(inconsistencies)  # make a copy so original is not altered
        inc.update(self._find_rpm_inconsistencies(source['archive'], rhcos.rhcos_content_tag(self.runtime)))
        if inc:  # format {annotation: description}
            inc_state = self.state.setdefault('inconsistencies', {}).setdefault('machine-os-content', [])
            inc_state.extend([desc for desc in inc.values() if desc not in inc_state])

        return {
            'annotations': self._inconsistency_annotation(inc.keys()),
            'name': "machine-os-content",
            'from': {
                'kind': 'DockerImage',
                # unlike other images, m-os-c originates in quay, does not need mirroring
                'name': source['image_src'],
            }
        }

    def _find_mismatched_siblings(self, builds) -> Set:
        """ Sibling images are those built from the same repository. We need to throw an error if there are sibling built from different commit.
        """
        # First, loop over all builds and store their source repos and commits to a dict
        repo_commit_nvrs = {}  # key is source repo url, value is another dict that key is commit hash and value is a set of nvrs.
        # Second, build a dict with keys are NVRs and values are the ImageMetadata objects. ImageMetadatas are used for logging state.
        nvr_images = {}

        for record in builds:
            # source repo url and commit hash are stored in image's environment variables.
            ar = record.archives[0]  # the build is a manifest list, let's look at the first architecture
            envs = ar["extra"]["docker"]["config"]["config"].get("Env", [])
            source_repo_entry = list(filter(lambda env: env.startswith("SOURCE_GIT_URL="), envs))
            source_commit_entry = list(filter(lambda env: env.startswith("SOURCE_GIT_COMMIT="), envs))
            if not source_repo_entry or not source_commit_entry:
                continue  # this image doesn't have required environment variables. is it a dist-git only image?
            source_repo = source_repo_entry[0][source_repo_entry[0].find("=") + 1:]  # SOURCE_GIT_URL=https://example.com => https://example.com
            source_commit = source_commit_entry[0][source_commit_entry[0].find("=") + 1:]  # SOURCE_GIT_COMMIT=abc => abc
            nvrs = repo_commit_nvrs.setdefault(source_repo, {}).setdefault(source_commit, set())
            nvrs.add(record.build["nvr"])
            nvr_images[record.build["nvr"]] = record.image

        # Finally, look at the dict and print an error if one repo has 2 or more commits
        mismatched_siblings = set()
        for repo, commit_nvrs in repo_commit_nvrs.items():
            if len(commit_nvrs) >= 2:
                red_print("The following NVRs are siblings but built from different commits:")
                for commit, nvrs in commit_nvrs.items():
                    for nvr in nvrs:
                        image = nvr_images[nvr]
                        mismatched_siblings.add(image.distgit_key)
                        red_print(f"{nvr}\t{image.distgit_key}\t{repo}\t{commit}")
        return mismatched_siblings


def default_is_base_name(runtime: Runtime):
    version = runtime.get_minor_version()
    if runtime.assembly == 'stream':
        return f'{version}-art-latest'
    else:
        return f'{version}-art-assembly-{runtime.assembly}'


def default_is_base_namespace():
    return "ocp"


def is_name_and_space(base_name, base_namespace, brew_arch, private):
    arch_suffix = go_suffix_for_arch(brew_arch)
    priv_suffix = "-priv" if private else ""
    name = f"{base_name}{arch_suffix}{priv_suffix}"
    namespace = f"{base_namespace}{arch_suffix}{priv_suffix}"
    return name, namespace
