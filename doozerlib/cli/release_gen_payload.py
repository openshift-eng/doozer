import io
import sys
from logging import Logger
from typing import Dict, List, Optional, Set

import click
import koji
import yaml
import json

from doozerlib import brew, state, exectools, embargo_detector
from doozerlib.cli import cli, pass_runtime
from doozerlib.exceptions import DoozerFatalError
from doozerlib.image import ImageMetadata
from doozerlib.util import red_print, yellow_print


@cli.command("release:gen-payload", short_help="Generate input files for release mirroring")
@click.option("--is-name", metavar='NAME', required=True,
              help="ImageStream .metadata.name value. Something like '4.2-art-latest'")
@click.option("--is-namespace", metavar='NAMESPACE', required=False, default='ocp',
              help="ImageStream .metadata.namespace value.\ndefault=ocp")
@click.option("--organization", metavar='ORGANIZATION', required=False, default='openshift-release-dev',
              help="Quay ORGANIZATION to mirror into.\ndefault=openshift-release-dev")
@click.option("--repository", metavar='REPO', required=False, default='ocp-v4.0-art-dev',
              help="Quay REPOSITORY in ORGANIZATION to mirror into.\ndefault=ocp-v4.0-art-dev")
@click.option("--event-id", metavar='NUM', required=False, type=int,
              help="A Brew event ID. If specified, the latest images as of the given Brew event will be chosen for mirroring intead of now.")
@pass_runtime
def release_gen_payload(runtime, is_name, is_namespace, organization, repository, event_id):
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

YOU MUST PROVIDE the base name for the image streams. The generated
files will append the -arch suffix to the given name.

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
    """
    runtime.initialize(clone_distgits=False, config_excludes='non_release')
    orgrepo = "{}/{}".format(organization, repository)
    cmd = runtime.command
    runtime.state[cmd] = dict(state.TEMPLATE_IMAGE)
    lstate = runtime.state[cmd]  # get local convenience copy

    images = [i for i in runtime.image_metas()]
    lstate['total'] = len(images)

    no_build_items = []
    invalid_name_items = []

    ose_prefixed_images = []
    for image in images:
        # Per clayton:
        """Tim Bielawa: note to self: is only for `ose-` prefixed images
        Clayton Coleman: Yes, Get with the naming system or get out of town
        """
        if not image.image_name_short.startswith("ose-"):
            invalid_name_items.append(image.image_name_short)
            red_print("NOT adding to IS (does not meet name/version conventions): {}".format(image.image_name_short))
            continue
        ose_prefixed_images.append(image)

    runtime.logger.info("Fetching latest image builds from Brew...")
    tag_component_tuples = [(image.candidate_brew_tag(), image.get_component_name()) for image in ose_prefixed_images]
    brew_session = runtime.build_retrying_koji_client()
    latest_builds = brew.get_latest_builds(tag_component_tuples, "image", event_id, brew_session)
    latest_builds = [builds[0] if builds else None for builds in latest_builds]  # flatten the data structure

    runtime.logger.info("Fetching image archives...")
    build_ids = [b["id"] if b else 0 for b in latest_builds]
    archives_list = brew.list_archives_by_builds(build_ids, "image", brew_session)

    mismatched_siblings = find_mismatched_siblings(ose_prefixed_images, latest_builds, archives_list, runtime.logger, lstate)

    embargoed_build_ids = set()  # a set of private image build ids
    if runtime.group_config.public_upstreams:
        # looking for embargoed image builds
        detector = embargo_detector.EmbargoDetector(brew_session, runtime.logger)
        for index, archive_list in enumerate(archives_list):
            if build_ids[index]:
                detector.archive_lists[build_ids[index]] = archive_list  # store to EmbargoDetector cache to limit Brew queries
        suspects = [b for b in latest_builds if b]
        embargoed_build_ids = detector.find_embargoed_builds(suspects)

    runtime.logger.info("Creating mirroring lists...")

    # These will map[arch] -> map[image_name] -> { version: version, release: release, image_src: image_src }
    mirroring = {}
    for i, image in enumerate(ose_prefixed_images):
        latest_build = latest_builds[i]
        archives = archives_list[i]
        error = None
        if image.distgit_key in mismatched_siblings:
            error = "Siblings built from different commits"
        elif not (latest_build and archives):  # build or archive doesn't exist
            error = f"Unable to find build for: {image.image_name_short}"
            no_build_items.append(image.image_name_short)
        else:
            for archive in archives:
                arch = archive["arch"]
                pullspecs = archive["extra"]["docker"]["repositories"]
                if not pullspecs or ":" not in pullspecs[-1]:  # in case of no pullspecs or invalid format
                    error = f"Unable to find pullspecs for: {image.image_name_short}"
                    red_print(error, file=sys.stderr)
                    state.record_image_fail(lstate, image, error, runtime.logger)
                    break
                # The tag that will be used in the imagestreams
                tag_name = image.image_name_short
                tag_name = tag_name[4:] if tag_name.startswith("ose-") else tag_name  # it _should_ but... to be safe
                digest = archive["extra"]['docker']['digests']['application/vnd.docker.distribution.manifest.v2+json']
                if not digest.startswith("sha256:"):  # It should start with sha256: for now. Let's raise an error if this changes.
                    raise ValueError(f"Received unrecognized digest {digest} for image {pullspecs[-1]}")
                mirroring_value = {'version': latest_build["version"], 'release': latest_build["release"], 'image_src': pullspecs[-1], 'digest': digest}
                embargoed = latest_build["id"] in embargoed_build_ids  # when public_upstreams are not configured, this is always false
                if not embargoed:  # exclude embargoed images from the ocp[-arch] imagestreams
                    runtime.logger.info(f"Adding {arch} image {pullspecs[-1]} to the public mirroring list with imagestream tag {tag_name}...")
                    mirroring.setdefault(arch, {})[tag_name] = mirroring_value
                else:
                    red_print(f"Found embargoed image {pullspecs[-1]}")
                if runtime.group_config.public_upstreams:
                    # when public_upstreams are configured, both embargoed and non-embargoed images should be included in the ocp[-arch]-priv imagestreams
                    runtime.logger.info(f"Adding {arch} image {pullspecs[-1]} to the private mirroring list with imagestream tag {tag_name}...")
                    mirroring.setdefault(f"{arch}-priv", {})[tag_name] = mirroring_value
        if not error:
            state.record_image_success(lstate, image)
        else:
            red_print(error, file=sys.stderr)
            state.record_image_fail(lstate, image, error, runtime.logger)

    for key in mirroring:
        private = key.endswith("-priv")
        arch = key[:-5] if private else key  # strip `-priv` suffix

        mirror_filename = 'src_dest.{}'.format(key)
        imagestream_filename = 'image_stream.{}'.format(key)
        target_is_name = is_name
        target_is_namespace = is_namespace
        if arch != 'x86_64':
            target_is_name = '{}-{}'.format(target_is_name, arch)
            target_is_namespace = '{}-{}'.format(target_is_namespace, arch)
        if private:
            target_is_name += "-priv"
            target_is_namespace += "-priv"

        def build_dest_name(tag_name):
            entry = mirroring[key][tag_name]
            tag = entry["digest"].replace(":", "-")  # sha256:abcdef -> sha256-abcdef
            return f"quay.io/{orgrepo}:{tag}"

        # Save the default SRC=DEST 'oc image mirror' input to a file for
        # later.
        with io.open(mirror_filename, 'w+', encoding="utf-8") as out_file:
            for tag_name in mirroring[key]:
                dest = build_dest_name(tag_name)
                out_file.write("{}={}\n".format(mirroring[key][tag_name]['image_src'], dest))

        with io.open("{}.yaml".format(imagestream_filename), 'w+', encoding="utf-8") as out_file:
            # Add a tag spec to the image stream. The name of each tag
            # spec does not include the 'ose-' prefix. This keeps them
            # consistent between OKD and OCP

            # Template Base Image Stream object.
            tag_list = []
            isb = {
                'kind': 'ImageStream',
                'apiVersion': 'image.openshift.io/v1',
                'metadata': {
                    'name': target_is_name,
                    'namespace': target_is_namespace,
                },
                'spec': {
                    'tags': tag_list,
                }
            }

            for tag_name in mirroring[key]:
                tag_list.append({
                    'name': tag_name,
                    'from': {
                        'kind': 'DockerImage',
                        'name': build_dest_name(tag_name)
                    }
                })

            # TODO: @jupierce: Let's comment this out for now. I need to explore some options with the rhcos team.
            # if private:
            #     # mirroring rhcos
            #     runtime.logger.info(f"Getting latest RHCOS pullspec for {arch}...")
            #     source_is_namespace = target_is_namespace[:-5]  # strip `-priv`
            #     rhcos_pullspec = get_latest_rhcos_pullspec(source_is_namespace, target_is_name)
            #     if not rhcos_pullspec:
            #         yellow_print(f"No RHCOS found from imagestream {target_is_name} in namespace {source_is_namespace}.")  # should we throw an error?
            #     tag_list.append({
            #         'name': "machine-os-content",
            #         'from': {
            #             'kind': 'DockerImage',
            #             'name': rhcos_pullspec
            #         }
            #     })

            # Not all images are built for non-x86 arches (e.g. kuryr), but they
            # may be mentioned in image references. Thus, make sure there is a tag
            # for every tag we find in x86_64 and provide just a dummy image.
            if 'cli' not in mirroring[key]:  # `cli` serves as the dummy image for the replacement
                if runtime.group_config.public_upstreams and not private:  # If cli is embargoed, it is expected that cli is missing in any non *-priv imagestreams.
                    runtime.logger.warning(f"Unable to find cli tag from {key} imagestream. Is `cli` image embargoed?")
                else:  # if CVE embargoes supporting is disabled or the "cli" image is also missing in *-priv namespaces, an error will be raised.
                    raise DoozerFatalError('A dummy image is required for tag {} on arch {}, but unable to find cli tag for this arch'.format(tag_name, arch))
            else:
                extra_tags = mirroring['x86_64-priv' if private else 'x86_64'].keys() - mirroring[key].keys()
                for tag_name in extra_tags:
                    yellow_print('Unable to find tag {} for arch {} ; substituting cli image'.format(tag_name, arch))
                    tag_list.append({
                        'name': tag_name,
                        'from': {
                            'kind': 'DockerImage',
                            'name': build_dest_name('cli')  # cli is always built and is harmless
                        }
                    })

            yaml.safe_dump(isb, out_file, indent=2, default_flow_style=False)

    if no_build_items:
        yellow_print("No builds found for:")
        for img in sorted(no_build_items):
            click.echo("   {}".format(img))

    if invalid_name_items:
        yellow_print("Images skipped due to invalid naming:")
        for img in sorted(invalid_name_items):
            click.echo("   {}".format(img))

    if mismatched_siblings:
        yellow_print("Images skipped due to siblings mismatch:")
        for img in sorted(invalid_name_items):
            click.echo("   {}".format(img))

# TODO: @jupierce: Let's comment this out for now. I need to explore some options with the rhcos team.
# def get_latest_rhcos_pullspec(namespace, image_stream_name):
#     rhcos_tag = "machine-os-content"
#     out, _ = exectools.cmd_assert(["oc", "get", "imagestream", image_stream_name, "-n", namespace, "-o", "json"])
#     imagestream = json.loads(out)
#     for tag_entry in imagestream["status"]["tags"]:
#         if tag_entry["tag"] != rhcos_tag:
#             continue
#         items = tag_entry.get("items")
#         if items:
#             return items[0]["dockerImageReference"]  # the first item should be latest
#     return None  # rhcos image is not found


def find_mismatched_siblings(images, builds, archives_list, logger, lstate):
    """ Sibling images are those built from the same repository. We need to throw an error if there are sibling built from different commit.
    """
    # First, loop over all builds and store their source repos and commits to a dict
    repo_commit_nvrs = {}  # key is source repo url, value is another dict that key is commit hash and value is a set of nvrs.
    for build, archives in zip(builds, archives_list):
        if not build or not archives:
            continue  # the image has no builds yet. should be captured by latter logic.
        # source repo url and commit hash are stored in image's environment variables.
        ar = archives[0]  # in case the build is a manifest list, let's look at the first architecture
        envs = ar["extra"]["docker"]["config"]["config"].get("Env", [])
        source_repo_entry = list(filter(lambda env: env.startswith("SOURCE_GIT_URL="), envs))
        source_commit_entry = list(filter(lambda env: env.startswith("SOURCE_GIT_COMMIT="), envs))
        if not source_repo_entry or not source_commit_entry:
            continue  # this image doesn't have required environment variables. is it a dist-git only image?
        source_repo = source_repo_entry[0][source_repo_entry[0].find("=") + 1:]  # SOURCE_GIT_URL=https://example.com => https://example.com
        source_commit = source_commit_entry[0][source_commit_entry[0].find("=") + 1:]  # SOURCE_GIT_COMMIT=abc => abc
        nvrs = repo_commit_nvrs.setdefault(source_repo, {}).setdefault(source_commit, set())
        nvrs.add(build["nvr"])

    # Second, build a dict with keys are NVRs and values are the ImageMetadata objects. ImageMetadatas are used for logging state.
    nvr_images = {}
    for image, build in zip(images, builds):
        if build:
            nvr_images[build["nvr"]] = image

    # Finally, look at the dict and print an error if one repo has 2 or more commits
    mismatched_siblings = set()
    for repo, commit_nvrs in repo_commit_nvrs.items():
        if len(commit_nvrs) >= 2:
            red_print("The following NVRs are siblings but built from different commits:", file=sys.stderr)
            for commit, nvrs in commit_nvrs.items():
                for nvr in nvrs:
                    image = nvr_images[nvr]
                    mismatched_siblings.add(image.distgit_key)
                    red_print(f"{nvr}\t{image.distgit_key}\t{repo}\t{commit}")
    return mismatched_siblings
