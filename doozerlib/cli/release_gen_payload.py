import click
import sys, io, yaml
from doozerlib.util import red_print, yellow_print
from doozerlib import state
from doozerlib.exceptions import DoozerFatalError
from doozerlib.brew import get_latest_builds, list_archives_by_builds
from doozerlib.cli import cli, pass_runtime


@cli.command("release:gen-payload", short_help="Generate input files for release mirroring")
@click.option("--is-name", metavar='NAME', required=True,
              help="ImageStream .metadata.name value. Something like '4.2-art-latest'")
@click.option("--is-namespace", metavar='NAMESPACE', required=False, default='ocp',
              help="ImageStream .metadata.namespace value.\ndefault=ocp")
@click.option("--organization", metavar='ORGANIZATION', required=False, default='openshift-release-dev',
              help="Quay ORGANIZATION to mirror into.\ndefault=openshift-release-dev")
@click.option("--repository", metavar='REPO', required=False, default='ocp-v4.0-art-dev',
              help="Quay REPOSITORY in ORGANIZATION to mirror into.\ndefault=ocp-v4.0-art-dev")
@pass_runtime
def release_gen_payload(runtime, is_name, is_namespace, organization, repository):
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

    # This will map[arch] -> map[image_name] -> { version: version, release: release, image_src: image_src }
    mirroring = {}

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
    latest_builds = get_latest_builds(tag_component_tuples, brew_session)

    runtime.logger.info("Fetching pullspecs...")
    build_ids = [builds[0]["id"] if builds else 0 for builds in latest_builds]
    archives_list = list_archives_by_builds(build_ids, "image", brew_session)

    runtime.logger.info("Creating mirroring lists...")
    for i, image in enumerate(ose_prefixed_images):
        latest_build = latest_builds[i]
        archives = archives_list[i]
        error = None
        if not (latest_build and archives):  # build or archive doesn't exist
            error = f"Unable to find build for: {image.image_name_short}"
            no_build_items.append(image.image_name_short)
            state.record_image_fail(lstate, image, error, runtime.logger)
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
                runtime.logger.info(f"Adding image {pullspecs[-1]} to the {arch} mirroring list with imagestream tag {tag_name}...")
                mirroring.setdefault(arch, {})[tag_name] = {'version': latest_build[0]["version"], 'release': latest_build[0]["release"], 'image_src': pullspecs[-1]}
        if not error:
            state.record_image_success(lstate, image)
        else:
            red_print(error, file=sys.stderr)
            state.record_image_fail(lstate, image, error, runtime.logger)

    for arch in mirroring:

        mirror_filename = 'src_dest.{}'.format(arch)
        imagestream_filename = 'image_stream.{}'.format(arch)
        target_is_name = is_name
        target_is_namespace = is_namespace
        if arch != 'x86_64':
            target_is_name = '{}-{}'.format(target_is_name, arch)
            target_is_namespace = '{}-{}'.format(target_is_namespace, arch)

        def build_dest_name(tag_name):
            entry = mirroring[arch][tag_name]
            if arch == 'x86_64':
                arch_ext = ''
            else:
                arch_ext = '-{}'.format(arch)
            return 'quay.io/{orgrepo}:{version}-{release}{arch_ext}-ose-{tag_name}-{uuid}'.format(orgrepo=orgrepo,
                                                                                                  version=entry['version'],
                                                                                                  release=entry['release'],
                                                                                                  arch_ext=arch_ext,
                                                                                                  tag_name=tag_name,
                                                                                                  uuid=runtime.uuid)

        # Save the default SRC=DEST 'oc image mirror' input to a file for
        # later.
        with io.open(mirror_filename, 'w+', encoding="utf-8") as out_file:
            for tag_name in mirroring[arch]:
                dest = build_dest_name(tag_name)
                out_file.write("{}={}\n".format(mirroring[arch][tag_name]['image_src'], dest))

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

            for tag_name in mirroring[arch]:
                tag_list.append({
                    'name': tag_name,
                    'from': {
                        'kind': 'DockerImage',
                        'name': build_dest_name(tag_name)
                    }
                })

            # Not all images are built for non-x86 arches (e.g. kuryr), but they
            # may be mentioned in image references. Thus, make sure there is a tag
            # for every tag we find in x86_64 and provide just a dummy image.
            for tag_name in mirroring['x86_64']:
                if tag_name not in mirroring[arch]:

                    if 'cli' not in mirroring[arch]:
                        raise DoozerFatalError('A dummy image is required for tag {} on arch {}, but unable to find cli tag for this arch'.format(tag_name, arch))

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
