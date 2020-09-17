import io
import os
import click

from dockerfile_parse import DockerfileParser
from doozerlib.model import Missing
from doozerlib.cli import cli, pass_runtime
from doozerlib import brew, state, exectools, embargo_detector
from doozerlib.util import get_docker_config_json
import yaml


@cli.group("images:streams", short_help="Manage ART equivalent images in upstream CI.")
def images_streams():
    pass


@images_streams.command('mirror')
@click.option('--stream', 'streams', metavar='STREAM_NAME', default=[], multiple=True, help='If specified, only these stream names will be mirrored.')
@click.option('--only-if-missing', default=False, is_flag=True, help='Only mirror the image if there is presently no equivalent image upstream.')
@click.option('--dry-run', default=False, is_flag=True, help='Do not build anything, but only print build operations.')
@pass_runtime
def images_streams_mirror(runtime, streams, only_if_missing, dry_run):
    runtime.initialize(clone_distgits=False, clone_source=False)
    runtime.assert_mutation_is_permitted()

    if streams:
        user_specified = True
    else:
        user_specified = False
        streams = runtime.get_stream_names()

    streams_config = runtime.streams
    for stream in streams:
        if streams_config[stream] is Missing:
            raise IOError(f'Did not find stream {stream} in streams.yml for this group')

        config = streams_config[stream]
        if config.mirror is True or user_specified:
            upstream_dest = config.upstream_image
            if upstream_dest is Missing:
                raise IOError(f'Unable to mirror stream {stream} as upstream_image is not defined')

            # If the configuration specifies a upstream_image_base, then ART is responsible for mirroring
            # that location and NOT the upstream_image. DPTP will take the upstream_image_base and
            # formulate the upstream_image.
            if config.upstream_image_base is not Missing:
                upstream_dest = config.upstream_image_base

            brew_image = config.image
            brew_pullspec = runtime.resolve_brew_image_url(brew_image)

            if only_if_missing:
                check_cmd = f'oc image info {upstream_dest}'
                rc, check_out, check_err = exectools.cmd_gather(check_cmd)
                if 'does not exist' not in check_err:  # should be 'error: image does not exist or you don't have permission to access the repository'
                    print(f'Image {upstream_dest} seems to exist already; skipping because of --only-if-missing')
                    continue

            cmd = f'oc image mirror {brew_pullspec} {upstream_dest}'

            if runtime.registry_config_dir is not None:
                cmd += f" --registry-config={get_docker_config_json(runtime.registry_config_dir)}"
            if dry_run:
                print(f'For {stream}, would have run: {cmd}')
            else:
                exectools.cmd_assert(cmd, retries=3, realtime=True)


@images_streams.command('check-upstream', short_help='Dumps information about CI buildconfigs/mirrored images associated with this group')
@pass_runtime
def images_streams_check_upstream(runtime):
    runtime.initialize(clone_distgits=False, clone_source=False)

    streams = runtime.get_stream_names()
    streams_config = runtime.streams
    istags_status = []
    for stream in streams:
        config = streams_config[stream]

        if not(config.transform or config.mirror):
            continue

        upstream_dest = config.upstream_image
        _, dest_ns, dest_istag = upstream_dest.rsplit('/', maxsplit=2)
        dest_imagestream, dest_tag = dest_istag.split(':')

        rc, stdout, stderr = exectools.cmd_gather(f'oc get -n {dest_ns} istag {dest_istag} --no-headers')
        if rc:
            istags_status.append(f'ERROR: {stream}\nIs not yet represented upstream in {dest_ns} istag/{dest_istag}')
        else:
            istags_status.append(f'OK: {stream} exists, but check whether it is recently updated\n{stdout}')

    bc_stdout, bc_stderr = exectools.cmd_assert(f'oc -n ci get -o=wide buildconfigs -l art-builder-group={runtime.group_config.name}')
    builds_stdout, builds_stderr = exectools.cmd_assert(f'oc -n ci get -o=wide builds -l art-builder-group={runtime.group_config.name}')
    ds_stdout, ds_stderr = exectools.cmd_assert(f'oc -n ci get ds -l art-builder-group={runtime.group_config.name}')
    print('Daemonset status (pins image to prevent gc on node; verify that READY=CURRENT):')
    print(ds_stdout or ds_stderr)
    print('Build configs:')
    print(bc_stdout or bc_stderr)
    print('Recent builds:')
    print(builds_stdout or builds_stderr)

    print('Upstream imagestream tag status')
    for istag_status in istags_status:
        print(istag_status)
        print()


@images_streams.command('start-builds', short_help='Triggers a build for each buildconfig associated with this group')
@pass_runtime
def images_streams_start_buildconfigs(runtime):
    runtime.initialize(clone_distgits=False, clone_source=False)
    bc_stdout, bc_stderr = exectools.cmd_assert(f'oc -n ci get -o=name buildconfigs -l art-builder-group={runtime.group_config.name}')
    bc_stdout = bc_stdout.strip()

    if bc_stdout:
        for name in bc_stdout.splitlines():
            print(f'Triggering: {name}')
            stdout, stderr = exectools.cmd_assert(f'oc -n ci start-build {name}')
            print('   ' + stdout or stderr)
    else:
        print('No buildconfigs associated with this group')


@images_streams.command('gen-buildconfigs', short_help='Generates buildconfigs necessary to assemble ART equivalent images upstream')
@click.option('--stream', 'streams', metavar='STREAM_NAME', default=[], multiple=True, help='If specified, only these stream names will be processed.')
@click.option('-o', '--output', metavar='FILENAME', required=True, help='The filename into which to write the YAML. It should be oc applied against api.ci as art-publish. The file may be empty if there are no buildconfigs.')
@click.option('--apply', default=False, is_flag=True, help='Apply the output if any buildconfigs are generated')
@pass_runtime
def images_streams_gen_buildconfigs(runtime, streams, output, apply):
    """
    ART has a mandate to make "ART equivalent" images available usptream for CI workloads. This enables
    CI to compile with the same golang version ART is using and use identical UBI8 images, etc. To accomplish
    this, streams.yml contains metadata which is extraneous to the product build, but critical to enable
    a high fidelity CI signal.
    It may seem at first that all we would need to do was mirror the internal brew images we use
    somewhere accessible by CI, but it is not that simple:
    1. When a CI build yum installs, it needs to pull RPMs from an RPM mirroring service that runs in
       CI. That mirroring service subsequently pulls and caches files ART syncs using reposync.
    2. There is a variation of simple golang builders that CI uses to compile test cases. These
       images are configured in ci-operator config's 'build_root' and they are used to build
       and run test cases. Sometimes called 'CI release' image, these images contain tools that
       are not part of the typical golang builder (e.g. tito).
    Both of these differences require us to 'transform' the image ART uses in brew into an image compatible
    for use in CI. A challenge to this transformation is that they must be performed in the CI context
    as they depend on the services only available in ci (e.g. base-4-6-rhel8.ocp.svc is used to
    find the current yum repo configuration which should be used).
    To accomplish that, we don't build the images ourselves. We establish OpenShift buildconfigs on the CI
    cluster which process intermediate images we push into the final, CI consumable image.
    These buildconfigs are generated dynamically by this sub-verb.
    The verb will also create a daemonset for the group on the CI cluster. This daemonset overcomes
    a bug in OpenShift 3.11 where the kubelet can garbage collection an image that the build process
    is about to use (because the kubelet & build do not communicate). The daemonset forces the kubelet
    to know the image is in use. These daemonsets can like be eliminated when CI infra moves fully to
    4.x.
    """
    runtime.initialize(clone_distgits=False, clone_source=False)
    runtime.assert_mutation_is_permitted()

    # Record whether this is for all streams or just user specified
    all_streams = not streams

    if not streams:
        # If not specified, use all.
        streams = runtime.get_stream_names()

    transform_rhel_7_base_repos = 'rhel-7/base-repos'
    transform_rhel_8_base_repos = 'rhel-8/base-repos'
    transform_rhel_7_golang = 'rhel-7/golang'
    transform_rhel_8_golang = 'rhel-8/golang'
    transform_rhel_7_ci_build_root = 'rhel-7/ci-build-root'
    transform_rhel_8_ci_build_root = 'rhel-8/ci-build-root'

    # The set of valid transforms
    transforms = set([
        transform_rhel_7_base_repos,
        transform_rhel_8_base_repos,
        transform_rhel_7_golang,
        transform_rhel_8_golang,
        transform_rhel_7_ci_build_root,
        transform_rhel_8_ci_build_root,
    ])

    major = runtime.group_config.vars['MAJOR']
    minor = runtime.group_config.vars['MINOR']

    buildconfig_definitions = []
    ds_container_definitions = []
    streams_config = runtime.streams
    for stream in streams:
        if streams_config[stream] is Missing:
            raise IOError(f'Did not find stream {stream} in streams.yml for this group')

        config = streams_config[stream]

        transform = config.transform
        if transform is Missing:
            # No buildconfig is necessary
            continue

        if transform not in transforms:
            raise IOError(f'Unable to render buildconfig for stream {stream} - transform {transform} not found within {transforms}')

        upstream_dest = config.upstream_image
        upstream_intermediate_image = config.upstream_image_base
        if upstream_dest is Missing or upstream_intermediate_image is Missing:
            raise IOError(f'Unable to render buildconfig for stream {stream} - you must define upstream_image_base AND upstream_image')

        # split a pullspec like registry.svc.ci.openshift.org/ocp/builder:rhel-8-golang-openshift-{MAJOR}.{MINOR}.art
        # into  OpenShift namespace, imagestream, and tag
        _, intermediate_ns, intermediate_istag = upstream_intermediate_image.rsplit('/', maxsplit=2)
        intermediate_imagestream, intermediate_tag = intermediate_istag.split(':')

        _, dest_ns, dest_istag = upstream_dest.rsplit('/', maxsplit=2)
        dest_imagestream, dest_tag = dest_istag.split(':')

        python_file_dir = os.path.dirname(__file__)

        # should align with files like: doozerlib/cli/streams_transforms/rhel-7/base-repos
        transform_template = os.path.join(python_file_dir, 'streams_transforms', transform, 'Dockerfile')
        with open(transform_template, mode='r', encoding='utf-8') as tt:
            transform_template_content = tt.read()

        dfp = DockerfileParser(cache_content=True, fileobj=io.BytesIO())
        dfp.content = transform_template_content

        # Make sure that upstream images can discern they are building in CI with ART equivalent images
        dfp.envs['OPENSHIFT_CI'] = 'true'

        dfp.labels['io.k8s.display-name'] = f'{dest_imagestream}-{dest_tag}'
        dfp.labels['io.k8s.description'] = f'ART equivalent image {runtime.group_config.name}-{stream} - {transform}'

        if transform == transform_rhel_8_base_repos or config.transform == transform_rhel_8_golang:
            # The repos transform create a build config that will layer the base image with CI appropriate yum
            # repository definitions.
            dfp.add_lines(f'RUN rm -rf /etc/yum.repos.d/*.repo && curl http://base-{major}-{minor}-rhel8.ocp.svc > /etc/yum.repos.d/ci-rpm-mirrors.repo')
            # Allow the base repos to be used BEFORE art begins mirroring 4.x to openshift mirrors.
            # This allows us to establish this locations later -- only disrupting CI for those
            # components that actually need reposync'd RPMs from the mirrors.
            dfp.add_lines('RUN yum config-manager --setopt=skip_if_unavailable=True --save')

        if transform == transform_rhel_7_base_repos or config.transform == transform_rhel_7_golang:
            # The repos transform create a build config that will layer the base image with CI appropriate yum
            # repository definitions.
            dfp.add_lines(f'RUN rm -rf /etc/yum.repos.d/*.repo && curl http://base-{major}-{minor}.ocp.svc > /etc/yum.repos.d/ci-rpm-mirrors.repo')
            # Allow the base repos to be used BEFORE art begins mirroring 4.x to openshift mirrors.
            # This allows us to establish this locations later -- only disrupting CI for those
            # components that actually need reposync'd RPMs from the mirrors.
            dfp.add_lines("RUN yum-config-manager --save '--setopt=*.skip_if_unavailable=True'")

        # We've arrived at a Dockerfile.
        dockerfile_content = dfp.content

        # Now to create a buildconfig for it.
        buildconfig = {
            'apiVersion': 'v1',
            'kind': 'BuildConfig',
            'metadata': {
                'name': f'{dest_imagestream}-{dest_tag}--art-builder',
                'namespace': 'ci',
                'labels': {
                    'art-builder-group': runtime.group_config.name,
                    'art-builder-stream': stream,
                },
                'annotations': {
                    'description': 'Generated by the ART pipeline by doozer. Processes raw ART images into ART equivalent images for CI.'
                }
            },
            'spec': {
                'failedBuildsHistoryLimit': 2,
                'output': {
                    'to': {
                        'kind': 'ImageStreamTag',
                        'namespace': dest_ns,
                        'name': dest_istag
                    }
                },
                'source': {
                    'dockerfile': dockerfile_content,
                    'type': 'Dockerfile'
                },
                'strategy': {
                    'dockerStrategy': {
                        'from': {
                            'kind': 'ImageStreamTag',
                            'name': intermediate_istag,
                            'namespace': intermediate_ns,
                        },
                        'imageOptimizationPolicy': 'SkipLayers',
                    },
                },
                'successfulBuildsHistoryLimit': 2,
                'triggers': [{
                    'imageChange': {},
                    'type': 'ImageChange'
                }]
            }
        }

        buildconfig_definitions.append(buildconfig)

        # define a daemonset container that will keep this image running on all nodes so that it will
        # not be garbage collected by the kubelet in 3.11.
        ds_container_definitions.append({
            "image": f"registry.svc.ci.openshift.org/{dest_ns}/{dest_istag}",
            "command": [
                "/bin/bash",
                "-c",
                "#!/bin/bash\nset -euo pipefail\ntrap 'jobs -p | xargs -r kill || true; exit 0' TERM\nwhile true; do\n  sleep 600 &\n  wait $!\ndone\n"
            ],
            "name": f"{dest_ns}-{dest_imagestream}-{dest_tag}".replace('.', '-'),
            "resources": {
                    "requests": {
                        "cpu": "50m"
                    }
            },
            "imagePullPolicy": "Always"
        })

    ds_name = 'art-managed-' + runtime.group_config.name + '-dont-gc-me-bro'
    daemonset_definition = {
        "kind": "DaemonSet",
        "spec": {
            "revisionHistoryLimit": 1,
            "template": {
                "spec": {
                    "containers": ds_container_definitions
                },
                "metadata": {
                    "labels": {
                        "app": ds_name
                    }
                }
            },
            "selector": {
                "matchLabels": {
                    "app": ds_name
                }
            },
            "templateGeneration": 1,
            "updateStrategy": {
                "rollingUpdate": {
                    "maxUnavailable": "50%"
                },
                "type": "RollingUpdate"
            }
        },
        "apiVersion": "extensions/v1beta1",
        "metadata": {
            "labels": {
                "app": ds_name,
                'art-builder-group': runtime.group_config.name,
            },
            "namespace": "ci",
            "name": ds_name
        }
    }

    with open(output, mode='w+', encoding='utf-8') as f:
        objects = list()
        objects.extend(buildconfig_definitions)
        if buildconfig_definitions and all_streams:
            # Don't update the daemonset unless all streams are accounted for
            objects.append(daemonset_definition)
        yaml.dump_all(objects, f, default_flow_style=False)

    if apply:
        if buildconfig_definitions:
            print('Applying buildconfigs...')
            exectools.cmd_assert(f'oc apply -f {output}')
        else:
            print('No buildconfigs were generated; skipping apply.')
