import click
import traceback
from typing import Optional

from doozerlib.cli import cli, option_commit_message, option_push, pass_runtime, validate_semver_major_minor_patch
from doozerlib import state, Runtime
from doozerlib.model import Missing
from doozerlib.util import yellow_print
from doozerlib.exceptions import DoozerFatalError


@cli.command("images:rebase", short_help="Refresh a group's distgit content from source content.")
@click.option("--version", metavar='VERSION', default=None, callback=validate_semver_major_minor_patch,
              help="Version string to populate in Dockerfiles. \"auto\" gets version from atomic-openshift RPM")
@click.option("--release", metavar='RELEASE', default=None, help="Release string to populate in Dockerfiles.")
@click.option("--embargoed", is_flag=True, help="Add .p1 to the release string for all images, which indicates those images have embargoed fixes")
@click.option("--repo-type", metavar="REPO_TYPE", envvar="OIT_IMAGES_REPO_TYPE",
              default="unsigned",
              help="Repo group type to use for version autodetection scan (e.g. signed, unsigned).")
@click.option("--force-yum-updates", is_flag=True, default=False,
              help="Inject \"yum update -y\" in the final stage of an image build. This ensures the component image will be able to override RPMs it is inheriting from its parent image using RPMs in the rebuild plashet.")
@option_commit_message
@option_push
@pass_runtime
def images_rebase(runtime: Runtime, version: Optional[str], release: Optional[str], embargoed: bool, repo_type: str, force_yum_updates: bool, message: str, push: bool):
    """
    Many of the Dockerfiles stored in distgit are based off of content managed in GitHub.
    For example, openshift-enterprise-node should always closely reflect the changes
    being made upstream in github.com/openshift/ose/images/node. This operation
    goes out and pulls the current source Dockerfile (and potentially other supporting
    files) into distgit and applies any transformations defined in the config yaml associated
    with the distgit repo.

    This operation will also set the version and release in the file according to the
    command line arguments provided.

    If a distgit repo does not have associated source (i.e. it is managed directly in
    distgit), the Dockerfile in distgit will not be rebased, but other aspects of the
    metadata may be applied (base image, tags, etc) along with the version and release.
    """

    runtime.initialize(validate_content_sets=True)

    if runtime.group_config.public_upstreams and (release is None or release != "+" and not release.endswith(".p?")):
        raise click.BadParameter("You must explicitly specify a `release` ending with `.p?` (or '+') when there is a public upstream mapping in ocp-build-data.")

    # This is ok to run if automation is frozen as long as you are not pushing
    if push:
        runtime.assert_mutation_is_permitted()

    cmd = runtime.command
    runtime.state.pop('images:update-dockerfile', None)
    runtime.state[cmd] = dict(state.TEMPLATE_IMAGE)
    lstate = runtime.state[cmd]  # get local convenience copy

    # If not pushing, do not clean up our work
    runtime.remove_tmp_working_dir = push

    # Get the version from the atomic-openshift package in the RPM repo
    if version == "auto":
        version = runtime.auto_version(repo_type)

    if version and not runtime.valid_version(version):
        raise ValueError(
            "invalid version string: {}, expecting like v3.4 or v1.2.3".format(version)
        )

    runtime.clone_distgits()
    metas = runtime.ordered_image_metas()
    lstate['total'] = len(metas)

    def dgr_rebase(image_meta, terminate_event):
        try:
            dgr = image_meta.distgit_repo()
            if embargoed:
                dgr.private_fix = True
            (real_version, real_release) = dgr.rebase_dir(version, release, terminate_event, force_yum_updates)
            sha = dgr.commit(message, log_diff=True)
            dgr.tag(real_version, real_release)
            runtime.add_record(
                "distgit_commit",
                distgit=image_meta.qualified_name,
                image=image_meta.config.name,
                sha=sha,
            )
            state.record_image_success(lstate, image_meta)

            if push:
                (meta, success) = dgr.push()
                if success is not True:
                    state.record_image_fail(lstate, meta, success)
                dgr.wait_on_cgit_file()

        except Exception as ex:
            # Only the message will recorded in the state. Make sure we print out a stacktrace in the logs.
            traceback.print_exc()

            owners = image_meta.config.owners
            owners = ",".join(list(owners) if owners is not Missing else [])
            runtime.add_record(
                "distgit_commit_failure",
                distgit=image_meta.qualified_name,
                image=image_meta.config.name,
                owners=owners,
                message=str(ex).replace("|", ""),
            )
            msg = str(ex)
            state.record_image_fail(lstate, image_meta, msg, runtime.logger)
            return False
        return True

    jobs = runtime.parallel_exec(
        lambda image_meta, terminate_event: dgr_rebase(image_meta, terminate_event),
        metas,
    )
    jobs.get()

    state.record_image_finish(lstate)

    failed = []
    for img, status in lstate['images'].items():
        if status is not True:  # anything other than true is fail
            failed.append(img)

    if lstate['status'] == state.STATE_FAIL:
        raise DoozerFatalError('One or more required images failed. See state.yaml')
    elif lstate['success'] == 0:
        raise DoozerFatalError('No required images were specified, but all images failed.')

    if failed:
        msg = "The following non-critical images failed to rebase:\n{}".format('\n'.join(failed))
        yellow_print(msg)
