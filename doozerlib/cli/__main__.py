# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function, unicode_literals
from future import standard_library

standard_library.install_aliases()
from doozerlib import Runtime, Dir, cli as cli_package
from doozerlib import state
from doozerlib.model import Missing
from doozerlib.brew import get_watch_task_info_copy
from doozerlib import metadata
from doozerlib.config import MetaDataConfig as mdc
from doozerlib.cli import cli, pass_runtime, validate_semver_major_minor_patch
from doozerlib.cli.release_gen_payload import release_gen_payload
from doozerlib.cli.detect_embargo import detect_embargo
from doozerlib.cli.images_health import images_health
from doozerlib.cli.images_streams import images_streams, images_streams_mirror, images_streams_gen_buildconfigs
from doozerlib.cli.scan_sources import config_scan_source_changes
from doozerlib.cli.rpms_build import rpms_build

from doozerlib.exceptions import DoozerFatalError
from doozerlib import exectools
from doozerlib.util import green_prefix, red_prefix, green_print, red_print, yellow_print, yellow_prefix, color_print, dict_get, analyze_debug_timing, get_cincinnati_channels, extract_version_fields, get_docker_config_json
from doozerlib import operator_metadata
from doozerlib import brew
import click
import os
import time
import shutil
import yaml
import collections
import sys
import subprocess
import urllib.request, urllib.parse, urllib.error
import tempfile
import traceback
import koji
import io
import json
import functools
import datetime
from typing import Dict, List
import re
import semver
import urllib
import pathlib
from numbers import Number
from multiprocessing.pool import ThreadPool
from multiprocessing import cpu_count
from dockerfile_parse import DockerfileParser
from doozerlib import dotconfig
from doozerlib import gitdata
from doozerlib.olm.bundle import OLMBundle


class RemoteRequired(click.Option):
    """
    Option wrapper class for items that aren't needed for local
    builds. Automatically handles them being required for remote
    but ignored when building local.
    When specified, options are assumed to be required for remote.
    There is no need to include `required=True` in the click.Option init.
    """
    def __init__(self, *args, **kwargs):
        kwargs['help'] = (
            kwargs.get('help', '') + '\nNOTE: This argument is ignored with the global option --local'
        ).strip()
        super(RemoteRequired, self).__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        if not ctx.obj.local and not (self.name in opts):
            self.required = True

        return super(RemoteRequired, self).handle_parse_result(
            ctx, opts, args)


option_commit_message = click.option("--message", "-m", cls=RemoteRequired, metavar='MSG', help="Commit message for dist-git.")
option_push = click.option('--push/--no-push', default=False, is_flag=True,
                           help='Pushes to distgit after local changes (--no-push by default).')

# =============================================================================
#
# CLI Commands
#
# =============================================================================


@cli.command("images:clone", help="Clone a group's image distgit repos locally.")
@click.option('--clone-upstreams', default=False, is_flag=True, help='Also clone upstream sources.')
@pass_runtime
def images_clone(runtime, clone_upstreams):
    runtime.initialize(clone_distgits=True, clone_source=clone_upstreams)
    # Never delete after clone; defeats the purpose of cloning
    runtime.remove_tmp_working_dir = False


@cli.command("rpms:clone", help="Clone a group's rpm distgit repos locally.")
@pass_runtime
def rpms_clone(runtime):
    runtime.initialize(mode='rpms', clone_distgits=True)
    # Never delete after clone; defeats the purpose of cloning
    runtime.remove_tmp_working_dir = False


@cli.command("rpms:clone-sources", help="Clone a group's rpm source repos locally and add to sources yaml.")
@click.option("--output-yml", metavar="YAML_PATH",
              help="Output yml file to write sources dict to. Can be same as --sources option but must be explicitly specified.")
@pass_runtime
def rpms_clone_sources(runtime, output_yml):
    runtime.initialize(mode='rpms')
    # Never delete after clone; defeats the purpose of cloning
    runtime.remove_tmp_working_dir = False
    [r for r in runtime.rpm_metas()]
    if output_yml:
        runtime.export_sources(output_yml)


@cli.command("rpms:build", help="Build rpms in the group or given by --rpms.")
@click.option("--version", metavar='VERSION', default=None, callback=validate_semver_major_minor_patch,
              help="Version string to populate in specfile.", required=True)
@click.option("--release", metavar='RELEASE', default=None,
              help="Release label to populate in specfile.", required=True)
@click.option("--embargoed", default=False, is_flag=True, help="Add .p1 to the release string for all rpms, which indicates those rpms have embargoed fixes")
@click.option('--scratch', default=False, is_flag=True, help='Perform a scratch build.')
@click.option('--dry-run', default=False, is_flag=True, help='Do not build anything, but only print build operations.')
@pass_runtime
def rpms_build(runtime, version, release, embargoed, scratch, dry_run):
    """
    Attempts to build rpms for all of the defined rpms
    in a group. If an rpm has already been built, it will be treated as
    a successful operation.
    """

    if version.startswith('v'):
        version = version[1:]

    runtime.initialize(mode='rpms', clone_distgits=False)

    if runtime.group_config.public_upstreams and (release is None or not release.endswith(".p?")):
        raise click.BadParameter("You must explicitly specify a `release` ending with `.p?` when there is a public upstream mapping in ocp-build-data.")

    runtime.assert_mutation_is_permitted()

    items = runtime.rpm_metas()
    if not items:
        runtime.logger.info("No RPMs found. Check the arguments.")
        exit(0)

    if embargoed:
        for rpm in items:
            rpm.private_fix = True

    results = runtime.parallel_exec(
        lambda rpm, terminate_event: rpm.build_rpm(
            version, release, terminate_event, scratch, local=runtime.local, dry_run=dry_run),
        items)
    results = results.get()
    failed = [name for name, r in results if not r]
    if failed:
        runtime.logger.error("\n".join(["Build/push failures:"] + sorted(failed)))
        exit(1)


@cli.command("images:list", help="List of distgits being selected.")
@pass_runtime
def images_list(runtime):
    runtime.initialize(clone_distgits=False)
    click.echo("------------------------------------------")
    for image in runtime.image_metas():
        click.echo(image.qualified_name)
    click.echo("------------------------------------------")
    click.echo("%s images" % len(runtime.image_metas()))


@cli.command("images:push-distgit", short_help="Push all distgist repos in working-dir.")
@pass_runtime
def images_push_distgit(runtime):
    """
    Run to execute an rhpkg push on all locally cloned distgit
    repositories. This is useful following a series of modifications
    in the local clones.
    """
    runtime.initialize(clone_distgits=True)
    runtime.push_distgits()


@cli.command("db:query", short_help="Query database records")
@click.option("-p", "--operation", required=True, metavar='NAME',
              help="Which operation to query (e.g. 'build')")
@click.option("-a", "--attribute", default=[], metavar='NAME', multiple=True,
              help="Attribute name to output")
@click.option("-m", "--match", default=[], metavar='NAME=VALUE', multiple=True,
              help="name=value matches (AND logic)")
@click.option("-l", "--like", default=[], metavar='NAME=%VALUE%', multiple=True,
              help="name LIKE value matches (AND logic)")
@click.option("-w", "--where", metavar='EXPR', default='',
              help="Complex expression instead of matching. e.g. ( `NAME`=\"VALUE\" and `NAME2` LIKE \"VALUE2%\" ) or NOT `NAME3` < \"VALUE3\"")
@click.option("-s", "--sort-by", '--order-by', default=None, metavar='NAME',
              help="Attribute name to sort by. Note: The sort attribute must be present in at least one of the predicates of the expression.")
@click.option("--limit", default=0, metavar='NUM',
              help="Limit records returned to specified number")
@click.option("-o", "--output", metavar='format', default="human", help='Output format: human, csv, or yaml')
@pass_runtime
def db_select(runtime, operation, attribute, match, like, where, sort_by, limit, output):
    """
    Select records from the datastore.

    \b
    For simpledb UI in a browser:
    https://chrome.google.com/webstore/detail/sdbnavigator/ddhigekdfabonefhiildaiccafacphgg/related?hl=en-US

    \b
    Select syntax:
    https://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/UsingSelect.html

    \b
    When using --sort-by:
    https://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/SortingDataSelect.html
    """

    runtime.initialize(clone_distgits=False, no_group=True)
    if not runtime.datastore:
        print('--datastore must be specified')
        exit(1)

    if not attribute:  # No attribute names identified? return everything.
        names = '*'
    else:
        names = ','.join([f'`{a}`' for a in attribute])

    where_str = ''
    if where:
        where_str = ' WHERE ' + f'{where}'
    elif match or like:
        where_str = ' WHERE '
        if match:
            quoted_match = ['`{}`="{}"'.format(*a_match.split('=')) for a_match in match]
            where_str += ' AND '.join(quoted_match)
        if like:
            quoted_like = ['`{}` LIKE "{}"'.format(*a_like.split('=')) for a_like in like]
            where_str += ' AND '.join(quoted_like)

    sort_by_str = ''
    if sort_by:
        # https://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/SortingDataSelect.html
        sort_by_str = f' ORDER BY `{sort_by}` ASC'

    domain = f'`{runtime.datastore}_{operation}`'

    result = runtime.db.select(f'SELECT {names} FROM {domain}{where_str}{sort_by_str}', limit=int(limit))

    if output.lower() == 'yaml':
        print(yaml.safe_dump(result))

    elif output.lower == 'csv':
        include_name = 'name' in attribute
        for item in result:
            if include_name:
                print('Name', end=',')
            for a in item['Attributes']:
                print(a['Name'], end=',')
            print()

            if include_name:
                print(item['Name'], end=',')
            for a in item['Attributes']:
                print(a['Value'], end=',')
            print()
    else:
        # human
        for item in result:
            print(item['Name'] + ':')

            for a in item['Attributes']:
                print('\t' + a['Name'], end='=')
                print(a['Value'])
            print()


@cli.command("images:update-dockerfile", short_help="Update a group's distgit Dockerfile from metadata.")
@click.option("--version", metavar='VERSION', default=None, callback=validate_semver_major_minor_patch,
              help="Version string to populate in Dockerfiles. \"auto\" gets version from atomic-openshift RPM")
@click.option("--release", metavar='RELEASE', default=None,
              help="Release label to populate in Dockerfiles (or + to bump).")
@click.option("--repo-type", metavar="REPO_TYPE", envvar="OIT_IMAGES_REPO_TYPE",
              default="unsigned",
              help="Repo group type to use for version autodetection scan (e.g. signed, unsigned).")
@option_commit_message
@option_push
@pass_runtime
def images_update_dockerfile(runtime, version, release, repo_type, message, push):
    """
    Updates the Dockerfile in each distgit repository with the latest metadata and
    the version/release information specified. This does not update the Dockerfile
    from any external source. For that, use images:rebase.

    Version:
    - If not specified, the current version is preserved.

    Release:
    - If not specified, the release label is removed.
    - If '+', the current release will be bumped.
    - Else, the literal value will be set in the Dockerfile.
    """

    if runtime.local:
        yellow_print('images:update-dockerfile is not valid when using --local, use images:rebase instead')
        sys.exit(1)

    runtime.initialize(validate_content_sets=True, clone_distgits=True)

    if runtime.group_config.public_upstreams and (release is None or (release != "+" and not release.endswith(".p?"))):
        raise click.BadParameter("You must explicitly specify a `release` ending with `.p?` (or '+') when there is a public upstream mapping in ocp-build-data.")

    # This is ok to run if automation is frozen as long as you are not pushing
    if push:
        runtime.assert_mutation_is_permitted()

    cmd = runtime.command
    runtime.state.pop('images:rebase', None)
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

    metas = runtime.image_metas()
    lstate['total'] = len(metas)

    def dgr_update(image_meta):
        try:
            dgr = image_meta.distgit_repo()
            _, prev_release, prev_private_fix = dgr.extract_version_release_private_fix()
            dgr.private_fix = bool(prev_private_fix)  # preserve the .p? field when updating the release
            (real_version, real_release) = dgr.update_distgit_dir(version, release, prev_release)
            dgr.commit(message)
            dgr.tag(real_version, real_release)
            state.record_image_success(lstate, image_meta)
            if push:
                (meta, success) = dgr.push()
                if success is not True:
                    state.record_image_fail(lstate, meta, success)
        except Exception as ex:
            msg = str(ex)
            state.record_image_fail(lstate, image_meta, msg, runtime.logger)
            return False
        return True

    jobs = runtime.parallel_exec(
        lambda image_meta, terminate_event: dgr_update(image_meta),
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
        msg = "The following non-critical images failed to update:\n{}".format('\n'.join(failed))
        yellow_print(msg)


@cli.command("images:covscan", short_help="Run a coverity scan for the specified images")
@click.option("--result-archive", metavar="ARCHIVE_DIR", required=True,
              help="The covscan process computes diffs between different runs. Location where all runs can store data.")
@click.option("--local-repo-rhel-7", metavar="REPO_DIR", default=[], multiple=True,
              help="Absolute path of yum repo to make available for rhel-7 scanner images")
@click.option("--local-repo-rhel-8", metavar="REPO_DIR", default=[], multiple=True,
              help="Absolute path of yum repo to make available for rhel-8 scanner images")
@click.option("--repo-type", metavar="REPO_TYPE", envvar="OIT_IMAGES_REPO_TYPE",
              default="unsigned",
              help="Repo group type to use for version autodetection scan (e.g. signed, unsigned).")
@click.option('--preserve-scanner-images', default=False, is_flag=True,
              help='Reuse any previous builders')
@pass_runtime
def images_covscan(runtime, result_archive, local_repo_rhel_7, local_repo_rhel_8, repo_type, preserve_scanner_images):
    """
    Runs a coverity scan against the specified images.

    \b
    Workflow:
    1. Doozer is invoked against a set of images.
    2. Each image's builder image is isolated from the distgit Dockerfile.
    3. That parent image is used as a base layer for a new scanner image. The scanner image is
       the image's builder image + coverity packages (see below on how to cache coverity repos locally
       to make the construction of this coverity image . If two images use the same builder,
       they will share this image during the same doozer run.
    4. Doozer isolates the Dockerfile instructions between its builder stage and the next FROM.
    5. These instructions are used to create a new Dockerfile. One that executes the isolated RUN instructions
       under the watchful eye of the coverity tools. In order words, this Dockerfile will run the build
       instructions from the first stage of the distgit Dockerfile, but with coverity monitoring the build
       and creating data files that can be used for analysis later. This data is written to a volume mounted on
       the host filesystem so that it can be used by subsequent containers.
    6. After a previous image runs the build, the host volume has coverity data accumulated from the build process.
    7. Using the scanner image again (since it has the coverity tools installed), the 'cov-analyze' command
       is run against this data. This is an hours long process for a large codebase and consumes all CPU
       on a system for that period. Data from this analysis is also written to the host volume.
    8. The scanner image is run against the analysis data (by mounting the host volume) again, this time
       converting the analysis to json (all_results.js).
    9. The scanner image is run again to transform the json into html (all_results.html).
    10. These files are copied out to the --result-archive directory under a directory named after the
       current distgit commit hash.
    11. Doozer then searches the result archive for any preceding commits that have been computed for this
       distgit's commit history. If one is found, and a 'waived.flag' file exists in the directory, then
       a tool called 'csdiff' is used to compare the difference between the current run and the most
       recently waived commit's 'all_results.js'.
    12. The computed difference is written to the current run's archive as 'diff_results.js' and transformed
       into html (diff_results.html). If no previous commit was waived, all_results.js/html and
       diff_results.js/html will match.
    13. Non-empty diff_results will be written into the doozer record. This record should be parsed by the
        pipeline.
    14. The pipeline (not doozer) should mirror the results out to a file server (e.g. rcm-guest)
        and then slack/email owners about the difference. The pipeline should wait until results are waived.
    15. If the results are waived, the pipeline should write 'waived.flag' to the appropriate result
        archive directory.
    16. Rinse and repeat for the next commit. Note that if doozer finds results computed already for a
        given commit hash on in the results archive, it will not recompute them with coverity.

    \b
    Caching coverity repos for rhel-7 locally:
    Use --local-repos to supply coverity repositories for the scanner image.
    $ reposync  -c covscan.repo -a x86_64 -d -p covscan_repos -n -e covscan_cache -r covscan -r covscan-testing
    $ createrepo_c covscan_repos/covscan
    $ createrepo_c covscan_repos/covscan-testing
    Where covscan.repo is:
        [covscan]
        name=Copr repo for covscan
        baseurl=http://coprbe.devel.redhat.com/repos/kdudka/covscan/epel-7-$basearch/
        skip_if_unavailable=True
        gpgcheck=0
        gpgkey=http://coprbe.devel.redhat.com/repos/kdudka/covscan/pubkey.gpg
        enabled=1
        enabled_metadata=1

        [covscan-testing]
        name=Copr repo for covscan-testing
        baseurl=http://coprbe.devel.redhat.com/repos/kdudka/covscan-testing/epel-7-$basearch/
        skip_if_unavailable=True
        gpgcheck=0
        gpgkey=http://coprbe.devel.redhat.com/repos/kdudka/covscan-testing/pubkey.gpg
        enabled=0
        enabled_metadata=1

    """
    def absolutize(path):
        return str(pathlib.Path(path).absolute())

    result_archive = absolutize(result_archive)
    local_repo_rhel_7 = [absolutize(repo) for repo in local_repo_rhel_7]
    local_repo_rhel_8 = [absolutize(repo) for repo in local_repo_rhel_8]

    runtime.initialize()
    metas = runtime.image_metas()

    if not preserve_scanner_images:
        # Delete where DOOZER_COVSCAN_GROUP={self.runtime.group_config.name}. These are
        # images created earlier for this group. No simultaneous runs for
        # the same group are allowed.
        rc, out, err = exectools.cmd_gather(f'docker image ls -a -q --filter label=DOOZER_COVSCAN_GROUP={runtime.group_config.name}')

        if rc == 0 and out:
            targets = " ".join(out.strip().split())
            rc, out, err = exectools.cmd_gather(f'docker rmi {targets}')
            if rc != 0:
                runtime.logger.warning(f'Unable to delete images: {targets}:\n{err}')

    for meta in metas:
        meta.covscan(result_archive=result_archive, repo_type=repo_type, local_repo_rhel_7=local_repo_rhel_7, local_repo_rhel_8=local_repo_rhel_8)


@cli.command("images:rebase", short_help="Refresh a group's distgit content from source content.")
@click.option("--version", metavar='VERSION', default=None, callback=validate_semver_major_minor_patch,
              help="Version string to populate in Dockerfiles. \"auto\" gets version from atomic-openshift RPM")
@click.option("--release", metavar='RELEASE', default=None, help="Release string to populate in Dockerfiles.")
@click.option("--embargoed", is_flag=True, help="Add .p1 to the release string for all images, which indicates those images have embargoed fixes")
@click.option("--repo-type", metavar="REPO_TYPE", envvar="OIT_IMAGES_REPO_TYPE",
              default="unsigned",
              help="Repo group type to use for version autodetection scan (e.g. signed, unsigned).")
@option_commit_message
@option_push
@pass_runtime
def images_rebase(runtime, version, release, embargoed, repo_type, message, push):
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
            (real_version, real_release) = dgr.rebase_dir(version, release, terminate_event)
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


@cli.command("images:foreach", short_help="Run a command relative to each distgit dir.")
@click.argument("cmd", nargs=-1)
@click.option("--message", "-m", metavar='MSG', help="Commit message for dist-git.", required=False)
@option_push
@pass_runtime
def images_foreach(runtime, cmd, message, push):
    """
    Clones all distgit repos found in the specified group and runs an arbitrary
    command once for each local distgit directory. If the command runs without
    error for all directories, a commit will be made. If not a dry_run,
    the repo will be pushed.

    \b
    The following environment variables will be available in each invocation:
    doozer_repo_name : The name of the distgit repository
    doozer_repo_namespace : The distgit repository namespaces (e.g. containers, rpms))
    doozer_config_filename : The config yaml (basename, no path) associated with an image
    doozer_distgit_key : The name of the distgit_key used with -i, -x for this image
    doozer_image_name : The name of the image from Dockerfile
    doozer_image_version : The current version found in the Dockerfile
    doozer_group: The group for this invocation
    doozer_data_path: The directory containing the doozer metadata
    doozer_working_dir: The current working directory
    """
    runtime.initialize(clone_distgits=True)

    # This is ok to run if automation is frozen as long as you are not pushing
    if push:
        runtime.assert_mutation_is_permitted()

    # If not pushing, do not clean up our work
    runtime.remove_tmp_working_dir = push

    cmd_str = " ".join(cmd)

    for image in runtime.image_metas():
        dgr = image.distgit_repo()
        with Dir(dgr.distgit_dir):
            runtime.logger.info("Executing in %s: [%s]" % (dgr.distgit_dir, cmd_str))

            dfp = DockerfileParser()
            dfp.content = image.fetch_cgit_file("Dockerfile")

            if subprocess.call(cmd_str,
                               shell=True,
                               env={"doozer_repo_name": image.name,
                                    "doozer_repo_namespace": image.namespace,
                                    "doozer_image_name": dfp.labels["name"],
                                    "doozer_image_version": dfp.labels["version"],
                                    "doozer_group": runtime.group,
                                    "doozer_data_path": runtime.data_dir,
                                    "doozer_working_dir": runtime.working_dir,
                                    "doozer_config_filename": image.config_filename,
                                    "doozer_distgit_key": image.distgit_key,
                                    }) != 0:
                raise IOError("Command return non-zero status")
            runtime.logger.info("\n")

        if message is not None:
            dgr.commit(message)

    if push:
        runtime.push_distgits()


@cli.command("images:revert", help="Revert a fixed number of commits in each distgit.")
@click.argument("count", nargs=1)
@click.option("--message", "-m", metavar='MSG', help="Commit message for dist-git.", default=None, required=False)
@option_push
@pass_runtime
def images_revert(runtime, count, message, push):
    """
    Revert a particular number of commits in each distgit repository. If
    a message is specified, a new commit will be made.
    """
    runtime.initialize()

    # This is ok to run if automation is frozen as long as you are not pushing
    if push:
        runtime.assert_mutation_is_permitted()

    # If not pushing, do not clean up our work
    runtime.remove_tmp_working_dir = push

    count = int(count) - 1
    if count < 0:
        runtime.logger.info("Revert count must be >= 1")

    if count == 0:
        commit_range = "HEAD"
    else:
        commit_range = "HEAD~%s..HEAD" % count

    cmd = ["git", "revert", "--no-commit", commit_range]

    cmd_str = " ".join(cmd)
    runtime.clone_distgits()
    dgrs = [image.distgit_repo() for image in runtime.image_metas()]
    for dgr in dgrs:
        with Dir(dgr.distgit_dir):
            runtime.logger.info("Running revert in %s: [%s]" % (dgr.distgit_dir, cmd_str))
            if subprocess.call(cmd_str, shell=True) != 0:
                raise IOError("Command return non-zero status")
            runtime.logger.info("\n")

        if message is not None:
            dgr.commit(message)

    if push:
        runtime.push_distgits()


@cli.command("images:merge-branch", help="Copy content of source branch to target.")
@click.option("--target", metavar="TARGET_BRANCH", help="Branch to populate from source branch.")
@click.option('--allow-overwrite', default=False, is_flag=True,
              help='Merge in source branch even if Dockerfile already exists in distgit')
@option_push
@pass_runtime
def images_merge(runtime, target, push, allow_overwrite):
    """
    For each distgit repo, copies the content of the group's branch to a new
    branch.
    """
    runtime.initialize()

    # This is ok to run if automation is frozen as long as you are not pushing
    if push:
        runtime.assert_mutation_is_permitted()

    # If not pushing, do not clean up our work
    runtime.remove_tmp_working_dir = push

    runtime.clone_distgits()
    dgrs = [image.distgit_repo() for image in runtime.image_metas()]
    for dgr in dgrs:
        with Dir(dgr.distgit_dir):
            dgr.logger.info("Merging from branch {} to {}".format(dgr.branch, target))
            dgr.merge_branch(target, allow_overwrite)
            runtime.logger.info("\n")

    if push:
        runtime.push_distgits()


def _taskinfo_has_timestamp(task_info, key_name):
    """
    Tests to see if a named timestamp exists in a koji taskinfo
    dict.
    :param task_info: The taskinfo dict to check
    :param key_name: The name of the timestamp key
    :return: Returns True if the timestamp is found and is a Number
    """
    return isinstance(task_info.get(key_name, None), Number)


def print_build_metrics(runtime):
    watch_task_info = get_watch_task_info_copy()
    runtime.logger.info("\n\n\nImage build metrics:")
    runtime.logger.info("Number of brew tasks attempted: {}".format(len(watch_task_info)))

    # Make sure all the tasks have the expected timestamps:
    # https://github.com/openshift/enterprise-images/pull/178#discussion_r173812940
    for task_id in list(watch_task_info.keys()):
        info = watch_task_info[task_id]
        runtime.logger.debug("Watch task info:\n {}\n\n".format(info))
        # Error unless all true
        if not ('id' in info
                and koji.TASK_STATES[info['state']] == 'CLOSED'
                and _taskinfo_has_timestamp(info, 'create_ts')
                and _taskinfo_has_timestamp(info, 'start_ts')
                and _taskinfo_has_timestamp(info, 'completion_ts')
                ):
            runtime.logger.error(
                "Discarding incomplete/error task info: {}".format(info))
            del watch_task_info[task_id]

    runtime.logger.info("Number of brew tasks successful: {}".format(len(watch_task_info)))

    # An estimate of how long the build time was extended due to FREE state (i.e. waiting for capacity)
    elapsed_wait_minutes = 0

    # If two builds each take one minute of actual active CPU time to complete, this value will be 2.
    aggregate_build_secs = 0

    # If two jobs wait 1m for in FREE state, this value will be '2' even if
    # the respective wait periods overlap. This is different from elapsed_wait_minutes
    # which is harder to calculate.
    aggregate_wait_secs = 0

    # Will be populated with earliest creation timestamp found in all the koji tasks; initialize with
    # infinity to support min() logic.
    min_create_ts = float('inf')

    # Will be populated with the latest completion timestamp found in all the koji tasks
    max_completion_ts = 0

    # Loop through all koji task infos and calculate min
    for task_id, info in watch_task_info.items():
        create_ts = info['create_ts']
        completion_ts = info['completion_ts']
        start_ts = info['start_ts']
        min_create_ts = min(create_ts, min_create_ts)
        max_completion_ts = max(completion_ts, max_completion_ts)
        build_secs = completion_ts - start_ts
        aggregate_build_secs += build_secs
        wait_secs = start_ts - create_ts
        aggregate_wait_secs += wait_secs

        runtime.logger.info('Task {} took {:.1f}m of active build and was waiting to start for {:.1f}m'.format(
            task_id,
            build_secs / 60.0,
            wait_secs / 60.0))
    runtime.logger.info('Aggregate time all builds spent building {:.1f}m'.format(aggregate_build_secs / 60.0))
    runtime.logger.info('Aggregate time all builds spent waiting {:.1f}m'.format(aggregate_wait_secs / 60.0))

    # If we successfully found timestamps in completed builds
    if watch_task_info:

        # For each minute which elapsed between the first build created (min_create_ts) to the
        # last build to complete (max_completion_ts), check whether there was any build that
        # was created but still waiting to start (i.e. in FREE state). If there is a build
        # waiting, include that minute in the elapsed wait time.

        for ts in range(int(min_create_ts), int(max_completion_ts), 60):
            # See if any of the tasks were created but not started during this minute
            for info in watch_task_info.values():
                create_ts = int(info['create_ts'])
                start_ts = int(info['start_ts'])
                # Was the build waiting to start during this minute?
                if create_ts <= ts <= start_ts:
                    # Increment and exit; we don't want to count overlapping wait periods
                    # since it would not accurately reflect the overall time savings we could
                    # expect with more capacity.
                    elapsed_wait_minutes += 1
                    break

        runtime.logger.info("Approximate elapsed time (wasted) waiting: {}m".format(elapsed_wait_minutes))
        elapsed_total_minutes = (max_completion_ts - min_create_ts) / 60.0
        runtime.logger.info("Elapsed time (from first submit to last completion) for all builds: {:.1f}m".format(elapsed_total_minutes))

        runtime.add_record("image_build_metrics", elapsed_wait_minutes=int(elapsed_wait_minutes),
                           elapsed_total_minutes=int(elapsed_total_minutes), task_count=len(watch_task_info))
    else:
        runtime.logger.info('Unable to determine timestamps from collected info: {}'.format(watch_task_info))


@cli.command("images:build", short_help="Build images for the group.")
@click.option("--repo-type", metavar="REPO_TYPE", envvar="OIT_IMAGES_REPO_TYPE",
              default='',
              help="Repo type (e.g. signed, unsigned).")
@click.option("--repo", default=[], metavar="REPO_URL",
              multiple=True, help="Custom repo URL to supply to brew build.")
@click.option('--push-to-defaults', default=False, is_flag=True,
              help='Push to default registries when build completes.')
@click.option("--push-to", default=[], metavar="REGISTRY", multiple=True,
              help="Specific registries to push to when image build completes.  [multiple]")
@click.option('--scratch', default=False, is_flag=True, help='Perform a scratch build.')
@click.option("--threads", default=1, metavar="NUM_THREADS",
              help="Number of concurrent builds to execute. Only valid for --local builds.")
@click.option("--filter-by-os", default=None, metavar="ARCH",
              help="Specify an exact arch to push (e.g. 'amd64').")
@click.option('--dry-run', default=False, is_flag=True, help='Do not build anything, but only print build operations.')
@pass_runtime
def images_build_image(runtime, repo_type, repo, push_to_defaults, push_to, scratch, threads, filter_by_os, dry_run):
    """
    Attempts to build container images for all of the distgit repositories
    in a group. If an image has already been built, it will be treated as
    a successful operation.

    If docker registries as specified, this action will push resultant
    images to those mirrors as they become available. Note that this should
    be more performant than running images:push since pushes can
    be performed in parallel with other images building.

    Tips on using custom --repo.
    1. Upload a .repo file (it must end in .repo) with your desired yum repos enabled
       into an internal location OSBS can reach like gerrit.
    2. Specify the raw URL to this file for the build.
    3. You will probably want to use --scratch since it is unlikely you want your
        custom build tagged.
    """
    # Initialize all distgit directories before trying to build. This is to
    # ensure all build locks are acquired before the builds start and for
    # clarity in the logs.
    runtime.initialize(clone_distgits=True)
    runtime.assert_mutation_is_permitted()

    cmd = runtime.command

    runtime.state[cmd] = dict(state.TEMPLATE_IMAGE)
    lstate = runtime.state[cmd]  # get local convenience copy

    pre_steps = ['images:update-dockerfile', 'images:rebase']
    pre_step = None
    for ps in pre_steps:
        if ps in runtime.state:
            pre_step = runtime.state[ps]

    required = []
    failed = []

    if pre_step:
        for img, status in pre_step['images'].items():
            if status is not True:  # anything other than true is fail
                img_obj = runtime.image_map[img]
                failed.append(img)
                if img_obj.required:
                    required.append(img)

    items = [m.distgit_repo() for m in runtime.ordered_image_metas()]

    lstate['total'] = len(items)

    if not items:
        runtime.logger.info("No images found. Check the arguments.")
        exit(1)

    if required:
        msg = 'The following images failed during the previous step and are required:\n{}'.format('\n'.join(required))
        lstate['status'] = state.STATE_FAIL
        lstate['msg'] = msg
        raise DoozerFatalError(msg)
    elif failed:
        # filter out failed images and their children
        failed = runtime.filter_failed_image_trees(failed)
        yellow_print('The following images failed the last step (or are children of failed images) and will be skipped:\n{}'.format('\n'.join(failed)))
        # reload after fail filtered
        items = [m.distgit_repo() for m in runtime.ordered_image_metas()]

        if not items:
            runtime.logger.info("No images left to build after failures and children filtered out.")
            exit(1)

    if not runtime.local:
        threads = None

    # load active build profile
    profiles = runtime.group_config.build_profiles
    active_profile = {}
    profile_name = runtime.profile or runtime.group_config.default_image_build_profile
    if profile_name:
        active_profile = profiles.primitive()["image"][profile_name]
    # provide build profile defaults
    active_profile.setdefault("targets", [])
    active_profile.setdefault("repo_type", "unsigned")
    active_profile.setdefault("repo_list", [])
    active_profile.setdefault("signing_intent", "unsigned")

    if repo_type:  # backward compatible with --repo-type option
        active_profile["repo_type"] = repo_type
        active_profile["signing_intent"] = "release" if repo_type == "signed" else repo_type
    if repo:
        active_profile["repo_list"] = list(repo)
    results = runtime.parallel_exec(
        lambda dgr, terminate_event: dgr.build_container(
            active_profile, push_to_defaults, additional_registries=push_to,
            terminate_event=terminate_event, scratch=scratch, realtime=(threads == 1), dry_run=dry_run, registry_config_dir=runtime.registry_config_dir, filter_by_os=filter_by_os),
        items, n_threads=threads)
    results = results.get()

    if not runtime.local:  # not needed for local builds
        try:
            print_build_metrics(runtime)
        except:
            # Never kill a build because of bad logic in metrics
            traceback.print_exc()
            runtime.logger.error("Error trying to show build metrics")

    failed = [name for name, r in results if not r]
    if failed:
        runtime.logger.error("\n".join(["Build/push failures:"] + sorted(failed)))
        if len(runtime.missing_pkgs):
            runtime.logger.error("Missing packages: \n{}".format("\n".join(runtime.missing_pkgs)))
        exit(1)

    # Push all late images
    for image in runtime.image_metas():
        image.distgit_repo().push_image([], push_to_defaults, additional_registries=push_to, push_late=True, registry_config_dir=runtime.registry_config_dir, filter_by_os=filter_by_os)

    state.record_image_finish(lstate)


@cli.command("images:push", short_help="Push the most recently built images to mirrors.")
@click.option('--tag', default=[], metavar="PUSH_TAG", multiple=True,
              help='Push to registry using these tags instead of default set.')
@click.option("--version-release", default=None, metavar="VERSION-RELEASE",
              help="Specify an exact version to pull/push (e.g. 'v3.9.31-1' ; default is latest built).")
@click.option('--to-defaults', default=False, is_flag=True, help='Push to default registries.')
@click.option('--late-only', default=False, is_flag=True, help='Push only "late" images.')
@click.option("--to", default=[], metavar="REGISTRY", multiple=True,
              help="Registry to push to when image build completes.  [multiple]")
@click.option("--filter-by-os", default=None, metavar="ARCH",
              help="Specify an exact arch to push (e.g. 'amd64').")
@click.option('--dry-run', default=False, is_flag=True, help='Only print tag/push operations which would have occurred.')
@pass_runtime
def images_push(runtime, tag, version_release, to_defaults, late_only, to, filter_by_os, dry_run):
    """
    Each distgit repository will be cloned and the version and release information
    will be extracted. That information will be used to determine the most recently
    built image associated with the distgit repository.

    An attempt will be made to pull that image and push it to one or more
    docker registries specified on the command line.
    """

    additional_registries = list(to)  # In case we get a tuple

    if to_defaults is False and len(additional_registries) == 0:
        click.echo("You need specify at least one destination registry.")
        exit(1)

    runtime.initialize()

    # This might introduce unwanted mutation, so prevent if automation is frozen
    runtime.assert_mutation_is_permitted()

    cmd = runtime.command
    runtime.state[cmd] = dict(state.TEMPLATE_IMAGE)
    lstate = runtime.state[cmd]  # get local convenience copy

    pre_step = runtime.state.get('images:build', None)

    required = []
    pre_failed = []

    if pre_step:
        for img, status in pre_step['images'].items():
            if status is not True:  # anything other than true is fail
                img_obj = runtime.image_map[img]
                pre_failed.append(img)
                if img_obj.required:
                    required.append(img)

    version_release_tuple = None

    if version_release:
        version_release_tuple = version_release.split('-')
        click.echo('Setting up to push: version={} release={}'.format(version_release_tuple[0], version_release_tuple[1]))

    items = runtime.image_metas()

    if required:
        msg = 'The following images failed during the previous step and are required:\n{}'.format('\n'.join(required))
        lstate['status'] = state.STATE_FAIL
        lstate['msg'] = msg
        raise DoozerFatalError(msg)
    elif pre_failed:
        # filter out failed images and their children
        failed = runtime.filter_failed_image_trees(pre_failed)
        yellow_print('The following images failed the last step (or are children of failed images) and will be skipped:\n{}'.format('\n'.join(failed)))
        # reload after fail filtered
        items = runtime.ordered_image_metas()

        if not items:
            runtime.logger.info("No images left to build after failures and children filtered out.")
            exit(1)

    # late-only is useful if we are resuming a partial build in which not all images
    # can be built/pushed. Calling images:push can end up hitting the same
    # push error, so, without late-only, there is no way to push "late" images and
    # deliver the partial build's last images.
    if not late_only:
        # Allow all non-late push operations to be attempted and track failures
        # with this list. Since "late" images are used as a marker for success,
        # don't push them if there are any preceding errors.
        # This error tolerance is useful primarily in synching images that our
        # team does not build but which should be kept up to date in the
        # operations registry.
        failed = []
        # Push early images

        results = runtime.parallel_exec(
            lambda img, terminate_event:
                img.distgit_repo().push_image(tag, to_defaults, additional_registries,
                                              version_release_tuple=version_release_tuple, dry_run=dry_run, registry_config_dir=runtime.registry_config_dir, filter_by_os=filter_by_os),
            items,
            n_threads=4
        )
        results = results.get()

        failed = [name for name, r in results if not r]
        if failed:
            runtime.logger.error("\n".join(["Push failures:"] + sorted(failed)))
            exit(1)

    # Push all late images
    for image in items:
        # Check if actually a late image to prevent cloning all distgit on --late-only
        if image.config.push.late is True:
            image.distgit_repo().push_image(tag, to_defaults, additional_registries,
                                            version_release_tuple=version_release_tuple,
                                            push_late=True, dry_run=dry_run, registry_config_dir=runtime.registry_config_dir, filter_by_os=filter_by_os)

    state.record_image_finish(lstate)


@cli.command("images:pull", short_help="Pull latest images from pulp")
@pass_runtime
def images_pull_image(runtime):
    """
    Pulls latest images from pull, fetching the dockerfiles from cgit to
    determine the version/release.
    """
    runtime.initialize(clone_distgits=True)
    for image in runtime.image_metas():
        image.pull_image()


@cli.command("images:show-tree", short_help="Display the image relationship tree")
@click.option(
    "--imagename", default=False, is_flag=True,
    help="Use the image name instead of the dist-git name")
@click.option(
    "--yml", default=False, is_flag=True,
    help="Ouput to yaml formatted text, otherwise generate a tree view")
@pass_runtime
def images_show_tree(runtime, imagename, yml):
    """
    Displays the parent/child relationship of all images or just those given.
    This can be helpful to determine build order and dependencies.
    """
    runtime.initialize(clone_distgits=False)

    images = list(runtime.image_metas())

    print_colors = ['green', 'cyan', 'blue', 'yellow', 'magenta']

    def name(image):
        return image.image_name if imagename else image.distgit_key

    if yml:
        color_print(yaml.safe_dump(runtime.image_tree, indent=2, default_flow_style=False), 'cyan')
    else:
        def print_branch(image, indent=0):
            num_child = len(image.children)
            for i in range(num_child):
                child = image.children[i]
                if i == (num_child - 1):
                    tree_char = '└─'
                else:
                    tree_char = '├─'
                tree_char += ('┐' if len(child.children) else ' ')

                line = '{} {} {}'.format(('  ' * indent), tree_char, name(child))
                color_print(line, print_colors[(indent + 1) % len(print_colors)])
                print_branch(child, indent + 1)

        for image in images:
            if not image.parent:
                line = name(image)
                color_print(line, print_colors[0])
                print_branch(image)


@cli.command("images:print", short_help="Print data for each image metadata")
@click.option(
    "--short", default=False, is_flag=True,
    help="Suppress all output other than the data itself")
@click.option('--show-non-release', default=False, is_flag=True,
              help='Include images which have been marked as non-release.')
@click.option('--show-base', default=False, is_flag=True,
              help='Include images which have been marked as base images.')
@click.option("--output", "-o", default=None,
              help="Write data to FILE instead of STDOUT")
@click.option("--label", "-l", default=None,
              help="The label you want to print if it exists. Empty string if n/a")
@click.argument("pattern", default="{build}", nargs=1)
@pass_runtime
def images_print(runtime, short, show_non_release, show_base, output, label, pattern):
    """
    Prints data from each distgit. The pattern specified should be a string
    with replacement fields:

    \b
    {type} - The type of the distgit (e.g. rpms)
    {name} - The name of the distgit repository (e.g. openshift-enterprise)
    {image_name} - The container registry image name (e.g. openshift3/ose-ansible)
    {image_name_short} - The container image name without the registry (e.g. ose-ansible)
    {component} - The component identified in the Dockerfile
    {image} - The image name according to distgit Dockerfile (image_name / image_name_short are faster)
    {version} - The version of the latest brew build
    {release} - The release of the latest brew build
    {build} - Shorthand for {component}-{version}-{release} (e.g. container-engine-v3.6.173.0.25-1)
    {repository} - Shorthand for {image}:{version}-{release}
    {label} - The label you want to print from the Dockerfile (Empty string if n/a)
    {bz_info} - All known BZ component information for the component
    {bz_product} - The BZ product, if known
    {bz_component} - The BZ component, if known
    {bz_subcomponent} - The BZ subcomponent, if known
    {lf} - Line feed

    If pattern contains no braces, it will be wrapped with them automatically. For example:
    "build" will be treated as "{build}"
    """

    runtime.initialize(clone_distgits=False)

    # If user omitted braces, add them.
    if "{" not in pattern:
        pattern = "{%s}" % pattern.strip()

    count = 0
    if short:
        echo_verbose = lambda _: None
    else:
        echo_verbose = click.echo

    if output is None:
        echo_verbose("")
        echo_verbose("------------------------------------------")
    else:
        green_print("Writing image list to {}".format(output))

    non_release_images = []
    images = []
    if show_non_release:
        images = list(runtime.image_metas())
    else:
        for i in runtime.image_metas():
            if i.for_release:
                images.append(i)
            else:
                non_release_images.append(i.distgit_key)

    for image in images:
        # skip base images unless requested
        if image.base_only and not show_base:
            continue
        # skip disabled images unless requested
        if not (image.enabled or runtime.load_disabled):
            continue

        dfp = None

        # Method to lazily load the remote dockerfile content.
        # Avoiding it when the content is not necessary speeds up most print operations.
        def get_dfp():
            nonlocal dfp
            if dfp:
                return dfp
            dfp = DockerfileParser(path=runtime.working_dir)
            try:
                dfp.content = image.fetch_cgit_file("Dockerfile")
                return dfp
            except Exception:
                raise DoozerFatalError("Error reading Dockerfile from distgit: {}".format(image.distgit_key))

        s = pattern
        s = s.replace("{build}", "{component}-{version}-{release}")
        s = s.replace("{repository}", "{image}:{version}-{release}")
        s = s.replace("{namespace}", image.namespace)
        s = s.replace("{name}", image.name)
        s = s.replace("{image_name}", image.image_name)
        s = s.replace("{image_name_short}", image.image_name_short)
        s = s.replace("{component}", image.get_component_name())

        if '{bz_' in s:
            mi = image.get_maintainer_info()
            desc = ''
            for k, v in mi.items():
                desc += f'[{k}={v}] '
            s = s.replace('{bz_info}', desc)
            s = s.replace('{bz_product}', mi.get('product', 'Unknown'))
            s = s.replace('{bz_component}', mi.get('component', 'Unknown'))
            s = s.replace('{bz_subcomponent}', mi.get('subcomponent', 'N/A'))

        if '{image}' in s:
            s = s.replace("{image}", get_dfp().labels["name"])

        if label is not None:
            s = s.replace("{label}", get_dfp().labels.get(label, ''))
        s = s.replace("{lf}", "\n")

        release_query_needed = '{release}' in s or '{pushes}' in s
        version_query_needed = '{version}' in s or '{pushes}' in s

        # Since querying nvr takes time, check before executing replace
        version = ''
        release = ''
        if release_query_needed or version_query_needed:
            try:
                _, version, release = image.get_latest_build_info()
            except IOError as err:
                err_msg = str(err)
                if err_msg.find("No builds detected") >= 0:
                    # ignore "No builds detected" error
                    runtime.logger.warning("No builds delected for {}: {}".format(image.name, err_msg))
                else:
                    raise err

        s = s.replace("{version}", version)
        s = s.replace("{release}", release)

        pushes_formatted = ''
        for push_name in image.get_default_push_names():
            pushes_formatted += '\t{} : [{}]\n'.format(push_name, ', '.join(image.get_default_push_tags(version, release)))

        if not pushes_formatted:
            pushes_formatted = "(None)"

        s = s.replace("{pushes}", '{}\n'.format(pushes_formatted))

        if "{" in s:
            raise IOError("Unrecognized fields remaining in pattern: %s" % s)

        if output is None:
            # Print to stdout
            click.echo(s)
        else:
            # Write to a file
            with io.open(output, 'a', encoding="utf-8") as out_file:
                out_file.write("{}\n".format(s))

        count += 1

    echo_verbose("------------------------------------------")
    echo_verbose("{} images".format(count))

    # If non-release images are being suppressed, let the user know
    if not show_non_release and non_release_images:
        echo_verbose("\nThe following {} non-release images were excluded; use --show-non-release to include them:".format(
            len(non_release_images)))
        for image in non_release_images:
            echo_verbose("    {}".format(image))


@cli.command("images:print-config-template", short_help="Create template package yaml from distgit Dockerfile.")
@click.argument("url", nargs=1)
def distgit_config_template(url):
    """
    Pulls the specified URL (to a Dockerfile in distgit) and prints the boilerplate
    for a config yaml for the image.
    """

    f = urllib.request.urlopen(url)
    if f.code != 200:
        click.echo("Error fetching {}: {}".format(url, f.code), err=True)
        exit(1)

    dfp = DockerfileParser()
    dfp.content = f.read()

    if "cgit/rpms/" in url:
        type = "rpms"
    elif "cgit/containers/" in url:
        type = "containers"
    elif "cgit/apbs/" in url:
        type = "apbs"
    else:
        raise IOError("doozer does not yet support that distgit repo type")

    config = {
        "repo": {
            "type": type,
        },
        "name": dfp.labels['name'],
        "from": {
            "image": dfp.baseimage
        },
        "labels": {},
        "owners": []
    }

    branch = url[url.index("?h=") + 3:]

    if "Architecture" in dfp.labels:
        dfp.labels["architecture"] = dfp.labels["Architecture"]

    component = dfp.labels.get("com.redhat.component", dfp.labels.get("BZComponent", None))

    if component is not None:
        config["repo"]["component"] = component

    managed_labels = [
        'vendor',
        'License',
        'architecture',
        'io.k8s.display-name',
        'io.k8s.description',
        'io.openshift.tags'
    ]

    for ml in managed_labels:
        if ml in dfp.labels:
            config["labels"][ml] = dfp.labels[ml]

    click.echo("---")
    click.echo("# populated from branch: {}".format(branch))
    yaml.safe_dump(config, sys.stdout, indent=2, default_flow_style=False)


@cli.command("images:query-rpm-version", short_help="Find the OCP version from the atomic-openshift RPM")
@click.option("--repo-type", metavar="REPO_TYPE", envvar="OIT_IMAGES_REPO_TYPE",
              default="unsigned",
              help="Repo group to scan for the RPM (e.g. signed, unsigned). env: OIT_IMAGES_REPO_TYPE")
@pass_runtime
def query_rpm_version(runtime, repo_type):
    """
    Retrieve the version number of the atomic-openshift RPM in the indicated
    repository. This is the version number that will be applied to new images
    created from this build.
    """
    runtime.initialize(clone_distgits=False)

    version = runtime.auto_version(repo_type)
    click.echo("version: {}".format(version))


@cli.command("cleanup", short_help="Cleanup the Doozer environment")
@pass_runtime
def cleanup(runtime):
    """
    Cleanup the OIT working environment.
    Currently this just clears out the working dir content
    """

    runtime.initialize(no_group=True)

    runtime.logger.info('Clearing out {}'.format(runtime.working_dir))

    ignore_list = ['settings.yaml']
    with Dir(runtime.working_dir):
        for ent in os.listdir("."):
            if ent in ignore_list:
                continue

            yellow_print(ent)
            # Otherwise, remove
            if os.path.isfile(ent) or os.path.islink(ent):
                os.remove(ent)
            else:
                shutil.rmtree(ent)


option_config_commit_msg = click.option("--message", "-m", metavar='MSG', help="Commit message for config change.", default=None)


# config:* commands are a special beast and
# requires the same non-standard runtime options
CONFIG_RUNTIME_OPTS = {
    'mode': 'both',           # config wants it all
    'clone_distgits': False,  # no need, just doing config
    'clone_source': False,    # no need, just doing config
    'disabled': True          # show all, including disabled/wip
}


# Normally runtime only runs in one mode as you never do
# rpm AND image operations at once. This is not so with config
# functions. This intelligently chooses modes for these only
def _fix_runtime_mode(runtime):
    mode = 'both'
    if runtime.rpms and not runtime.images:
        mode = 'rpms'
    elif runtime.images and not runtime.rpms:
        mode = 'images'

    CONFIG_RUNTIME_OPTS['mode'] = mode


@cli.command("config:commit", help="Commit pending changes from config:new")
@option_config_commit_msg
@click.option('--push/--no-push', default=False, is_flag=True,
              help='Push changes back to config repo. --no-push is default')
@pass_runtime
def config_commit(runtime, message, push):
    """
    Commit outstanding metadata config changes
    """
    _fix_runtime_mode(runtime)
    runtime.initialize(no_group=False, **CONFIG_RUNTIME_OPTS)

    # This is ok to run if automation is frozen as long as you are not pushing
    if push:
        runtime.assert_mutation_is_permitted()

    config = mdc(runtime)
    config.commit(message)
    if push:
        config.push()


@cli.command("config:push", help="Push all pending changes to config repo")
@pass_runtime
def config_push(runtime):
    """
    Push changes back to config repo.
    Will of course fail if user does not have write access.
    """
    _fix_runtime_mode(runtime)
    runtime.initialize(no_group=False, **CONFIG_RUNTIME_OPTS)
    config = mdc(runtime)
    config.push()


@cli.command("config:get", short_help="Pull latest config data into working directory")
@pass_runtime
def config_get(runtime):
    """
    Pull latest config data into working directory.
    This function exists as a convenience for working with the
    config manually.
    """
    _fix_runtime_mode(runtime)
    runtime.initialize(no_group=False, **CONFIG_RUNTIME_OPTS)


@cli.command("config:read-group", short_help="Output aspects of the group.yml")
@click.argument("key", nargs=1, metavar="KEY", type=click.STRING, default=None, required=False)
@click.option("--length", "as_len", default=False, is_flag=True, help='Print length of dict/list specified by key')
@click.option("--yaml", "as_yaml", default=False, is_flag=True, help='Print results in a yaml block')
@click.option("--permit-missing-group", default=False, is_flag=True, help='Show default if group is missing')
@click.option("--default", help="Value to print if key cannot be found", default=None)
@click.option("--out-file", help="Specific key in config to print", default=None)
@pass_runtime
def config_read_group(runtime, key, as_len, as_yaml, permit_missing_group, default, out_file):
    """
    Read data from group.yaml for given group and key, If key is not specified,
    the entire group data structure will be output.

    Usage:

    $ doozer --group=openshift-4.0 config:read-group [KEY] --yaml

    Where [KEY] is a key inside group.yaml that you would like to read.
    Key dot-notation is supported, such as: sources.ose.branch.fallback

    Examples:
    $ doozer --group=openshift-4.3 config:read-group sources.ose.url

    # Print yaml formatted list of non_release images (print empty list if not present).
    $ doozer --group=openshift-4.3 config:read-group --default '[]' --yaml non_release.images

    # How many images are in the non_release.images list (print 0 list if not present)
    $ doozer --group=openshift-4.3 config:read-group --default 0 --len non_release.images

    """
    _fix_runtime_mode(runtime)
    try:
        runtime.initialize(no_group=False, **CONFIG_RUNTIME_OPTS)
    except gitdata.GitDataException:
        # This may happen if someone if trying to get data for a branch that does not exist.
        # This may be perfectly OK if they are just trying to check the next minor's branch,
        # but that branch does not exist yet. Caller must specify --permit-missing to allow
        # this behavior.
        if permit_missing_group and default:
            print(default)
            exit(0)
        raise

    if key is None:
        value = runtime.raw_group_config
    else:
        value = dict_get(runtime.raw_group_config, key, None)
        if value is None:
            if default is not None:
                print(default)
                exit(0)
            raise DoozerFatalError('No default specified and unable to find key: {}'.format(key))

    if as_len:
        if hasattr(value, '__len__'):
            value = len(value)
        else:
            raise DoozerFatalError('Extracted element has no length: {}'.format(key))
    elif as_yaml:
        value = yaml.safe_dump(value, indent=2, default_flow_style=False)

    if out_file:
        # just in case
        out_file = os.path.expanduser(out_file)
        with io.open(out_file, 'w', encoding="utf-8") as f:
            f.write(value)

    print(str(value))


@cli.command("config:update-mode", short_help="Update config(s) mode. enabled|disabled|wip")
@click.argument("mode", nargs=1, metavar="MODE", type=click.Choice(metadata.CONFIG_MODES))  # new mode value
@click.option('--push/--no-push', default=False, is_flag=True,
              help='Push changes back to config repo. --no-push is default')
@option_config_commit_msg
@pass_runtime
def config_mode(runtime, mode, push, message):
    """Update [MODE] of given config(s) to one of:
    - enable: Normal operation
    - disable: Will not be used unless explicitly specified
    - wip: Same as `disable` plus affected by --wip flag

    Filtering of configs is based on usage of the following global options:
    --group, --images/-i, --rpms/-r

    See `doozer --help` for more.

    Usage:

    $ doozer --group=openshift-4.0 -i aos3-installation config:mode [MODE]

    Where [MODE] is one of enable, disable, or wip.

    Multiple configs may be specified and updated at once.

    Commit message will default to stating mode change unless --message given.
    If --push not given must use config:push after.
    """
    _fix_runtime_mode(runtime)
    if not runtime.load_wip and CONFIG_RUNTIME_OPTS['mode'] == 'both':
        red_print('Updating all mode for all configs in group is not allowed! Please specifiy configs directly.')
        sys.exit(1)
    runtime.initialize(**CONFIG_RUNTIME_OPTS)
    config = mdc(runtime)
    config.update('mode', mode)
    if not message:
        message = 'Updating [mode] to "{}"'.format(mode)
    config.commit(message)

    if push:
        config.push()


@cli.command("config:print", short_help="View config for given images / rpms")
@click.option("-n", "--name-only", default=False, is_flag=True, multiple=True,
              help="Just print name of matched configs. Overrides --key")
@click.option("--key", help="Specific key in config to print", default=None)
@click.option("--yaml", "as_yaml", default=False, is_flag=True, help='Print results in a yaml block')
@pass_runtime
def config_print(runtime, key, name_only, as_yaml):
    """Print name, sub-key, or entire config

    Filtering of configs is based on usage of the following global options:
    --group, --images/-i, --rpms/-r

    See `doozer --help` for more.

    Examples:

    Print all configs in group:

        $ doozer --group=openshift-4.0 config:print

    Print single config in group:

        $ doozer --group=openshift-4.0 -i aos3-installation config:print

    Print `owners` key from all configs in group:

        $ doozer --group=openshift-4.0 config:print --key owners

    Print only names of configs in group:

        $ doozer --group=openshift-4.0 config:print --name-only
    """
    _fix_runtime_mode(runtime)
    runtime.initialize(**CONFIG_RUNTIME_OPTS)
    config = mdc(runtime)
    config.config_print(key, name_only, as_yaml)


@cli.command("config:gen-csv", short_help="Generate .csv file for given images/rpms")
@click.option("--keys", help="Specific key in config to print, separated by commas: --keys key,name,owners",
              default=None)
@click.option("--type", "as_type", default=None, help='Write content type: image or rpm')
@click.option("--output", "-o", default=None, help='Write csv data to FILE instead of STDOUT')
@pass_runtime
def config_gencsv(runtime, keys, as_type, output):
    """Generate .csv file for given --keys and --type

    By default print out with STDOUT, you can use --output/-o to specify an output file

    Filtering of configs is based on usage of the following global options:
    --group, --images/-i, --rpms/-r

    See `doozer --help` for more.

    Examples:

    Generate a CSV where each row is distgit key, image name, and owners list:

        $ doozer --group=openshift-4.0 config:gen-csv --type image --keys key,name,owners

    Generate a CSV only include image aos3-installation each row is distgit key, image name, output to file ./image.csv:

        $ doozer --group=openshift-4.0 -i aos3-installation config:gen-csv --type image --keys key,name --output ./image.csv

    """
    _fix_runtime_mode(runtime)
    runtime.initialize(**CONFIG_RUNTIME_OPTS)
    config = mdc(runtime)
    config.config_gen_csv(keys, as_type, output)


@cli.command("release:calc-previous", short_help="Returns a list of releases to include in a release's previous field")
@click.option("--version", metavar='NEW_VER', required=True,
              help="The release to calculate previous for (e.g. 4.5.4 or 4.6.0..hotfix)")
@click.option("-a", "--arch",
              metavar='ARCH',
              help="Arch for which the repo should be generated",
              default='x86_64', required=False)
@click.option("--graph-url", metavar='GRAPH_URL', required=False,
              default='https://api.openshift.com/api/upgrades_info/v1/graph',
              help="Cincinnati graph URL to query")
@click.option("--graph-content-stable", metavar='JSON_FILE', required=False,
              help="Override content from stable channel - primarily for testing")
@click.option("--graph-content-candidate", metavar='JSON_FILE', required=False,
              help="Override content from stable channel - primarily for testing")
@click.option("--suggestions-url", metavar='SUGGESTIONS_URL', required=False,
              default="https://raw.githubusercontent.com/openshift/cincinnati-graph-data/master/build-suggestions/",
              help="Suggestions URL, load from {major}-{minor}-{arch}.yaml")
def release_calc_previous(version, arch, graph_url, graph_content_stable, graph_content_candidate, suggestions_url):
    major, minor = extract_version_fields(version, at_least=2)[:2]
    if arch == 'x86_64':
        arch = 'amd64'  # Cincinnati is go code, and uses a different arch name than brew

    # Refer to https://docs.google.com/document/d/16eGVikCYARd6nUUtAIHFRKXa7R_rU5Exc9jUPcQoG8A/edit
    # for information on channels & edges

    # Get the names of channels we need to analyze
    candidate_channel = get_cincinnati_channels(major, minor)[0]
    prev_candidate_channel = get_cincinnati_channels(major, minor - 1)[0]

    def sort_semver(versions):
        return sorted(versions, key=functools.cmp_to_key(semver.compare), reverse=True)

    def get_channel_versions(channel):
        """
        Queries Cincinnati and returns a tuple containing:
        1. All of the versions in the specified channel in decending order (e.g. 4.6.26, ... ,4.6.1)
        2. A map of the edges associated with each version (e.g. map['4.6.1'] -> [ '4.6.2', '4.6.3', ... ]
        :param channel: The name of the channel to inspect
        :return: (versions, edge_map)
        """
        content = None

        if channel == 'stable' and graph_content_stable:
            # permit command line override
            with open(graph_content_stable, 'r') as f:
                content = f.read()

        if channel != 'stable' and graph_content_candidate:
            # permit command line override
            with open(graph_content_candidate, 'r') as f:
                content = f.read()

        if not content:
            url = f'{graph_url}?arch={arch}&channel={channel}'
            req = urllib.request.Request(url)
            req.add_header('Accept', 'application/json')
            content = exectools.urlopen_assert(req).read()

        graph = json.loads(content)
        versions = [node['version'] for node in graph['nodes']]
        descending_versions = sort_semver(versions)

        edges: Dict[str, List] = dict()
        for v in versions:
            # Ensure there is at least an empty list for all versions.
            edges[v] = []

        for edge_def in graph['edges']:
            # edge_def example [22, 20] where is number is an offset into versions
            from_ver = versions[edge_def[0]]
            to_ver = versions[edge_def[1]]
            edges[from_ver].append(to_ver)

        return descending_versions, edges

    def get_build_suggestions(suggestions_url, major, minor, arch):
        """
        Loads suggestions_url/major.minor.yaml and returns minor_min, minor_max,
        minor_block_list, z_min, z_max, and z_block_list
        :param suggestions_url: Base url to /{major}.{minor}.yaml
        :param major: Major version
        :param minor: Minor version
        :param arch: Architecture to lookup
        :return: {minor_min, minor_max, minor_block_list, z_min, z_max, z_block_list}
        """
        url = f'{suggestions_url}/{major}.{minor}.yaml'
        req = urllib.request.Request(url)
        req.add_header('Accept', 'application/yaml')
        suggestions = yaml.safe_load(exectools.urlopen_assert(req))
        if arch in suggestions:
            return suggestions[arch]
        else:
            return suggestions['default']

    upgrade_from = set()
    prev_versions, prev_edges = get_channel_versions(prev_candidate_channel)
    curr_versions, current_edges = get_channel_versions(candidate_channel)
    suggestions = get_build_suggestions(suggestions_url, major, minor, arch)
    for v in prev_versions:
        if (semver.VersionInfo.parse(v) >= semver.VersionInfo.parse(suggestions['minor_min'])
                and semver.VersionInfo.parse(v) < semver.VersionInfo.parse(suggestions['minor_max'])
                and v not in suggestions['minor_block_list']):
            upgrade_from.add(v)
    for v in curr_versions:
        if (semver.VersionInfo.parse(v) >= semver.VersionInfo.parse(suggestions['z_min'])
                and semver.VersionInfo.parse(v) < semver.VersionInfo.parse(suggestions['z_max'])
                and v not in suggestions['z_block_list']):
            upgrade_from.add(v)

    candidate_channel_versions, candidate_edges = get_channel_versions(candidate_channel)
    # 'nightly' was an older convention. This nightly variant check can be removed by Oct 2020.
    if 'nightly' not in version and 'hotfix' not in version:
        # If we are not calculating a previous list for standard release, we want edges from previously
        # released hotfixes to be valid for this node IF and only if that hotfix does not
        # have an edge to TWO previous standard releases.
        # ref: https://docs.google.com/document/d/16eGVikCYARd6nUUtAIHFRKXa7R_rU5Exc9jUPcQoG8A/edit

        # If a release name in candidate contains 'hotfix', it was promoted as a hotfix for a customer.
        previous_hotfixes = list(filter(lambda release: 'nightly' in release or 'hotfix' in release, candidate_channel_versions))
        # For each hotfix that doesn't have 2 outgoing edges, and it as an incoming edge to this release
        for hotfix_version in previous_hotfixes:
            if len(candidate_edges[hotfix_version]) < 2:
                upgrade_from.add(hotfix_version)

    results = sort_semver(list(upgrade_from))
    print(','.join(results))


@cli.command("beta:reposync", short_help="Sync yum repos listed in group.yaml to local directory.")
@click.option("-o", "--output", metavar="DIR", help="Output directory to sync to", required=True)
@click.option("-c", "--cachedir", metavar="DIR", help="Cache directory for yum", required=True)
@click.option("-a", "--arch",
              metavar='ARCH',
              help="Arch for which the repo should be generated",
              default='x86_64', required=False)
@click.option("--repo-type", metavar="REPO_TYPE", envvar="OIT_IMAGES_REPO_TYPE",
              default="unsigned",
              help="Repo group type to use for repo file generation (e.g. signed, unsigned).")
@click.option("-n", "--name", "names", default=[], metavar='NAME', multiple=True,
              help="Only sync the specified repository names; if not specified all will be synced.")
@click.option('--dry-run', default=False, is_flag=True,
              help='Print derived yum configuration for sync operation and exit')
@pass_runtime
def beta_reposync(runtime, output, cachedir, arch, repo_type, names, dry_run):
    """Sync yum repos listed in group.yaml to local directory.
    See `doozer --help` for more.

    Examples:

    Perform reposync operation for ppc64le architecture, preferring unsigned repos:

        $ doozer --group=openshift-4.0 beta:reposync -o /tmp/repo_sync -c /tmp/cache/ --repo-type unsigned --arch ppc64le

    Synchronize signed, x86_64, rpms from rhel-server-ose-rpms repository.

        $ doozer --group=openshift-4.0 beta:reposync -o /tmp/repo_sync -c /tmp/cache/ --repo-type signed -r rhel-server-ose-rpms

    """
    runtime.initialize(clone_distgits=False)
    repos = runtime.repos

    yum_conf = """
[main]
cachedir={}/$basearch/$releasever
keepcache=0
debuglevel=2
logfile={}/yum.log
exactarch=1
obsoletes=1
gpgcheck=1
plugins=1
installonly_limit=3
""".format(cachedir, runtime.working_dir)

    optional_fails = []

    repos_content = repos.repo_file(repo_type, enabled_repos=None, arch=arch)
    content = "{}\n\n{}".format(yum_conf, repos_content)

    if dry_run:
        print(content)
        return

    if not os.path.isdir(cachedir):
        yellow_print('Creating cachedir: {}'.format(cachedir))
        exectools.cmd_assert('mkdir -p {}'.format(cachedir))

    try:
        # If corrupted, reposync metadata can interfere with subsequent runs.
        # Ensure we have a clean space for each invocation.
        metadata_dir = tempfile.mkdtemp(prefix='reposync-metadata.', dir=cachedir)
        yc_file = tempfile.NamedTemporaryFile()
        yc_file.write(content.encode('utf-8'))

        # must flush so it can be read
        yc_file.flush()

        exectools.cmd_assert('yum clean all')  # clean the cache first to avoid outdated repomd.xml files

        for repo in repos.values():

            # If specific names were specified, only synchronize them.
            if names and repo.name not in names:
                continue

            if not repo.is_reposync_enabled():
                runtime.logger.info('Skipping repo {} because reposync is disabled in group.yml'.format(repo.name))
                continue

            color_print('Syncing repo {}'.format(repo.name), 'blue')
            cmd = f'reposync -c {yc_file.name} -p {output} --delete --arch {arch} -r {repo.name} -e {metadata_dir}'
            if repo.is_reposync_latest_only():
                cmd += ' -n'

            rc, out, err = exectools.cmd_gather(cmd, realtime=True)
            if rc != 0:
                if not repo.cs_optional:
                    raise DoozerFatalError(err)
                else:
                    runtime.logger.warning('Failed to sync repo {} but marked as optional: {}'.format(repo.name, err))
                    optional_fails.append(repo.name)
            else:
                rc, out, err = exectools.cmd_gather('createrepo_c {}'.format(os.path.join(output, repo.name)))
                if rc != 0:
                    if not repo.cs_optional:
                        raise DoozerFatalError(err)
                    else:
                        runtime.logger.warning('Failed to run createrepo_c on {} but marked as optional: {}'.format(repo.name, err))
                        optional_fails.append(repo.name)
    finally:
        yc_file.close()
        shutil.rmtree(metadata_dir, ignore_errors=True)

    if optional_fails:
        yellow_print('Completed with the following optional repos skipped or partial due to failure, see log.:\n{}'.format('\n'.join(optional_fails)))
    else:
        green_print('Repos synced to {}'.format(output))


@cli.command("config:update-required", short_help="Update images that are required")
@click.option("--image-list", help="File with list of images, one per line.", required=True)
@pass_runtime
def config_update_required(runtime, image_list):
    """Ingest list of images and update data repo
    with which images are required and which are not.
    """
    _fix_runtime_mode(runtime)
    runtime.initialize(**CONFIG_RUNTIME_OPTS)

    with io.open(image_list, 'r', encoding="utf-8") as il:
        image_list = [i.strip() for i in il.readlines() if i.strip()]

    resolved = []
    required = []
    optional = []
    for img in runtime.image_metas():
        name = img.image_name
        slash = img.image_name.find('/')
        if slash >= 0:
            name = name[slash + 1:]
        found = False
        for i in image_list:
            if i == name or i == name.replace('ose-', ''):
                required.append(img)
                resolved.append(i)
                found = True
                green_print('{} -> {}'.format(img.distgit_key, i))
                break
        if not found:
            optional.append(img)

    missing = list(set(image_list) - set(resolved))
    if missing:
        yellow_print('\nThe following images in the data set could not be resolved:')
        yellow_print('\n'.join(missing))

    for img in required:
        msg = 'Updating {} to be required'.format(img.distgit_key)
        color_print(msg, color='blue')

        data_obj = runtime.gitdata.load_data(path='images', key=img.distgit_key)
        data_obj.data['required'] = True
        data_obj.save()

    for img in optional:
        msg = 'Updating {} to be optional'.format(img.distgit_key)
        color_print(msg, color='blue')

        data_obj = runtime.gitdata.load_data(path='images', key=img.distgit_key)
        data_obj.data.pop('required', None)
        data_obj.save()

    green_print('\nComplete! Remember to commit and push the changes!')


@cli.command("operator-metadata:build", short_help="Update operator(s) metadata")
@click.argument("nvr_list", nargs=-1, required=True)
@click.option("--stream", required=True, type=click.Choice(['dev', 'stage', 'prod']),
              help="Metadata repo to apply update. Possible values: dev, stage, prod.")
@click.option("--image-ref-mode", required=False, type=click.Choice(['by-arch', 'manifest-list']),
              help="Build mode for image references. Possible values: by-arch, manifest-list (default: group.yml operator_image_ref_mode)")
@click.option("--merge-branch", required=False,
              help="Branch to be updated on the metadata repo (default: rhaos-4-rhel-7)")
@click.option("-f", "--force", required=False, is_flag=True,
              help="Perform a build even if there is nothing new on the metadata repo")
@pass_runtime
def update_operator_metadata(runtime, nvr_list, stream, image_ref_mode, merge_branch, force=False):
    """Update the corresponding metadata repositories of the given operators
    and build "metadata containers" with nothing but a collection of manifests on them.

    nvr_list: One or more NVRs to have their corresponding metadata repos updated (i.e.: cluster-logging-operator-container-v4.2.0-201908070219)
    merge_branch: metadata repos have multiple branches (dev, stage and prod) representing the destination of the updates.

    Usage example with single NVR:

    $ doozer -g openshift-4.2 operator-metadata:build cluster-logging-operator-container-v4.2.0-201908070219 --stream dev --merge-branch rhaos-4-rhel-8

    Usage example with multiple NVRs:

    $ doozer -g openshift-4.2 operator-metadata:build cluster-logging-operator-container-v4.2.0-201908070219 elasticsearch-operator-container-v4.2.0-201908070219 --stream prod
    """
    runtime.initialize(clone_distgits=False)
    runtime.assert_mutation_is_permitted()

    if not image_ref_mode:
        image_ref_mode = runtime.group_config.operator_image_ref_mode

    if not merge_branch:
        merge_branch = 'rhaos-4-rhel-7'
        # @TODO: populate merge_branch from runtime once the following MRs are merged:
        #     https://gitlab.cee.redhat.com/openshift-art/ocp-build-data/merge_requests/222
        #     https://gitlab.cee.redhat.com/openshift-art/ocp-build-data/merge_requests/223
        # merge_branch = runtime.group_config.operator_metadata_branch

    try:

        def gotta_catchem_all(*args, **kwargs):
            # Threadpool swallow stack traces from workers; make sure to print out details if they occur
            try:
                return operator_metadata.update_and_build(*args, **kwargs)
            except:
                runtime.logger.error('Error building operator metadata for: {}'.format(args))
                traceback.print_exc()
                raise

        ok = ThreadPool(cpu_count()).map(gotta_catchem_all, [
            (nvr, stream, runtime, image_ref_mode, merge_branch, force) for nvr in nvr_list
        ])
        # @TODO: save state.yaml (check how to do that)
    except Exception as e:
        ok = [False]
        red_print(str(e))

    sys.exit(0 if all(ok) else 1)


@cli.command("operator-metadata:latest-build", short_help="Print latest metadata build of a given operator")
@click.option("--nvr", "-n", help="Specify Name-Version-Release. Needs --stream", multiple=True)
@click.option("--stream", "-s", type=click.Choice(['dev', 'stage', 'prod']),
              help="Metadata stream to select. Possible values: dev, stage, prod. Needs --nvr")
@click.argument("operator_list", nargs=-1)
@pass_runtime
def operator_metadata_latest_build(runtime, nvr, stream, operator_list):
    """Print the latest metadata build of a given operator

    operator_list: One or more operator names

    Usage example:

    $ doozer -g openshift-4.2 operator-metadata:latest-build cluster-logging-operator

    Usage example with multiple operators:

    $ doozer -g openshift-4.2 operator-metadata:latest-build cluster-logging-operator elasticsearch-operator

    Usage example when feeding NVR and stream:

    $ doozer -g openshift-4.2 operator-metadata:latest-build --stream stage --nvr cluster-logging-operator-container-v4.2.1-201910221723 --nvr sriov-network-operator-container-v4.2.0-201909131819
    """

    if operator_list and (stream or nvr):
        click.echo('Need either a list of operators, or specified by NVR and stream. Exiting.', err=True)
        sys.exit(1)
    if stream and not nvr:
        click.echo('--stream needs to be accompanied by --nvr. Exiting.', err=True)
        sys.exit(1)
    if nvr and not stream:
        click.echo('--nvr needs to be accompanied by --stream. Exiting.', err=True)
        sys.exit(1)
    if not nvr and not operator_list:
        click.echo("Missing arguments: Need nvr's or operators to work with")
        sys.exit(1)

    runtime.initialize(clone_distgits=False)

    if operator_list:
        for operator in operator_list:
            click.echo(operator_metadata.OperatorMetadataLatestBuildReporter(operator, runtime).get_latest_build())
    else:
        for build in nvr:
            click.echo(operator_metadata.OperatorMetadataLatestNvrReporter(build, stream, runtime).get_latest_build())


@cli.command("analyze:debug-log", short_help="Output an analysis of the debug log")
@click.argument("debug_log", nargs=1)
def analyze_debug_log(debug_log):
    """
    Specify a doozer working directory debug.log as the argument. A table with be printed to stdout.
    The top row of the table will be all the threads that existed during the doozer runtime.
    T1, T2, T3, ... each represent an independent thread which was created and performed some action.

    The first column of the graph indicates a time interval. Each interval is 10 seconds. Actions
    performed by each thread are grouped into the interval in which those actions occurred. The
    action performed by a thread is aligned and printed under the thread's column.

    Example 1:
    *     T1    T2    T3
    0     loading metadata A                          # During the first 10 seconds, T1 is perform actions.
     0    loading metaddata B                         # Multiple events can happen during the same interval.
     0    loading metaddata C

    Example 2:
    *     T1    T2    T3
    0     loading metadata A                          # During the first 10 seconds, T1 is perform actions.
     0    loading metaddata B                         # Multiple events can happen during the same interval.
    1           cloning                               # During seconds 10-20, three threads start cloning.
     1    cloning
     1                cloning
    """
    f = os.path.abspath(debug_log)
    analyze_debug_timing(f)


@cli.command('olm-bundle:list-olm-operators', short_help='List all images that are OLM operators')
@pass_runtime
def list_olm_operators(runtime):
    """
    Example:
    $ doozer --group openshift-4.5 olm-bundle:list-olm-operators
    """
    runtime.initialize(clone_distgits=False)

    for image in runtime.image_metas():
        if image.enabled and image.config['update-csv'] is not Missing:
            print(image.get_component_name())


@cli.command("olm-bundle:print", short_help="Print data for each operator",
             context_settings=dict(
                 ignore_unknown_options=True,  # permit patterns starting with dash; allows printing yaml lists
             ))
@click.argument("pattern", default="{component}", nargs=1)
@pass_runtime
def olm_bundles_print(runtime, pattern):
    """
    Prints data from each distgit. The pattern specified should be a string
    with replacement fields:

    \b
    {distgit_key} - The distgit key of the operator
    {component} - The component identified in the Dockerfile
    {nvr} - NVR of latest operator build
    {bundle_component} - The operator's bundle component name
    {paired_bundle_nvr} - NVR of bundle associated with the latest operator NVR (may not exist)
    {paired_bundle_pullspec} - The pullspec for the bundle associated with the latest operator NVR (may not exist)
    {bundle_nvr} - NVR of latest operator bundle build
    {bundle_pullspec} - The pullspec for the latest build
    {lf} - Line feed

    If pattern contains no braces, it will be wrapped with them automatically. For example:
    "component" will be treated as "{component}"
    """

    runtime.initialize(clone_distgits=False)

    # If user omitted braces, add them.
    if "{" not in pattern:
        pattern = "{%s}" % pattern.strip()

    for image in runtime.ordered_image_metas():
        if not image.enabled or image.config['update-csv'] is Missing:
            continue
        olm_bundle = OLMBundle(runtime)

        s = pattern
        s = s.replace("{lf}", "\n")
        s = s.replace("{distgit_key}", image.distgit_key)
        s = s.replace("{component}", image.get_component_name())

        build_info = image.get_latest_build()
        nvr = build_info['nvr']
        s = s.replace('{nvr}', nvr)
        olm_bundle.get_operator_buildinfo(nvr=nvr)  # Populate the object for the operator we are interested in.
        s = s.replace('{bundle_component}', olm_bundle.bundle_brew_component)
        bundle_image_name = olm_bundle.get_bundle_image_name()

        if '{paired_' in s:
            # Paired bundle values must correspond exactly to the latest operator NVR.
            paired_bundle_nvr = olm_bundle.find_bundle_for(nvr)
            if not paired_bundle_nvr:
                paired_bundle_nvr = 'None'  # Doesn't exist
                paired_pullspec = 'None'
            else:
                paired_version_release = paired_bundle_nvr[len(olm_bundle.bundle_brew_component) + 1:]
                paired_pullspec = runtime.resolve_brew_image_url(f'{bundle_image_name}:{paired_version_release}')
            s = s.replace('{paired_bundle_nvr}', paired_bundle_nvr)
            s = s.replace('{paired_bundle_pullspec}', paired_pullspec)

        if '{bundle_' in s:
            # Unpaired is just whatever bundle build was most recent
            build = olm_bundle.get_latest_bundle_build()
            if not build:
                bundle_nvr = 'None'
                bundle_pullspec = 'None'
            else:
                bundle_nvr = build['nvr']
                version_release = bundle_nvr[len(olm_bundle.bundle_brew_component) + 1:]
                # Build pullspec like: registry-proxy.engineering.redhat.com/rh-osbs/openshift-ose-clusterresourceoverride-operator-bundle:v4.7.0.202012082225.p0-1
                bundle_pullspec = runtime.resolve_brew_image_url(f'{bundle_image_name}:{version_release}')

            s = s.replace('{bundle_nvr}', bundle_nvr)
            s = s.replace('{bundle_pullspec}', bundle_pullspec)

        if "{" in s:
            raise IOError("Unrecognized fields remaining in pattern: %s" % s)

        click.echo(s)


@cli.command('olm-bundle:rebase', short_help='Update bundle distgit repo with manifests from given operator NVR')
@click.argument('operator_nvrs', nargs=-1, required=True)
@pass_runtime
def rebase_olm_bundle(runtime, operator_nvrs):
    """
    Example:
    $ doozer --group openshift-4.2 olm-bundle:rebase \
        sriov-network-operator-container-v4.2.30-202004200449 \
        elasticsearch-operator-container-v4.2.30-202004240858 \
        cluster-logging-operator-container-v4.2.30-202004240858
    """
    runtime.initialize(clone_distgits=False)
    ThreadPool(len(operator_nvrs)).map(lambda nvr: OLMBundle(runtime).rebase(nvr), operator_nvrs)


@cli.command('olm-bundle:build', short_help='Build bundle containers of given operators')
@click.argument('operator_names', nargs=-1, required=True)
@pass_runtime
def build_olm_bundle(runtime, operator_names):
    """
    Example:
    $ doozer --group openshift-4.2 olm-bundle:build \
        sriov-network-operator \
        elasticsearch-operator \
        cluster-logging-operator
    """
    runtime.initialize(clone_distgits=False)
    ThreadPool(len(operator_names)).map(lambda name: OLMBundle(runtime).build(name), operator_names)


@cli.command('olm-bundle:rebase-and-build', short_help='Shortcut for olm-bundle:rebase and olm-bundle:build')
@click.argument('operator_nvrs', nargs=-1, required=True)
@click.option("-f", "--force", required=False, is_flag=True,
              help="Perform a build even if previous bundles for given NVRs already exist")
@pass_runtime
def rebase_and_build_olm_bundle(runtime, operator_nvrs, force=False):
    """Having in mind that its primary use will be inside a Jenkins job, this command combines the
    execution of both commands in sequence (since they are commonly used together anyway), using the
    same OLMBundle instance, in order to save time and avoid fetching the same information twice.

    Also, the output of this command is more verbose, in order to provide relevant info back to the
    next steps of the Jenkins job, avoiding the need to retrieve a lot of data via Groovy.

    Example:
    $ doozer --group openshift-4.2 olm-bundle:rebase-and-build \
        sriov-network-operator-container-v4.2.30-202004200449 \
        elasticsearch-operator-container-v4.2.30-202004240858 \
        cluster-logging-operator-container-v4.2.30-202004240858
    """
    runtime.initialize(clone_distgits=False)

    def rebase_and_build(nvr):
        try:
            olm_bundle = OLMBundle(runtime)

            bundle_nvr = olm_bundle.find_bundle_for(nvr)
            if bundle_nvr and not force:
                return {
                    'success': True,
                    'task_url': None,
                    'bundle_nvr': bundle_nvr,
                }

            did_rebase = olm_bundle.rebase(nvr)
            return {
                'success': olm_bundle.build() if did_rebase else True,
                'task_url': olm_bundle.task_url if hasattr(olm_bundle, 'task_url') else None,
                'bundle_nvr': olm_bundle.get_latest_bundle_build_nvr(),
            }
        except:
            runtime.logger.error('Error during rebase or build for: {}'.format(nvr))
            traceback.print_exc()
            return {
                'success': False,
                'task_url': None,
                'bundle_nvr': None,
            }

    results = ThreadPool(len(operator_nvrs)).map(rebase_and_build, operator_nvrs)

    for result in results:
        runtime.logger.info('SUCCESS={success} {task_url} {bundle_nvr}'.format(**result))
        click.echo(result['bundle_nvr'])

    rc = 0 if all(list(map(lambda i: i['success'], results))) else 1

    if rc:
        runtime.logger.error('One or more bundles failed; look above for SUCCESS=False')

    sys.exit(rc)


def main():
    try:
        if 'REQUESTS_CA_BUNDLE' not in os.environ:
            os.environ['REQUESTS_CA_BUNDLE'] = '/etc/pki/tls/certs/ca-bundle.crt'

        cli(obj={})
    except DoozerFatalError as ex:
        # Allow capturing actual tool errors and print them
        # nicely instead of a gross stack-trace.
        # All internal errors that should simply cause the app
        # to exit with an error code should use DoozerFatalError
        red_print('\nDoozer Failed With Error:\n' + str(ex))

        if cli_package.CTX_GLOBAL and cli_package.CTX_GLOBAL.obj:
            cli_package.CTX_GLOBAL.obj.state['status'] = state.STATE_FAIL
            cli_package.CTX_GLOBAL.obj.state['msg'] = str(ex)
        sys.exit(1)
    finally:
        if cli_package.CTX_GLOBAL and cli_package.CTX_GLOBAL.obj and cli_package.CTX_GLOBAL.obj.initialized:
            cli_package.CTX_GLOBAL.obj.save_state()


if __name__ == '__main__':
    main()
