import json
import multiprocessing
import sys
from typing import Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import click
import yaml

from doozerlib import Runtime, brew, exectools
from doozerlib import build_status_detector as bs_detector
from doozerlib.cli import cli, pass_runtime
from doozerlib.exceptions import DoozerFatalError
from doozerlib.util import green_print


@click.group("detect-embargo", short_help="Check whether one or more images, RPMs, or release payloads have embargoed fixes.")
def detect_embargo():
    pass


cli.add_command(detect_embargo)


@detect_embargo.command("nvr", short_help="Detect embargoed fixes in given builds")
@click.option("--yaml", "as_yaml", is_flag=True,
              help="Print out the result as YAML format.")
@click.option("--json", "as_json", is_flag=True,
              help="Print out the result as JSON format.")
@click.argument("nvrs", metavar="NVRS...", nargs=-1, required=True)
@pass_runtime
def detect_in_nvr(runtime: Runtime, nvrs, as_yaml, as_json):
    """ Check whether one or more Brew builds have embargoed fixes.

    If neither --yaml nor --json is given, this program will exit with status 0 if true, or with status 2 if not.
    Errors are signaled by a non-zero status that is not 2.

    Example:

        $ doozer --group=openshift-4.6 detect-embargo nvr openshift-4.6.0-202007110420.p1.git.0.4de1d1d.el8 openshift-clients-4.6.0-202007152229.p0.git.3651.9a1d25f.el8
    """
    if as_yaml and as_json:
        raise click.BadParameter("Must use one of --yaml or --json.")
    runtime.initialize(clone_distgits=False)
    embargoed_builds = detect_embargoes_in_nvrs(runtime, nvrs)
    print_result_and_exit(embargoed_builds, None, None, as_yaml, as_json)


@detect_embargo.command("tag", short_help="Detect embargoed fixes in builds with given Koji tags")
@click.option("--kind", "kind", default="all", metavar="KIND", type=click.Choice(["rpm", "image", "all"]),
              help="Only detect the specified kind of builds. [rpm, image, all] (Default to all)")
@click.option("--exclude", "excluded_tags", metavar="TAG", multiple=True,
              help="Koji tag name that the build must not have [multiple]")
@click.option("--event-id", metavar='NUM', required=False, type=int,
              help="A Brew event ID. If specified, the last NVRs as of the given Brew event will be chosen instead of latest")
@click.option("--yaml", "as_yaml", is_flag=True,
              help="Print out the result as YAML format.")
@click.option("--json", "as_json", is_flag=True,
              help="Print out the result as JSON format.")
@click.argument("tags", metavar="TAGS...", nargs=-1, required=True)
@pass_runtime
def detect_in_tag(runtime: Runtime, kind, tags, excluded_tags, event_id, as_yaml, as_json):
    """ Check whether one or more Brew tags have builds that include embargoed fixes.

    If neither --yaml nor --json is given, this program will exit with status 0 if true, or with status 2 if not.
    Errors are signaled by a non-zero status that is not 2.

    Example:
        $ doozer --group=openshift-4.6 detect-embargo tag rhaos-4.6-rhel-8-candidate rhaos-4.6-rhel-7-candidate --exclude rhaos-4.6-rhel-8  --exclude rhaos-4.6-rhel-7
    """
    if as_yaml and as_json:
        raise click.BadParameter("Must use one of --yaml or --json.")
    runtime.initialize(clone_distgits=False)
    embargoed_builds = detect_embargoes_in_tags(runtime, kind, tags, excluded_tags, event_id)
    print_result_and_exit(embargoed_builds, None, None, as_yaml, as_json)


@detect_embargo.command("pullspec", short_help="Check whether one or more arbitrary brew images (referred by pullspecs) has embargoed fixes.")
@click.option("--yaml", "as_yaml", is_flag=True,
              help="Print out the result as YAML format.")
@click.option("--json", "as_json", is_flag=True,
              help="Print out the result as JSON format.")
@click.argument("pullspecs", metavar="PULLSPECS...", nargs=-1, required=True)
@pass_runtime
def detect_in_pullspec(runtime, pullspecs, as_yaml, as_json):
    """ Check whether one or more arbitrary container images (referred by pullspecs) have embargoed fixes.

    If neither --yaml nor --json is given, this program will exit with status 0 if true, or with status 2 if not.
    Errors are signaled by a non-zero status that is not 2.

    Example:

        $ doozer --group=openshift-4.6 detect-embargo pullspec registry-proxy.engineering.redhat.com/rh-osbs/openshift-ose-ironic@sha256:caa56dc45580ec5103564125cbb87f41559aec89de4a8efd099777463381180c
    """
    if as_yaml and as_json:
        raise click.BadParameter("Must use one of --yaml or --json.")
    runtime.initialize(clone_distgits=False)
    embargoed_pullspecs, embargoed_builds = detect_embargoes_in_pullspecs(runtime, pullspecs)
    print_result_and_exit(embargoed_builds, embargoed_pullspecs, None, as_yaml, as_json)


@detect_embargo.command("release", short_help="Check whether one or more release payloads has embargoed fixes.")
@click.option("--yaml", "as_yaml", is_flag=True,
              help="Print out the result as YAML format.")
@click.option("--json", "as_json", is_flag=True,
              help="Print out the result as JSON format.")
@click.argument("pullspecs", metavar="PULLSPECS...", nargs=-1, required=True)
@pass_runtime
def detect_in_release(runtime, pullspecs, as_yaml, as_json):
    """ Check whether one or more release payloads have images with embargoed fixes.

    If neither --yaml nor --json is given, this program will exit with status 0 if true, or with status 2 if not.
    Errors are signaled by a non-zero status that is not 2.

    Example:

        $ doozer --group=openshift-4.6 detect-embargo release registry.svc.ci.openshift.org/ocp/release:4.6.0-0.nightly-2020-07-17-043219
    """
    if as_yaml and as_json:
        raise click.BadParameter("Must use one of --yaml or --json.")
    runtime.initialize(clone_distgits=False)
    embargoed_releases, embargoed_pullspecs, embargoed_builds = detect_embargoes_in_releases(runtime, pullspecs)
    print_result_and_exit(embargoed_builds, embargoed_pullspecs, embargoed_releases, as_yaml, as_json)


def detect_embargoes_in_nvrs(runtime: Runtime, nvrs: List[str]):
    """ Finds embargoes in given NVRs
    :param runtime: the runtime
    :param nvrs: list of build NVRs
    :return: list of Brew build dicts that have embargoed fixes
    """
    runtime.logger.info(f"Fetching {len(nvrs)} builds from Brew...")
    brew_session = runtime.build_retrying_koji_client()
    builds = brew.get_build_objects(nvrs, brew_session)
    for i, b in enumerate(builds):
        if not b:
            raise DoozerFatalError(f"Unable to get {nvrs[i]} from Brew.")
    runtime.logger.info(f"Detecting embargoes for {len(nvrs)} builds...")
    detector = bs_detector.BuildStatusDetector(brew_session, runtime.logger)
    embargoed_build_ids = detector.find_embargoed_builds(builds)
    embargoed_builds = [b for b in builds if b["id"] in embargoed_build_ids]
    return embargoed_builds


def detect_embargoes_in_tags(runtime: Runtime, kind: str, included_tags: List[str], excluded_tags: List[str], event_id: Optional[int]):
    """ Finds embargoes in builds with given tags
    :param runtime: the runtime
    :param included_tags: list of koji tags that the returned builds must have
    :param excluded_tags: list of koji tags that the returned builds must not have
    :return: list of Brew build dicts that have embargoed fixes
    """
    brew_session = runtime.build_retrying_koji_client()
    runtime.logger.info(f"Fetching builds from Brew tags {included_tags}...")
    build_type = None if kind == "all" else kind
    latest_build_lists = brew.get_latest_builds([(tag, None) for tag in included_tags], build_type, event_id, brew_session)
    included_builds = [b for builds in latest_build_lists if builds for b in builds]  # flatten latest_build_lists
    runtime.logger.info(f"Found {len(included_builds)} builds from Brew tags {included_tags}.")
    if included_builds and excluded_tags:  # if we have tags to exclude, get all builds with excluded_tags then exclude them
        runtime.logger.info(f"Fetching builds from Brew tags {excluded_tags}...")
        excluded_build_lists = brew.get_tagged_builds(excluded_tags, build_type, event_id, brew_session)
        excluded_build_ids = {b["id"] for builds in excluded_build_lists if builds for b in builds}
        builds = [b for b in included_builds if b["id"] not in excluded_build_ids]
        runtime.logger.info(f"Excluded {len(included_builds) - len(builds)} builds that are also tagged into {excluded_tags}.")
        included_builds = builds

    # Builds may have duplicate entries if we query from multiple tags. Don't worry, BuildStatusDetector is smart.
    runtime.logger.info(f"Detecting embargoes for {len(included_builds)} builds...")
    detector = bs_detector.BuildStatusDetector(brew_session, runtime.logger)
    embargoed_build_ids = detector.find_embargoed_builds(included_builds)
    embargoed_builds = [b for b in included_builds if b["id"] in embargoed_build_ids]
    return embargoed_builds


def detect_embargoes_in_pullspecs(runtime: Runtime, pullspecs: List[str]):
    """ Finds embargoes in given image pullspecs
    :param runtime: the runtime
    :param nvrs: list of image pullspecs
    :return: list of Brew build dicts that have embargoed fixes
    """
    runtime.logger.info(f"Fetching manifests for {len(pullspecs)} pullspecs...")
    jobs = runtime.parallel_exec(lambda pullspec, _: get_nvr_by_pullspec(pullspec), pullspecs,
                                 min(len(pullspecs), multiprocessing.cpu_count() * 4, 32))
    nvrs = jobs.get()
    suspect_nvrs = []
    suspect_pullspecs = []
    for index, nvr in enumerate(nvrs):
        n, v, r = nvr
        if not n or not v or not r:
            runtime.logger.warning(f"Assuming {pullspecs[index]} is not embargoed because it doesn't have valid NVR labels.")
            continue
        suspect_nvrs.append(f"{n}-{v}-{r}")
        suspect_pullspecs.append(pullspecs[index])

    embargoed_builds = detect_embargoes_in_nvrs(runtime, suspect_nvrs)
    embargoed_build_nvrs = {b["nvr"] for b in embargoed_builds}
    embargoed_pullspecs = [pullspec for index, pullspec in enumerate(suspect_pullspecs) if suspect_nvrs[index] in embargoed_build_nvrs]
    return embargoed_pullspecs, embargoed_builds


def detect_embargoes_in_releases(runtime: Runtime, pullspecs: List[str]):
    """ Finds embargoes in given release payloads
    :param runtime: the runtime
    :param nvrs: list of release pullspecs
    :return: list of Brew build dicts that have embargoed fixes
    """
    runtime.logger.info(f"Fetching component pullspecs from {len(pullspecs)} release payloads...")
    jobs = runtime.parallel_exec(lambda pullspec, _: get_image_pullspecs_from_release_payload(pullspec), pullspecs,
                                 min(len(pullspecs), multiprocessing.cpu_count() * 4, 32))
    pullspec_lists = jobs.get()
    embargoed_releases = []
    embargoed_pullspecs = []
    embargoed_builds = []
    for index, image_pullspecs in enumerate(pullspec_lists):
        p, b = detect_embargoes_in_pullspecs(runtime, list(image_pullspecs))
        if p:  # release has embargoes
            embargoed_releases.append(pullspecs[index])
            embargoed_pullspecs += p
            embargoed_builds += b
    return embargoed_releases, embargoed_pullspecs, embargoed_builds


def print_result_and_exit(embargoed_builds, embargoed_pullspecs, embargoed_releases, as_yaml, as_json):
    """ Prints embargo detection result and exit
    :param embargoed_builds: list of dicts of embargoed builds
    :embargoed_pullspecs: list of embargoed image pullspecs
    :embargoed_releases: list of pullspecs of embargoed release payloads
    :as_yaml: if true, print the result as an YAML document
    :as_json: if true, print the result as a JSON document
    """
    out = {
        "has_embargoes": bool(embargoed_builds)
    }
    if embargoed_builds:
        out["builds"] = embargoed_builds
    if embargoed_pullspecs:
        out["pullspecs"] = embargoed_pullspecs
    if embargoed_builds:
        out["releases"] = embargoed_releases
    if as_yaml:
        yaml.dump(out, sys.stdout)
    elif as_json:
        json.dump(out, sys.stdout)
    elif not embargoed_builds:
        green_print("No builds contain embargoed fixes.")
        exit(2)  # we use this special exit status to indicate no embargoed fixes are detected.
        return  # won't reach here. it is for spoofing the unittest when exit is mocked.
    else:
        if embargoed_releases:
            green_print(f"Found {len(embargoed_releases)} release payload containing embargoed fixes:")
            for release in embargoed_releases:
                green_print(release)
        green_print(f"Found {len(embargoed_builds)} builds containing embargoed fixes:")
        for index, build in enumerate(embargoed_builds):
            line = f"{build['id']}\t{build['nvr']}"
            if embargoed_pullspecs:
                line += f"\t{embargoed_pullspecs[index]}"
            green_print(line)
    exit(0)


def get_nvr_by_pullspec(pullspec: str) -> Tuple[str, str, str]:
    """ Retrieves (name, version, release) of the image referenced by a pullspec
    :param pullspec: image pullspec
    :return: a (name, version, release) tuple
    """
    if not urlparse(pullspec).scheme:
        pullspec = "docker://" + pullspec
    # If pullspec references a manifest list, 'skopeo inspect --config' should be able to process it under the hood
    out, _ = exectools.cmd_assert(["skopeo", "inspect", "--config", "--", pullspec])
    image_info = json.loads(out)
    labels = image_info["config"]["Labels"]
    return (labels.get("com.redhat.component"), labels.get("version"), labels.get("release"))


def get_image_pullspecs_from_release_payload(payload_pullspec: str, ignore={"machine-os-content"}) -> Iterable[str]:
    """ Retrieves pullspecs of images in a release payload.
    :param payload_pullspec: release payload pullspec
    :param ignore: a set of image names that we want to exclude from the return value (e.g. machine-os-content)
    :return: an iterator over pullspecs of component images
    """
    out, _ = exectools.cmd_assert(["oc", "adm", "release", "info", "-o", "json", "--", payload_pullspec])
    payload_info = json.loads(out)
    for tag in payload_info["references"]["spec"]["tags"]:
        if tag["name"] in ignore:
            continue
        yield tag["from"]["name"]
