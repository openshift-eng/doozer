import asyncio
import json
from typing import Dict, List, Sequence, Set, Tuple

import aiohttp
import click

from doozerlib import constants, exectools, logutil, util
from doozerlib.cli import cli, click_coroutine
from doozerlib.model import Model
from doozerlib.rhcos import RHCOSBuildInspector
from doozerlib.runtime import Runtime

logger = logutil.getLogger(__name__)


@cli.command("get-nightlies", short_help="Determine set(s) of accepted nightlies with matching contents for all architectures")
@click.option("--matching", metavar="NIGHTLY_NAME", multiple=True, help="Only report nightlies with the same content as named nightly")
@click.option("--allow-inconsistency", is_flag=True, help="Allow nightlies that fail deeper consistency checks")
@click.option("--allow-pending", is_flag=True, help="Include nightlies that have not completed tests")
@click.option("--allow-rejected", is_flag=True, help="Include nightlies that have failed tests")
@click.option("--exclude-arch", "exclude_arches", metavar="ARCH", multiple=True, help="Exclude arch(es) normally included in this version (multi,aarch64,...)")
@click.option("--limit", default=1, type=int, metavar='NUM', help="Number of sets of nightlies to print")
@click.option("--details", is_flag=True, help="Print some nightly details including RHCOS build id")
@click.option("--latest", is_flag=True, help="Just get the latest nightlies for all arches (accepted or not)")
@click.pass_obj
@click_coroutine
async def get_nightlies(runtime: Runtime, matching: Tuple[str, ...], exclude_arches: Tuple[str, ...],
                        allow_inconsistency: bool,
                        allow_pending: bool,
                        allow_rejected: bool, limit: int, details: bool, latest: bool):
    """
    Find set(s) including a nightly for each arch with matching contents
    according to source commits and NVRs (or in the case of RHCOS containers,
    RPM content).

    \b
    By default:
    * only one set of nightlies (the most recent) is displayed (see --limit)
    * only accepted nightlies will be examined (see --allow-pending/rejected)
    * only 100% consistent nightlies will be displayed (see --allow-inconsistency)
    * all arches configured for the group will be required (see --exclude-arch)

    You may also specify a desired nightly or nightlies (see --matching) to filter
    results to only sets that include the matched nightlies in their respective
    arch(es). Examples:

     \b
     $ doozer -q -g openshift-4.8 get-nightlies --limit 3
     4.8.0-0.nightly-s390x-2022-07-19-121001 4.8.0-0.nightly-ppc64le-2022-07-19-120922 4.8.0-0.nightly-2022-07-19-120845
     4.8.0-0.nightly-s390x-2022-07-19-001435 4.8.0-0.nightly-ppc64le-2022-07-19-001401 4.8.0-0.nightly-2022-07-19-001308
     4.8.0-0.nightly-s390x-2022-07-15-184648 4.8.0-0.nightly-ppc64le-2022-07-15-184608 4.8.0-0.nightly-2022-07-15-190253

     \b
     Match one preferred nightly:
     $ doozer -q -g openshift-4.8 get-nightlies --matching 4.8.0-0.nightly-2022-07-19-001308
     4.8.0-0.nightly-s390x-2022-07-19-001435 4.8.0-0.nightly-ppc64le-2022-07-19-001401 4.8.0-0.nightly-2022-07-19-001308
     \b
     Match nightlies in multiple arches:
     $ doozer -q -g openshift-4.8 get-nightlies --matching 4.8.0-0.nightly-2022-07-19-001308 --matching 4.8.0-0.nightly-ppc64le-2022-07-19-120922
     No sets of equivalent nightlies found for given parameters.
     \b
     Match two nightlies in same arch:
     $ doozer -q -g openshift-4.8 get-nightlies --limit 3 --matching 4.8.0-0.nightly-2022-07-19-001308 --matching 4.8.0-0.nightly-2022-07-15-190253
     4.8.0-0.nightly-s390x-2022-07-19-001435 4.8.0-0.nightly-ppc64le-2022-07-19-001401 4.8.0-0.nightly-2022-07-19-001308
     4.8.0-0.nightly-s390x-2022-07-15-184648 4.8.0-0.nightly-ppc64le-2022-07-15-184608 4.8.0-0.nightly-2022-07-15-190253

    All matches specified must exist (to guard against typo/mis-paste) with the correct state:

     \b
     $ doozer -q -g openshift-4.8 get-nightlies --matching 4.8.0-that-exists-not
     Found no nightlies in state {'Accepted'} matching {'4.8.0-that-exists-not'}

    If results do not include a nightly that you expect to see, check the
    doozer debug.log where details about equivalence failures are logged.
    Matching is performed in two phases:

      * The first uses only info from the nightly release images for quick
        comparison in order to construct candidate equivalent sets of nightlies.
      * The second retrieves image info for all payload content in order to
        compare group image NVRs and RHCOS RPM content.
    """
    # parameter validation/processing
    if latest and limit > 1:
        raise ValueError("Don't use --latest and --limit > 1")
    if limit < 1:
        raise ValueError("--limit must be a positive integer")
    if latest:
        allow_pending = True
        allow_rejected = True
    runtime.initialize(clone_distgits=False)
    include_arches: Set[str] = determine_arch_list(runtime, set(exclude_arches))

    # make lists of nightly objects per arch
    try:
        nightlies = await find_rc_nightlies(runtime, include_arches, allow_pending, allow_rejected, matching)
        nightlies_for_arch: Dict[str, List[Nightly]] = {
            arch: [Nightly(nightly_info=n) for n in nightlies]
            for arch, nightlies in nightlies.items()
        }
    except NoMatchingNightlyException as ex:
        util.red_print(ex)
        exit(1)

    # retrieve release info for each nightly image (with concurrency)
    await asyncio.gather(*[
        nightly.populate_nightly_release_data()
        for arch, nightlies in nightlies_for_arch.items()
        for nightly in nightlies
    ])

    # find sets of nightlies where all arches have equivalent content
    inconsistent_nightly_sets = []
    remaining = limit
    for nightly_set in generate_nightly_sets(nightlies_for_arch):
        # check for deeper equivalence
        await nightly_set.populate_nightly_content(runtime)
        if await nightly_set.deeper_equivalence():
            util.green_print(nightly_set.details() if details else nightly_set)
            remaining -= 1
            if not remaining:
                break  # don't spend time checking more than were requested
        else:
            inconsistent_nightly_sets.append(nightly_set)

    if allow_inconsistency and remaining:
        for nightly_set in inconsistent_nightly_sets[:remaining]:
            util.yellow_print(nightly_set.details() if details else nightly_set)
            remaining -= 1

    if remaining == limit:
        util.red_print("No sets of equivalent nightlies found for given parameters.")
        exit(1)


def determine_arch_list(runtime: Runtime, exclude_arches: Set[str]) -> Set[str]:
    """
    Validate exclude_arches and return group-configured arches without them
    """
    available_arches: Set[str] = set(runtime.arches)
    # TODO: managing multi requires an oc new enough to understand
    # manifest-listed release images, and possibly other complications - tackle
    # this when we get closer to releasing multi.
    #
    # if runtime.group_config.multi_arch.enabled:
    #     available_arches.add("multi")

    try:
        exclude_arches = set(util.brew_arch_for_go_arch(arch) for arch in exclude_arches)
    except Exception as ex:
        raise ValueError(f"invalid --exclude-arch: {ex}")

    for arch in exclude_arches:
        if arch not in available_arches:
            raise ValueError(f"arch {arch} is not among the available arches for this version: {available_arches}")

    return available_arches - exclude_arches


class NoMatchingNightlyException(Exception):
    """Indicates one or more nightlies that were requested to match were not found"""
    pass


class EmptyArchException(Exception):
    """Indicates there are no (accepted) nightlies for an arch"""
    pass


async def find_rc_nightlies(runtime: Runtime, arches: Set[str], allow_pending: bool, allow_rejected: bool, matching: Sequence[str] = []) -> Dict[str, List[Dict]]:
    """
    Retrieve current nightly dicts for each arch, in order RC gives them (most
    recent to oldest). Filter to Accepted unless allow_pending/rejected is true.
    ref. https://amd64.ocp.releases.ci.openshift.org/api/v1/releasestream/4.12.0-0.nightly/tags
    Each nightly dict looks like:
    {
      "name": "4.12.0-0.nightly-2022-07-15-132344",
      "phase": "Ready",
      "pullSpec": "registry.ci.openshift.org/ocp/release:4.12.0-0.nightly-2022-07-15-132344",
      "downloadURL": "https://openshift-release-artifacts.apps.ci.l2s4.p1.openshiftapps.com/4.12.0-0.nightly-2022-07-15-132344"
    }
    """

    async def _find_nightlies(_arch: str):
        # retrieve the list of nightlies from the release-controller
        rc_url: str = f"{rc_api_url(tag_base, _arch)}/tags"
        logger.info(f"Reading nightlies from {rc_url}")

        async with aiohttp.ClientSession() as session:
            async with session.get(rc_url) as resp:
                data = await resp.json()

        # filter them per parameters
        nightlies: List[Dict] = [
            nightly for nightly in (data.get("tags") or [])
            if nightly["phase"] in allowed_phases
        ]
        matched_nightlies: List[Dict] = [
            nightly for nightly in nightlies
            if nightly["name"] in matching
        ]
        if matched_nightlies:
            nightlies = matched_nightlies  # no need to look at others in this arch
            for nightly in matched_nightlies:
                found_matching[nightly["name"]] = True

        if not nightlies:
            # must be nightlies to succeed; user must adjust parameters
            raise EmptyArchException(f"No nightlies for {tag_base} {_arch} are in state {allowed_phases}")
        nightlies_for_arch[_arch] = nightlies

    matching = set(matching)
    found_matching: Dict[str, bool] = {name: False for name in matching}
    nightlies_for_arch: Dict[str, List[Dict]] = {}
    allowed_phases = {"Accepted"}
    if allow_pending:
        allowed_phases.add("Ready")
    if allow_rejected:
        allowed_phases.add("Rejected")

    tag_base: str = f"{runtime.group_config.vars.MAJOR}.{runtime.group_config.vars.MINOR}.0-0.nightly"
    await asyncio.gather(*(_find_nightlies(arch) for arch in arches))

    # make sure we found every match we expected
    unmatched: Set[str] = {name for name, found in found_matching.items() if not found}
    if matching and unmatched:
        raise NoMatchingNightlyException(f"Found no nightlies in state {allowed_phases} matching {unmatched}")

    return nightlies_for_arch


def rc_api_url(tag: str, arch: str) -> str:
    """
    base url for a release tag in release controller.

    @param tag  The RC release stream as a string (e.g. "4.9.0-0.nightly")
    @param arch  architecture we are interested in (e.g. "s390x")
    @return e.g. "https://s390x.ocp.releases.ci.openshift.org/api/v1/releasestream/4.9.0-0.nightly-s390x"
    """
    arch = util.go_arch_for_brew_arch(arch)
    arch_suffix = util.go_suffix_for_arch(arch)
    return f"{constants.RC_BASE_URL.format(arch=arch)}/api/v1/releasestream/{tag}{arch_suffix}"


# only look up the same container image info once and store it here
image_info_cache: Dict[str, Dict] = {}


class Nightly:
    """
    Class to enable the comparison of two nightlies. An initial comparison
    based on release image info checks that they have the same source commits.
    (RHCOS containers have no source commit annotation so are not compared.)

    When initial comparison succeeds, a deeper comparison can be performed
    using image info from pullspecs and RHCOS build records.
    """

    def __init__(
            self, nightly_info: Dict = None, release_image_info: Dict = None,
            name: str = None, phase: str = None, pullspec: str = None):

        self.nightly_info = nightly_info or {}
        self.release_image_info = release_image_info or {}

        # have to specify these one way or another or init will fail
        self.name = name or self.nightly_info["name"]
        self.phase = phase or self.nightly_info["phase"]
        self.pullspec = pullspec or self.nightly_info["pullSpec"]

        # filled by populate_nightly_release_data
        self.commit_for_tag = {}
        self.pullspec_for_tag = {}
        self.rhcos_tag_names = set()

        # filled by populate_nightly_content
        self.nvr_for_tag = {}
        self.rhcos_inspector = None

    async def populate_nightly_release_data(self):
        """
        retrieve release_image_info from output of `oc adm release info -o json` for the nightly pullspec.
        """
        release_json_str, _ = await exectools.cmd_assert_async(f"oc adm release info {self.pullspec} -o=json", retries=3)
        self.release_image_info = json.loads(release_json_str)
        self._process_nightly_release_data()

    def _process_nightly_release_data(self):
        """update the nightly attrs with new fields from the release image data"""
        for tag in self.release_image_info["references"]["spec"]["tags"]:
            name = tag["name"]
            commit = tag["annotations"]["io.openshift.build.commit.id"]
            self.pullspec_for_tag[name] = tag["from"]["name"]
            self.commit_for_tag[name] = commit or None
            if not commit:  # assume RHCOS
                self.rhcos_tag_names.add(name)

        self.tag_names = set(self.commit_for_tag.keys())

        # retain only entries that are not stand-ins.
        # NOTE: required stand-in member "pod" is hardcoded
        pod_commit = self.commit_for_tag["pod"]
        self.commit_for_tag = {
            tag: commit
            for tag, commit in self.commit_for_tag.items()
            if tag == "pod" or commit != pod_commit
        }

    def __eq__(self, other: 'Nightly'):
        """
        Determine whether self and other are source-commit equivalent.

        Does not require that all of the same tags are present in both, because
        it turns out there are situations where differences in the tag set
        between arches are normal.

        Does check that if both have a tag, they have the same source commit
        (for group images - RHCOS images have none).
        """

        for tag, commit in self.commit_for_tag.items():
            other_commit = other.commit_for_tag.get(tag)
            if commit and other_commit and commit != other_commit:
                logger.debug(f"different because {tag}@{commit} != {tag}@{other_commit}")
                return False
            # ^^ missing entries automatically match

        return True

    def __repr__(self):
        # helpful for failing tests/errors; not intended for users
        return f"{self.name}: {self.commit_for_tag}"

    @exectools.limit_concurrency(500)
    async def retrieve_image_info_async(self, pullspec: str) -> Model:
        """pull/cache/return json info for a container pullspec (enable concurrency)"""
        if pullspec not in image_info_cache:
            image_json_str, _ = await exectools.cmd_assert_async(
                f"oc image info {pullspec} -o=json --filter-by-os=amd64",
                retries=3
            )
            image_info_cache[pullspec] = Model(json.loads(image_json_str))
        return image_info_cache[pullspec]

    async def populate_nightly_content(self, runtime, arch: str):
        """Retrieve image NVRs and RHCOS build data concurrently for deeper comparison"""
        await asyncio.gather(*(
            self.retrieve_image_info_async(self.pullspec_for_tag[tag])
            for tag in self.commit_for_tag
        ))
        if not self.rhcos_inspector:
            ps4tag = {tag: self.pullspec_for_tag[tag] for tag in self.rhcos_tag_names}
            self.rhcos_inspector = RHCOSBuildInspector(runtime, ps4tag, arch)

    async def retrieve_nvr_for_tag(self, tag: str) -> str:
        """Retrieve group image NVR according to the image info at the tag pullspec"""
        if tag not in self.nvr_for_tag:
            data = await self.retrieve_image_info_async(self.pullspec_for_tag[tag])
            labels = data.config.config.Labels
            labels = tuple(labels[k] for k in ("com.redhat.component", "version", "release"))
            self.nvr_for_tag[tag] = labels if all(labels) else None
        return self.nvr_for_tag[tag]

    async def deeper_equivalence(self, other: 'Nightly') -> bool:
        """
        Do a deeper equivalence check that requires populate_nightly_content
        having been called beforehand. This checks that group images NVRs and
        RHCOS RPM content are the same for both nightlies.
        """
        # check that group images in the release all have the same nvr
        for tag, commit in self.commit_for_tag.items():
            other_commit = other.commit_for_tag.get(tag)
            if not commit or not other_commit:
                continue  # ignore missing or non-group entries
            self_nvr, other_nvr = await asyncio.gather(*(self.retrieve_nvr_for_tag(tag), other.retrieve_nvr_for_tag(tag)))
            if self_nvr != other_nvr:
                if self_nvr[0] != other_nvr[0]:
                    # give alt images (where components differ for the same tag) a pass.
                    # most of the time they'll have the same VR but that's not absolutely
                    # guaranteed (one build could flake then succeed with later R).
                    # so just rely on source commit equivalence (already verified)
                    # and ignore the slim possibility that the RPMs installed differ.
                    continue
                logger.debug(f"different because {tag} NVR {self_nvr} != {other_nvr}")
                return False

        return self.deeper_nightly_rhcos(other)

    def deeper_nightly_rhcos(self, other: 'Nightly') -> bool:
        """Check that the two have the same RHCOS contents according to build records"""
        for nightly in (self, other):
            if not nightly.rhcos_inspector:
                raise Exception(f"No rhcos_inspector for nightly {nightly}, should have called populate_nightly_content first")
            nightly._rhcos_rpms = {
                nevra[0]: (nevra[2], nevra[3])
                for nevra in nightly.rhcos_inspector.get_os_metadata_rpm_list()
            }

        logger.debug(f"comparing {self.rhcos_inspector} and {other.rhcos_inspector}")
        for rpm_name, vr in self._rhcos_rpms.items():
            if rpm_name in other._rhcos_rpms and vr != other._rhcos_rpms[rpm_name]:
                logger.debug(f"different '{rpm_name}' version-release {vr} != {other._rhcos_rpms[rpm_name]}")
                return False

        return True


class NightlySetDuplicateArchException(Exception):
    """Indicates a bug where code tried to add a nightly with an arch that's already represented in the nightly set"""


class NightlySet:
    """
    Class to represent a set of nightlies per arch that have equivalent
    content.
    """

    def __init__(self, nightly_for_arch: Dict[str, Nightly]):
        self.nightly_for_arch = nightly_for_arch
        self.timestamp = max(nightly.release_image_info["config"]["created"] for nightly in nightly_for_arch.values())

    def generate_equivalents_with(self, arch: str, nightlies: List[Nightly]) -> List['NightlySet']:
        """
        Test each nightly for equivalence with all existing set members, and
        return a list of new sets extended with those that match.

        Assuming nightlies are listed in order of age and are always updated to
        newer rather than rolled back to earlier images, the sets generated
        will always be in order of age as well. We can imagine special cases
        where we generate nightlies with rollbacks, but it seems safe to assume
        when we want to generate a release that there will be no shenanigans or
        that the user can use --limit or --matching to get what they expect.
        """
        if arch in self.nightly_for_arch:
            raise NightlySetDuplicateArchException(f"Cannot add arch {arch} when already present in {self}")

        new_sets: List[NightlySet] = []
        for nightly in nightlies:
            nightly_for_arch = {arch: nightly}  # initialize new set with candidate nightly
            for exarch, existing in self.nightly_for_arch.items():
                logger.debug(f"comparing {nightly.name} and {existing.name}")
                if nightly != existing:
                    break  # not equivalent
                nightly_for_arch[exarch] = existing

            if len(nightly_for_arch) > len(self.nightly_for_arch):  # all were added
                new_sets.append(NightlySet(nightly_for_arch))

        return new_sets

    def __repr__(self):
        return " ".join(nightly.name for nightly in self.nightly_for_arch.values())

    def details(self) -> str:
        return f"{self}\n" + "".join(
            f"{arch}: {nightly.name} {nightly.phase} rhcos: {nightly.release_image_info['displayVersions']['machine-os']['Version']}\n"
            for arch, nightly in self.nightly_for_arch.items()
        )

    async def populate_nightly_content(self, runtime):
        """Prepare Nightlys for deeper (more expensive) comparison"""
        await asyncio.gather(*(
            nightly.populate_nightly_content(runtime, arch)
            for arch, nightly in self.nightly_for_arch.items()
        ))

    async def deeper_equivalence(self) -> bool:
        """Check that all Nightlys have deeper equivalency"""
        nightlies = list(self.nightly_for_arch.values())
        while len(nightlies) > 1:
            this = nightlies.pop()
            for other in nightlies:
                logger.debug(f"comparing {this.name} and {other.name}")
                if not await this.deeper_equivalence(other):
                    logger.debug(f"deeper equivalence failed for {self}")
                    return False

        return True


def generate_nightly_sets(nightlies_for_arch: Dict[str, List[Nightly]]) -> List[NightlySet]:
    """
    Build all-arch sets of equivalent (according to Nightly.__eq__) nightlies.
    We initialize sets with the arch with the fewest nightlies, then extend
    them with all equivalent nightlies from one arch at a time.
    """
    nightly_sets: List[NightlySet] = []

    # process arches from shortest list to longest to maximize elimination
    for arch, nightlies in sorted(nightlies_for_arch.items(), key=lambda it: len(it[1])):
        if not nightly_sets:
            # seed with the first arch
            nightly_sets = [NightlySet({arch: nightly}) for nightly in nightlies]
            continue

        # try to combine nightlies in this arch with existing sets
        new_sets: List[NightlySet] = []
        for nightly_set in nightly_sets:
            new_sets.extend(nightly_set.generate_equivalents_with(arch, nightlies))
        if not new_sets:
            return []  # no sets left to extend
        nightly_sets = new_sets

    # sorted by timestamp; already expected to be ordered that way, but this ensures it
    return sorted(nightly_sets, reverse=True, key=lambda nightly_set: nightly_set.timestamp)
