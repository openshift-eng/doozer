from typing import List, Dict, Optional, Set, Tuple
import asyncio
import click
import hashlib
import json
import sys
from urllib import request
from doozerlib.cli import cli, click_coroutine
from doozerlib.model import Model
from doozerlib import constants, util, exectools, logutil
from doozerlib.rhcos import RHCOSBuildInspector

logger = logutil.getLogger(__name__)


@cli.command("get-nightlies", short_help="Determine set(s) of accepted nightlies with matching contents for all architectures")
@click.option("--matching", metavar="NIGHTLY_NAME", multiple=True, help="Only report nightlies with the same content as named nightly")
@click.option("--allow-pending", is_flag=True, help="Include nightlies that have not completed tests")
@click.option("--allow-rejected", is_flag=True, help="Include nightlies that have failed tests")
@click.option("--exclude-arch", metavar="ARCH", multiple=True, help="Exclude arch(es) normally included in this version (multi,aarch64,...)")
@click.option("--limit", default=1, metavar='NUM', help="Number of sets of nightlies to print")
@click.option("--rhcos", is_flag=True, help="Print rhcos build id with nightlies")
@click.option("--latest", is_flag=True, help="Just get the latest nightlies for all arches (accepted or not)")
@click.pass_obj
@click_coroutine
async def get_nightlies(runtime, matching: List[str], exclude_arch: List[str], allow_pending: bool, allow_rejected: bool, limit: str, rhcos: bool, latest: bool):
    """
    Find one or more sets of nightlies (including all arches) that have
    matching content according to source commits and NVRs (or in the case of
    RHCOS containers, RPM content).

    By default:

    * only one set of nightlies (the most recent) is displayed (see --limit)

    * only accepted nightlies will be examined (see --allow-pending / --allow-rejected)

    * all arches configured for the group will be included (see --exclude-arch)

    You may also specify a desired nightly or nightlies (see --matching), in
    which case those will be the only nightlies examined in their respective
    arches.

    If results do not include a nightly that you expect to see, check the
    doozer debug.log where details about equivalence failures are logged.
    Matching is performed in two phases:

    * The first uses only info from the nightly release images for quick
      comparison in order to construct candidate equivalent sets of nightlies.

    * The second retrieves image info for all payload content in order to
      compare group image NVRs and RHCOS RPM content.
    """
    # parameter validation/processing
    limit = int(limit)
    if latest and limit > 1:
        raise ValueError("Don't use --latest and --limit > 1")
    if limit < 1:
        raise ValueError("--limit must be a positive integer")
    if latest:
        allow_pending = True
        allow_rejected = True
    runtime.initialize(clone_distgits=False)
    include_arches: Set[str] = determine_arch_list(runtime, set(exclude_arch))

    # make lists of nightly objects per arch
    try:
        nightlies_for_arch: Dict[str, List[Dict]] = find_rc_nightlies(runtime, include_arches, allow_pending, allow_rejected, matching)
    except NoMatchingNightlyException as ex:
        util.red_print(ex)
        exit(1)

    # retrieve release info for each nightly image (with concurrency)
    await asyncio.gather(*[
        populate_nightly_release_data(nightly)
        for arch, nightlies in nightlies_for_arch.items()
        for nightly in nightlies
    ])

    # find sets of nightlies where all arches have equivalent content
    eq_sets = []
    for eq_set in generate_equivalence_sets(nightlies_for_arch):
        # check for deeper equivalence
        await eq_set.populate_deeper_equivalence(runtime)
        if eq_set.deeper_equivalence():
            eq_sets.append(eq_set)
            util.green_print(eq_set)
            if len(eq_sets) >= limit:
                break  # don't spend time looking for more than were requested

    if not eq_sets:
        util.red_print("No sets of equivalent nightlies found for given parameters.")
        exit(1)


def determine_arch_list(runtime, exclude_arches: Set[str]) -> Set[str]:
    """
    Validate exclude_arches and return group-configured arches without them
    """
    available_arches: Set[str] = set(runtime.group_config.arches or [])
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


def find_rc_nightlies(runtime, arches: Set[str], allow_pending: bool, allow_rejected: bool, matching: Optional[List[str]] = []) -> Dict[str, List[Dict]]:
    """
    Find all current nightlies for each arch, in order RC gives them (most
    recent to oldest). Filter to Accepted unless allow_rejected is true.
    ref.  https://amd64.ocp.releases.ci.openshift.org/api/v1/releasestream/4.12.0-0.nightly/tags
    Each nightly looks like:
    {
      "name": "4.12.0-0.nightly-2022-07-15-132344",
      "phase": "Ready",
      "pullSpec": "registry.ci.openshift.org/ocp/release:4.12.0-0.nightly-2022-07-15-132344",
      "downloadURL": "https://openshift-release-artifacts.apps.ci.l2s4.p1.openshiftapps.com/4.12.0-0.nightly-2022-07-15-132344"
    }
    """
    found_matching: Dict[str, bool] = {name: False for name in matching}
    nightlies_for_arch: Dict[str, List[Dict]] = {}
    allowed_phases = {"Accepted"}
    if allow_pending:
        allowed_phases.add("Ready")
    if allow_rejected:
        allowed_phases.add("Rejected")

    tag_base: str = f"{runtime.group_config.vars.MAJOR}.{runtime.group_config.vars.MINOR}.0-0.nightly"
    for arch in arches:
        # retrieve the list of nightlies from the release-controller
        rc_url: str = f"{rc_api_url(tag_base, arch)}/tags"
        logger.info(f"Reading nightlies from {rc_url}")
        with request.urlopen(rc_url) as req:
            data = json.loads(req.read().decode())

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
            raise EmptyArchException(f"No nightlies for {tag_base} {arch} are in state {allowed_phases}")
        nightlies_for_arch[arch] = nightlies

    # make sure we found every match we expected
    unmatched: Set[str] = {name for name, found in found_matching.items() if not found}
    if matching and unmatched:
        raise NoMatchingNightlyException(f"Found no nightlies in state {allowed_phases} matching {unmatched}")

    return nightlies_for_arch


def rc_api_url(tag, arch):
    """
    base url for a release tag in release controller.

    @param tag  The RC release tag as a string (e.g. "4.9.0-0.nightly")
    @param arch  architecture we are interested in (e.g. "s390x")
    @return e.g. "https://s390x.ocp.releases.ci.openshift.org/api/v1/releasestream/4.9.0-0.nightly-s390x"
    """
    arch = util.go_arch_for_brew_arch(arch)
    arch_suffix = util.go_suffix_for_arch(arch)
    return f"{constants.RC_BASE_URL.format(arch=arch)}/api/v1/releasestream/{tag}{arch_suffix}"


async def populate_nightly_release_data(nightly: Dict[str, str]):
    """
    update the nightly record with new fields from the release image:
    releaseInfo: output from `oc adm release info -o json` for the nightly pullspec
    equivalence: an EquivalenceBase object for comparing to other nightlies
    """
    release_json_str, _ = await exectools.cmd_assert_async(f"oc adm release info {nightly['pullSpec']} -o=json", retries=3)
    info = nightly["releaseInfo"] = json.loads(release_json_str)
    nightly["equivalence"] = EquivalenceBase(info)


# only look up the same container info once and store it here
image_info_cache: Dict[str, Dict] = {}


class EquivalenceBase:
    """
    Class to enable an initial comparison of two nightlies, just checking that
    they have the same source commits. RHCOS containers have no source commit
    annotation so are compared based on image labels (only after other
    comparisons succeed).

    If the initial comparison succeeds, then a deeper comparison can be
    performed using image info from pullspecs and RHCOS build records.
    """

    def __init__(self, release_info: Dict):
        self.release_info = release_info
        self.commit_for_tag = {}
        self.pullspec_for_tag = {}
        self.rhcos_tag_names = set()
        self.rhcos_tag_digests = {}
        self.nvr_for_tag = {}
        self.rhcos_inspector = None  # filled by populate_deeper_equivalence
        for tag in release_info["references"]["spec"]["tags"]:
            name = tag["name"]
            commit = tag["annotations"]["io.openshift.build.commit.id"]
            self.pullspec_for_tag[name] = tag["from"]["name"]
            self.commit_for_tag[name] = commit or None
            if not commit:  # assume RHCOS
                self.rhcos_tag_names.add(name)

        self.tag_names = set(self.commit_for_tag.keys())

        # remove entries that are just stand-ins
        pod_commit = self.commit_for_tag["pod"]  # NOTE: hardcoded required "stand-in" member
        self.commit_for_tag = {
            tag: commit
            for tag, commit in self.commit_for_tag.items()
            if tag == "pod" or commit != pod_commit
        }

    def __eq__(self, other):
        """
        Determine whether self and other are superficially equivalent.
        We do not check that all of the same tags are present in both, because
        it turns out there are situations where differences between arches in
        the tag set are normal.
        We do check that if both have a tag, they have the same source commit
        (for group images) or the same set of RPM labels (for RHCOS images).
        """

        for tag, commit in self.commit_for_tag.items():
            other_commit = other.commit_for_tag.get(tag)
            if commit and other_commit and commit != other_commit:
                logger.debug(f"different because {tag}@{commit} != {tag}@{other_commit}")
                return False
            # ^^ missing entries automatically match

        for tag in self.rhcos_tag_names:
            if self.retrieve_rhcos_tag_digest(tag) != other.retrieve_rhcos_tag_digest(tag):
                logger.debug("different because rhcos digests don't match")
                return False

        return True

    def retrieve_rhcos_tag_digest(self, tag):
        """
        From the RHCOS container info extract distingushing characteristics and
        generate a digest of them.

        For now the characteristics are just labels for RPMs in the container.
        """
        if tag in self.rhcos_tag_digests:
            logger.debug(f"digest {self.rhcos_tag_digests[tag]} for {tag}")
            return self.rhcos_tag_digests[tag]

        labels = {
            key: val.rsplit(".", 1)[0]  # remove arch suffix from rpm
            for key, val in self.retrieve_image_info(self.pullspec_for_tag[tag]).config.config.Labels.items()
            if key.startswith("com.coreos.rpm.") and "kernel-rt" not in key
        }
        self.rhcos_tag_digests[tag] = hashlib.sha256(json.dumps(labels, sort_keys=True).encode("utf-8")).hexdigest()
        logger.debug(f"digest {self.rhcos_tag_digests[tag]} for {labels} from {tag}")
        return self.rhcos_tag_digests[tag]

    def __repr__(self):
        return f"{self.commit_for_tag}/{self.rhcos_tag_digests}"

    def retrieve_image_info(self, pullspec):
        """pull and cache json info for a container pullspec"""
        if pullspec not in image_info_cache:
            image_json_str, _ = exectools.cmd_assert(
                f"oc image info {pullspec} -o=json --filter-by-os=amd64",
                retries=3
            )
            image_info_cache[pullspec] = Model(json.loads(image_json_str))
        return image_info_cache[pullspec]

    async def retrieve_image_info_async(self, pullspec):
        """pull and cache json info for a container pullspec (enable concurrency)"""
        if pullspec not in image_info_cache:
            image_json_str, _ = await exectools.cmd_assert_async(
                f"oc image info {pullspec} -o=json --filter-by-os=amd64",
                retries=3
            )
            image_info_cache[pullspec] = Model(json.loads(image_json_str))
        return image_info_cache[pullspec]

    async def populate_deeper_equivalence(self, runtime, arch):
        """Retrieve image NVRs and RHCOS build data concurrently for deeper comparison"""
        await asyncio.gather(*(
            self.retrieve_image_info_async(self.pullspec_for_tag[tag])
            for tag in self.commit_for_tag
        ))
        if not self.rhcos_inspector:
            ps4tag = {tag: self.pullspec_for_tag[tag] for tag in self.rhcos_tag_names}
            self.rhcos_inspector = RHCOSBuildInspector(runtime, ps4tag, arch)

    def retrieve_nvr_for_tag(self, tag: str):
        """Retrieve group image NVR according to the image info at the tag pullspec"""
        if tag not in self.nvr_for_tag:
            labels = self.retrieve_image_info(self.pullspec_for_tag[tag]).config.config.Labels
            labels = tuple(labels[k] for k in ("com.redhat.component", "version", "release"))
            self.nvr_for_tag[tag] = labels if all(labels) else None
        return self.nvr_for_tag[tag]

    def deeper_equivalence(self, other):
        """
        Do a deeper (more expensive) equivalence check that requires
        populate_deeper_equivalence having been called beforehand. This checks
        that group images NVRs and RHCOS RPM content are the same for both
        nightlies.
        """
        # check that group images in the release all have the same nvr
        for tag, commit in self.commit_for_tag.items():
            other_commit = other.commit_for_tag.get(tag)
            if not commit or not other_commit:
                continue  # ignore missing or non-group entries
            self_nvr = self.retrieve_nvr_for_tag(tag)
            other_nvr = other.retrieve_nvr_for_tag(tag)
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

        return self.deeper_equivalence_rhcos(other)

    def deeper_equivalence_rhcos(self, other):
        """Check that the two have the same RHCOS contents according to build records"""
        for eq_base in (self, other):
            if not eq_base.rhcos_inspector:
                raise Exception(f"No rhcos_inspector for eq_base {eq_base}, should have called populate_deeper_equivalence first")
            eq_base._rhcos_rpms = {
                nevra[0]: (nevra[2], nevra[3])
                for nevra in eq_base.rhcos_inspector.get_os_metadata_rpm_list()
            }

        logger.debug(f"comparing {self.rhcos_inspector} and {other.rhcos_inspector}")
        for rpm_name, vr in self._rhcos_rpms.items():
            if rpm_name in other._rhcos_rpms and vr != other._rhcos_rpms[rpm_name]:
                logger.debug(f"different '{rpm_name}' version-release {vr} != {other._rhcos_rpms[rpm_name]}")
                return False

        return True


class EqSetDuplicateArchException(Exception):
    """Indicates a bug where code tried to add a nightly with an arch that's already represented in the eq set"""


class EquivalenceSet:
    """
    Class to represent a set of nightlies from each arch that are equivalent
    according to their EquivalenceBase. They can be sorted by creation date
    (latest timestamp of nightlies in the set)
    """

    def __init__(self, nightly_for_arch: Dict[str, Dict]):
        self.nightly_for_arch = nightly_for_arch
        self.timestamp = max(nightly["releaseInfo"]["config"]["created"] for nightly in nightly_for_arch.values())

    def augment(self, arch: str, nightly: Dict):
        """
        If the nightly being added is equivalent to all existing members, spawn
        a new set that includes it and return that. Otherwise return None.
        """
        if arch in self.nightly_for_arch:
            raise EqSetDuplicateArchException(f"Cannot add arch {arch} when already present in {self}")

        nightly_for_arch = {arch: nightly}
        for arch, existing in self.nightly_for_arch.items():
            logger.debug(f"comparing {nightly['name']} and {existing['name']}")
            if nightly["equivalence"] != existing["equivalence"]:
                return None  # not equivalent
            nightly_for_arch[arch] = existing

        return EquivalenceSet(nightly_for_arch)

    def __repr__(self):
        return " ".join(nightly['name'] for nightly in self.nightly_for_arch.values())

    async def populate_deeper_equivalence(self, runtime):
        """Prepare EquivalenceBases for deeper (more expensive) comparison"""
        await asyncio.gather(*(
            nightly["equivalence"].populate_deeper_equivalence(runtime, arch)
            for arch, nightly in self.nightly_for_arch.items()
        ))

    def deeper_equivalence(self):
        """Check that all EquivalenceBases have deeper equivalency"""
        nightlies = list(self.nightly_for_arch.values())
        while len(nightlies) > 1:
            this = nightlies.pop()
            for other in nightlies:
                logger.debug(f"comparing {this['name']} and {other['name']}")
                if not this["equivalence"].deeper_equivalence(other["equivalence"]):
                    logger.debug(f"deeper equivalence failed for {self}")
                    return False

        return True


def generate_equivalence_sets(nightlies_for_arch: Dict[str, List[Dict]]) -> List[EquivalenceSet]:
    """
    Build all-arch sets of equivalent (according to EquivalenceBase.__eq__) nightlies.
    We initialize sets with the arch with the fewest nightlies, then extend
    them with all equivalent nightlies from one arch at a time.
    """
    eq_sets: List[EquivalenceSet] = []

    # process arches from shortest list to longest to maximize elimination
    for arch, nightlies in sorted(nightlies_for_arch.items(), key=lambda it: len(it[1])):
        if not eq_sets:
            # seed with the first arch
            eq_sets = [EquivalenceSet({arch: nightly}) for nightly in nightlies]
            continue
        # try to combine everything in this arch with existing sets
        new_sets: List[EquivalenceSet] = []
        for eq_set in eq_sets:
            for nightly in nightlies:
                new_set = eq_set.augment(arch, nightly)
                if new_set:
                    new_sets.append(new_set)

        if not new_sets:
            return new_sets  # no sets left to augment
        eq_sets = new_sets

    return sorted(eq_sets, reverse=True, key=lambda eq_set: eq_set.timestamp)
