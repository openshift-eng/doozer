from logging import Logger
from multiprocessing import Lock
from typing import Dict, List, Optional, Set, Iterable

from koji import ClientSession

from doozerlib import brew, util


class BuildStatusDetector:
    """ a BuildStatusDetector can find builds with embargoed fixes
    """
    def __init__(self, session: ClientSession, logger: Optional[Logger] = None):
        """ creates a new BuildStatusDetector
        :param session: a koji client session
        :param logger: a logger
        """
        self.koji_session = session
        self.logger = logger
        self.shipping_statuses: Dict[int, bool] = {}  # a dict for caching build shipping statues. key is build id, value is True if shipped.
        self.archive_lists: Dict[int, List[Dict]] = {}  # a dict for caching archive lists. key is build id, value is a list of archives associated with that build.

    def find_embargoed_builds(self, builds: List[Dict], candidate_tags: Iterable[str]) -> Set[int]:
        """ find embargoed builds in given list of koji builds
        :param builds: a list of koji build dicts returned by the koji api
        :param candidate_tags: a list of candidate tags for the images being examined
        :return: a set of build IDs that have embargoed fixes
        """
        # first, exclude all shipped builds from suspicion of embargo - by definition no longer secret
        self.logger and self.logger.info("Filtering out shipped builds...")
        shipped_ids = self.find_shipped_builds({b["id"] for b in builds})
        suspects = [b for b in builds if b["id"] not in shipped_ids]

        # next, consider remaining builds embargoed if the release field includes .p1
        embargoed_ids = {b["id"] for b in suspects if util.isolate_pflag_in_release(b["release"]) == "p1"}

        # finally, look at the remaining images in case they include embargoed rpms
        remaining_ids = {b["id"] for b in suspects if b["id"] not in embargoed_ids}
        embargoed_ids.update(self.find_with_embargoed_rpms(remaining_ids, candidate_tags))

        return embargoed_ids

    def find_shipped_builds(self, build_ids: Set[int]) -> Set[int]:
        """ find shipped builds in the given builds
        :param build_ids: a list of build IDs
        :return: a set of shipped build IDs
        """
        uncached = set(build_ids) - self.shipping_statuses.keys()
        if uncached:
            uncached = list(uncached)
            self.logger and self.logger.info(f'Getting tags for {len(uncached)} builds...')
            tag_lists = brew.get_builds_tags(uncached, self.koji_session)
            for index, tags in enumerate(tag_lists):
                build_id = uncached[index]
                # a shipped build should have a Brew tag ending with `-released`, like `RHBA-2020:2713-released`
                shipped = any(map(lambda tag: tag["name"].endswith("-released"), tags))
                self.shipping_statuses[build_id] = shipped  # save to cache
        result = set(filter(lambda build_id: self.shipping_statuses[build_id], build_ids))
        return result

    def find_with_embargoed_rpms(self, suspect_build_ids: Set[int], candidate_tags: Iterable[str]) -> Set[int]:
        """ look for embargoed RPMs in the image archives (one per arch for every image)
        :param suspect_build_ids: a list of koji build ids
        :param candidate_tags: a list of candidate tags for the images being examined
        :return: a set of build IDs that contain embargoed RPM contents
        """
        self.populate_archive_lists(suspect_build_ids)

        embargoed_rpm_ids = set()
        for tag in candidate_tags:
            embargoed_rpm_ids.update(self.rpms_in_embargoed_tag(tag))

        embargoed_image_ids = set()
        for suspect in suspect_build_ids:
            for archive in self.archive_lists[suspect]:
                rpms = archive["rpms"]
                suspected_rpms = [
                    rpm for rpm in rpms
                    if util.isolate_pflag_in_release(rpm["release"]) == "p1"
                    or rpm["build_id"] in embargoed_rpm_ids
                ]
                shipped = self.find_shipped_builds([rpm["build_id"] for rpm in suspected_rpms])
                embargoed_rpms = [rpm for rpm in suspected_rpms if rpm["build_id"] not in shipped]
                if embargoed_rpms:
                    image_build_id = archive["build_id"]
                    embargoed_image_ids.add(image_build_id)
                    break  # once marked embargoed, no point in checking other arches

        return embargoed_image_ids

    def populate_archive_lists(self, suspect_build_ids: Set[int]):
        """ populate self.archive_lists with any build IDs not already cached
        :param suspect_build_ids: a list of koji build ids
        """
        build_ids = list(suspect_build_ids - self.archive_lists.keys())
        if build_ids:
            self.logger and self.logger.info(f"Fetching image archives for {len(build_ids)} builds...")
            archive_lists = brew.list_archives_by_builds(build_ids, "image", self.koji_session)  # if a build is not an image (e.g. rpm), Brew will return an empty archive list for that build
            for build_id, archive_list in zip(build_ids, archive_lists):
                self.archive_lists[build_id] = archive_list  # save to cache

    embargoed_rpms_cache = {}  # define cache field to be used in method

    def rpms_in_embargoed_tag(self, candidate_tag: List[str]) -> Set[int]:
        """ find a list of RPMs in an -embargoed tag.
        these are builds tagged in from an external source, e.g. kernel.
        :param candidate_tag: string tag name that contains candidate builds
        :return: a list of brew RPMs from builds in the corresponding embargoed tag
        """
        embargoed_tag = candidate_tag.replace('-candidate', '-embargoed')
        key = embargoed_tag
        with self.cache_lock:
            if key not in self.embargoed_rpms_cache:
                # note that we want all builds in the tag, not just the latest
                embargoed_rpms = self.koji_session.listTagged(embargoed_tag, event=None, type="rpm")
                self.embargoed_rpms_cache[key] = {r["id"] for r in embargoed_rpms}

        return self.embargoed_rpms_cache[key]

    cache_lock = Lock()
    unshipped_candidate_rpms_cache = {}

    def find_unshipped_candidate_rpms(self, candidate_tag, event=None):
        """ find latest RPMs in the candidate tag that have not been shipped yet.

        <lmeyer> i debated whether to consider builds unshipped if not shipped
        in the same OCP version (IOW the base tag), and ultimately decided we're
        not concerned if an image is using something already shipped elsewhere,
        just if it's not using what we're trying to ship new.

        :param candidate_tag: string tag name to search for candidate builds
        :return: a list of brew RPMs (the contents of the builds) from unshipped latest builds
        """
        key = (candidate_tag, event)
        with self.cache_lock:
            if key not in self.unshipped_candidate_rpms_cache:
                latest = self.koji_session.getLatestBuilds(candidate_tag, event=event, type="rpm")
                shipped_ids = self.find_shipped_builds([b["id"] for b in latest])
                unshipped_build_ids = [build["id"] for build in latest if build["id"] not in shipped_ids]
                rpms_lists = brew.list_build_rpms(unshipped_build_ids, self.koji_session)
                self.unshipped_candidate_rpms_cache[key] = [r for rpms in rpms_lists for r in rpms]

        return self.unshipped_candidate_rpms_cache[key]
