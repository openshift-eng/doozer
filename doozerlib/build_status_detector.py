from logging import Logger
from multiprocessing import Lock
from typing import Dict, List, Optional, Union, Set

from koji import ClientSession

from doozerlib import brew


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

    def find_embargoed_builds(self, builds: List[Dict]) -> Set[int]:
        """ find embargoes in given list of koji builds
        :param builds: a list of koji build dicts returned by the koji api
        :return: a set of build IDs that have embargoed fixes
        """
        # first, exclude all shipped builds
        self.logger and self.logger.info("Filtering out shipped builds...")
        shipped = self.find_shipped_builds([b["id"] for b in builds])
        suspects = [b for b in builds if b["id"] not in shipped]

        # second, if a build's release field includes .p1, it is embargoed
        embargoed = {b["id"] for b in suspects if ".p1" in b["release"]}

        # finally, look at the rpms in .p0 images in case they include unshipped .p1 rpms
        suspect_build_ids = {b["id"] for b in suspects if b["id"] not in embargoed}  # non .p1 build IDs

        # look up any build IDs not already in self.archive_lists cache
        build_ids = list(suspect_build_ids - self.archive_lists.keys())
        if build_ids:
            self.logger and self.logger.info(f"Fetching image archives for {len(build_ids)} builds...")
            archive_lists = brew.list_archives_by_builds(build_ids, "image", self.koji_session)  # if a build is not an image (e.g. rpm), Brew will return an empty archive list for that build
            for build_id, archive_list in zip(build_ids, archive_lists):
                self.archive_lists[build_id] = archive_list  # save to cache

        # look for embargoed RPMs in the image archives (one per arch for every image)
        for suspect in suspect_build_ids:
            for archive in self.archive_lists[suspect]:
                rpms = archive["rpms"]
                suspected_rpms = [rpm for rpm in rpms if ".p1" in rpm["release"]]  # there should be a better way to check the release field...
                shipped = self.find_shipped_builds([rpm["build_id"] for rpm in suspected_rpms])
                embargoed_rpms = [rpm for rpm in suspected_rpms if rpm["build_id"] not in shipped]
                if embargoed_rpms:
                    image_build_id = archive["build_id"]
                    embargoed.add(image_build_id)

        return embargoed

    def find_shipped_builds(self, build_ids: Set[Union[int, str]]):
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

    unshipped_candidate_rpms_cache = {}
    cache_lock = Lock()

    def find_unshipped_candidate_rpms(self, candidate_tag, event=None):
        """ find latest RPMs in the candidate tag that have not been shipped yet.

        <lmeyer> i debated whether to consider builds unshipped if not shipped
        in the same OCP version (IOW the base tag), and ultimately decided we're
        not concerned if an image is using something already shipped elsewhere,
        just if it's not using what we're trying to ship new.

        :param candidate_tag: string tag name to search for candidate builds
        :return: a list of brew RPMs from those builds (not the builds themselves)
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
