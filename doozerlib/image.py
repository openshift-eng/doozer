from __future__ import absolute_import, print_function, unicode_literals

import hashlib
import json
import random
from typing import Any, Dict, Optional, Tuple

from doozerlib import brew, exectools, util
from doozerlib.distgit import pull_image
from doozerlib.metadata import Metadata, RebuildHint, RebuildHintCode
from doozerlib.model import Missing, Model
from doozerlib.pushd import Dir
from doozerlib import coverity


class ImageMetadata(Metadata):

    def __init__(self, runtime, data_obj: Dict, commitish: Optional[str] = None, clone_source: Optional[bool] = False, prevent_cloning: Optional[bool] = False):
        super(ImageMetadata, self).__init__('image', runtime, data_obj, commitish, prevent_cloning=prevent_cloning)
        self.image_name = self.config.name
        self.required = self.config.get('required', False)
        self.image_name_short = self.image_name.split('/')[-1]
        self.parent = None
        self.children = []  # list of ImageMetadata which use this image as a parent.
        dependents = self.config.get('dependents', [])
        for d in dependents:
            self.children.append(self.runtime.late_resolve_image(d, add=True))
        if clone_source:
            runtime.resolve_source(self)

    def is_ancestor(self, image):
        """
        :param image: A Metadata object or a distgit_key to check.
        :return: Returns whether the specified image is an ancestor of this image (eg. a parent, or parent's parent...)
        """
        if isinstance(image, Metadata):
            image = image.distgit_key

        parent = self.parent
        while parent:
            if parent.distgit_key == image:
                return True
            parent = parent.parent

        return False

    def get_descendants(self):
        """
        :return: Returns a set of children imagemetadata and their children, ..etc.
        """
        descendants = set()
        for child in self.children:
            descendants.add(child)
            descendants.update(child.get_descendants())

        return descendants

    def resolve_parent(self):
        """
        :return: Resolves and returns imagemeta.parent attribute for this image's parent image OR None if no parent is defined.
        """
        if 'from' in self.config:
            if 'member' in self.config['from']:
                base = self.config['from']['member']
                try:
                    self.parent = self.runtime.resolve_image(base)
                except:
                    self.parent = None

                if self.parent:
                    self.parent.add_child(self)
        return self.parent

    def add_child(self, child):
        """
        Adds a child imagemetadata to this list of children for this image.
        :param child:
        :return:
        """
        self.children.append(child)

    @property
    def base_only(self):
        """
        Some images are marked base-only.  Return the flag from the config file
        if present.
        """
        return self.config.base_only

    @property
    def is_payload(self):
        return self.config.get('for_payload', False)

    @property
    def for_release(self):
        return self.config.get('for_release', True)

    def get_brew_image_name_short(self):
        # Get image name in the Brew pullspec. e.g. openshift3/ose-ansible --> openshift3-ose-ansible
        return self.image_name.replace("/", "-")

    def pull_url(self):
        # Don't trust what is the Dockerfile for version & release. This field may not even be present.
        # Query brew to find the most recently built release for this component version.
        _, version, release = self.get_latest_build_info()

        # we need to pull images from proxy if 'brew_image_namespace' is enabled:
        # https://source.redhat.com/groups/public/container-build-system/container_build_system_wiki/pulling_pre_quay_switch_over_osbs_built_container_images_using_the_osbs_registry_proxy
        if self.runtime.group_config.urls.brew_image_namespace is not Missing:
            name = self.runtime.group_config.urls.brew_image_namespace + '/' + self.config.name.replace('/', '-')
        else:
            name = self.config.name

        return "{host}/{name}:{version}-{release}".format(
            host=self.runtime.group_config.urls.brew_image_host, name=name, version=version,
            release=release)

    def pull_image(self):
        pull_image(self.pull_url())

    def get_default_push_tags(self, version, release):
        push_tags = [
            "%s-%s" % (version, release),  # e.g. "v3.7.0-0.114.0.0"
            "%s" % version,  # e.g. "v3.7.0"
        ]

        # it's possible but rare that an image will have an alternate
        # tags along with the regular ones
        # append those to the tag list.
        if self.config.push.additional_tags is not Missing:
            push_tags.extend(self.config.push.additional_tags)

        # In v3.7, we use the last .0 in the release as a bump field to differentiate
        # image refreshes. Strip this off since OCP will have no knowledge of it when reaching
        # out for its node image.
        if "." in release:
            # Strip off the last field; "0.114.0.0" -> "0.114.0"
            push_tags.append("%s-%s" % (version, release.rsplit(".", 1)[0]))

        # Push as v3.X; "v3.7.0" -> "v3.7"
        push_tags.append("%s" % (version.rsplit(".", 1)[0]))
        return push_tags

    def get_default_repos(self):
        """
        :return: Returns a list of ['ns/repo', 'ns/repo'] found in the image config yaml specified for default pushes.
        """
        # Repos default to just the name of the image (e.g. 'openshift3/node')
        default_repos = [self.config.name]

        # Unless overridden in the config.yml
        if self.config.push.repos is not Missing:
            default_repos = self.config.push.repos.primitive()

        return default_repos

    def get_default_push_names(self):
        """
        :return: Returns a list of push names that should be pushed to for registries defined in
        group.yml and for additional repos defined in image config yaml.
        (e.g. ['registry/ns/repo', 'registry/ns/repo', ...]).
        """

        # Will be built to include a list of 'registry/ns/repo'
        push_names = []

        default_repos = self.get_default_repos()  # Get a list of [ ns/repo, ns/repo, ...]

        default_registries = []
        if self.runtime.group_config.push.registries is not Missing:
            default_registries = self.runtime.group_config.push.registries.primitive()

        for registry in default_registries:
            registry = registry.rstrip("/")   # Remove any trailing slash to avoid mistaking it for a namespace
            for repo in default_repos:
                namespace, repo_name = repo.split('/')
                if '/' in registry:  # If registry overrides namespace
                    registry, namespace = registry.split('/')
                push_names.append('{}/{}/{}'.format(registry, namespace, repo_name))

        # image config can contain fully qualified image names to push to (registry/ns/repo)
        if self.config.push.also is not Missing:
            push_names.extend(self.config.push.also)

        return push_names

    def get_additional_push_names(self, additional_registries):
        """
        :return: Returns a list of push names based on a list of additional registries that
        need to be pushed to (e.g. ['registry/ns/repo', 'registry/ns/repo', ...]).
        """

        if not additional_registries:
            return []

        # Will be built to include a list of 'registry/ns/repo'
        push_names = []

        default_repos = self.get_default_repos()  # Get a list of [ ns/repo, ns/repo, ...]

        for registry in additional_registries:
            registry = registry.rstrip("/")   # Remove any trailing slash to avoid mistaking it for a namespace
            for repo in default_repos:
                namespace, repo_name = repo.split('/')
                if '/' in registry:  # If registry overrides namespace
                    registry, namespace = registry.split('/')
                push_names.append('{}/{}/{}'.format(registry, namespace, repo_name))

        return push_names

    # Class methods to speed up computation of does_image_need_change if multiple
    # images will be assessed.
    # Mapping of brew pullspec => most recent brew build dict.
    builder_image_builds = dict()

    def does_image_need_change(self, changing_rpm_packages=[], buildroot_tag=None, newest_image_event_ts=None, oldest_image_event_ts=None) -> Tuple[Metadata, RebuildHint]:
        """
        Answers the question of whether the latest built image needs to be rebuilt based on
        the packages (and therefore RPMs) it is dependent on might have changed in tags
        relevant to the image. A check is also made if the image depends on a package
        we know is changing because we are about to rebuild it.
        :param changing_rpm_packages: A list of package names that are about to change.
        :param buildroot_tag: The build root for this image
        :param newest_image_event_ts: The build timestamp of the most recently built image in this group.
        :param oldest_image_event_ts: The build timestamp of the oldest build in this group from getLatestBuild of each component.
        :return: (meta, RebuildHint).
        """

        dgk = self.distgit_key
        runtime = self.runtime

        builds_contained_in_archives = {}  # build_id => result of koji.getBuild(build_id)
        with runtime.pooled_koji_client_session() as koji_api:

            image_build = self.get_latest_build(default='')
            if not image_build:
                # Seems this have never been built. Mark it as needing change.
                return self, RebuildHint(RebuildHintCode.NO_LATEST_BUILD, 'Image has never been built before')

            self.logger.debug(f'Image {dgk} latest is {image_build}')

            image_nvr = image_build['nvr']
            image_build_event_id = image_build['creation_event_id']  # the brew event that created this build

            self.logger.info(f'Running a change assessment on {image_nvr} built at event {image_build_event_id}')

            # Very rarely, an image might need to pull a package that is not actually installed in the
            # builder image or in the final image.
            # e.g. https://github.com/openshift/ironic-ipa-downloader/blob/999c80f17472d5dbbd4775d901e1be026b239652/Dockerfile.ocp#L11-L14
            # This is programmatically undetectable through koji queries. So we allow extra scan-sources hints to
            # be placed in the image metadata.
            if self.config.scan_sources.extra_packages is not Missing:
                for package_details in self.config.scan_sources.extra_packages:
                    extra_package_name = package_details.name
                    extra_package_brew_tag = package_details.tag
                    # Example of queryHistory: https://gist.github.com/jupierce/943b845c07defe784522fd9fd76f4ab0
                    extra_latest_tagging_infos = koji_api.queryHistory(table='tag_listing', tag=extra_package_brew_tag, package=extra_package_name, active=True)['tag_listing']

                    if extra_latest_tagging_infos:
                        extra_latest_tagging_infos.sort(key=lambda event: event['create_event'])
                        # We have information about the most recent time this package was tagged into the
                        # relevant tag. Why the tagging event and not the build time? Well, the build could have been
                        # made long ago, but only tagged into the relevant tag recently.
                        extra_latest_tagging_event = extra_latest_tagging_infos[-1]['create_event']
                        self.logger.debug(f'Checking image creation time against extra_packages {extra_package_name} in tag {extra_package_brew_tag} @ tagging event {extra_latest_tagging_event}')
                        if extra_latest_tagging_event > image_build_event_id:
                            return self, RebuildHint(RebuildHintCode.PACKAGE_CHANGE, f'Image {dgk} is sensitive to extra_packages {extra_package_name} which changed at event {extra_latest_tagging_event}')
                    else:
                        self.logger.warning(f'{dgk} unable to find tagging event for for extra_packages {extra_package_name} in tag {extra_package_brew_tag} ; Possible metadata error.')

            # Collect build times from any parent/builder images used to create this image
            builders = list(self.config['from'].builder) or []
            builders.append(self.config['from'])  # Add the parent image to the builders
            for builder in builders:
                if builder.member:
                    # We can't determine if images are about to change. Defer to scan-sources.
                    continue

                if builder.image:
                    builder_image_name = builder.image
                elif builder.stream:
                    builder_image_name = runtime.resolve_stream(builder.stream).image
                else:
                    raise IOError(f'Unable to determine builder or parent image pullspec from {builder}')

                if "." in builder_image_name.split('/', 2)[0]:
                    # looks like full pullspec with domain name; e.g. "registry.redhat.io/ubi8/nodejs-12:1-45"
                    builder_image_url = builder_image_name
                else:
                    # Assume this is a org/repo name relative to brew; e.g. "openshift/ose-base:ubi8"
                    builder_image_url = self.runtime.resolve_brew_image_url(builder_image_name)

                builder_brew_build = ImageMetadata.builder_image_builds.get(builder_image_url, None)

                if not builder_brew_build:
                    out, err = exectools.cmd_assert(f'oc image info {builder_image_url} --filter-by-os amd64 -o=json', retries=5, pollrate=10)
                    latest_builder_image_info = Model(json.loads(out))
                    builder_info_labels = latest_builder_image_info.config.config.Labels
                    builder_nvr_list = [builder_info_labels['com.redhat.component'], builder_info_labels['version'], builder_info_labels['release']]

                    if not all(builder_nvr_list):
                        raise IOError(f'Unable to find nvr in {builder_info_labels}')

                    builder_image_nvr = '-'.join(builder_nvr_list)
                    builder_brew_build = koji_api.getBuild(builder_image_nvr)
                    ImageMetadata.builder_image_builds[builder_image_url] = builder_brew_build
                    self.logger.debug(f'Found that builder or parent image {builder_image_url} has event {builder_brew_build}')

                if image_build_event_id < builder_brew_build['creation_event_id']:
                    self.logger.info(f'will be rebuilt because a builder or parent image changed: {builder_image_name}')
                    return self, RebuildHint(RebuildHintCode.BUILDER_CHANGING, f'A builder or parent image {builder_image_name} has changed since {image_nvr} was built')

            build_root_change = brew.has_tag_changed_since_build(runtime, koji_api, image_build, buildroot_tag, inherit=True)
            if build_root_change:
                self.logger.info(f'Image will be rebuilt due to buildroot change since {image_nvr} (last build event={image_build_event_id}). Build root change: [{build_root_change}]')
                return self, RebuildHint(RebuildHintCode.BUILD_ROOT_CHANGING, f'Buildroot tag changes since {image_nvr} was built')

            archives = koji_api.listArchives(image_build['build_id'])

            # Compare to the arches in runtime
            build_arches = set()
            for a in archives:
                # When running with cachito, not all archives returned are images. Filter out non-images.
                if a['btype'] == 'image':
                    build_arches.add(a['extra']['image']['arch'])

            target_arches = set(self.get_arches())
            if target_arches != build_arches:
                # The latest brew build does not exactly match the required arches as specified in group.yml
                return self, RebuildHint(RebuildHintCode.ARCHES_CHANGE, f'Arches of {image_nvr}: ({build_arches}) does not match target arches {target_arches}')

            for archive in archives:
                # Example results of listing RPMs in an given imageID:
                # https://gist.github.com/jupierce/a8798858104dcf6dfa4bd1d6dd99d2d8
                archive_id = archive['id']
                rpm_entries = koji_api.listRPMs(imageID=archive_id)
                for rpm_entry in rpm_entries:
                    build_id = rpm_entry['build_id']
                    build = koji_api.getBuild(build_id, brew.KojiWrapperOpts(caching=True))
                    package_name = build['package_name']
                    if package_name in changing_rpm_packages:
                        return self, RebuildHint(RebuildHintCode.PACKAGE_CHANGE, f'Image includes {package_name} which is also about to change')
                    # Several RPMs may belong to the same package, and each archive must use the same
                    # build of a package, so all we need to collect is the set of build_ids for the packages
                    # across all of the archives.
                    builds_contained_in_archives[build_id] = build

        self.logger.info(f'Checking whether any of the installed builds {len(builds_contained_in_archives)} has been tagged by a relevant tag since this image\'s build brew event {image_build_event_id}')

        installed_builds = list(builds_contained_in_archives.values())
        # Shuffle the builds before starting the threads. The reason is that multiple images are going to be performing
        # these queries simultaneously. Those images have similar packages (typically rooting in a rhel base image).
        # The KojiWrapper caching mechanism will allow two simultaneous calls to a Koji API to hit the actual
        # server since no result has yet been returned. Shuffling the installed package list spreads the threads
        # out among the packages to reduce re-work by the server.
        random.shuffle(installed_builds)
        changes_res = runtime.parallel_exec(
            f=lambda installed_package_build, terminate_event: is_image_older_than_package_build_tagging(self, image_build_event_id, installed_package_build, newest_image_event_ts, oldest_image_event_ts),
            args=installed_builds,
            n_threads=10
        )

        for rebuild_hint in changes_res.get():
            if rebuild_hint.rebuild:
                return self, rebuild_hint

        return self, RebuildHint(RebuildHintCode.BUILD_IS_UP_TO_DATE, 'No change detected')

    def covscan(self, cc: coverity.CoverityContext) -> bool:
        self.logger.info('Setting up for coverity scan')
        dgr = self.distgit_repo()
        with Dir(dgr.distgit_dir):
            if coverity.run_covscan(cc):
                cc.mark_results_done()
                return True
            else:
                self.logger.error('Error computing coverity results for this image')
                return False

    def calculate_config_digest(self, group_config, streams):
        image_config: Dict[str, Any] = self.config.primitive()  # primitive() should create a shallow clone for the underlying dict
        group_config: Dict[str, Any] = group_config.primitive()
        streams: Dict[str, Any] = streams.primitive()
        if "owners" in image_config:
            del image_config["owners"]  # ignore the owners entry for the digest: https://issues.redhat.com/browse/ART-1080
        message = {
            "config": image_config,
        }

        repos = set(image_config.get("enabled_repos", []) + image_config.get("non_shipping_repos", []))
        if repos:
            message["repos"] = {repo: group_config["repos"][repo] for repo in repos}

        builders = image_config.get("from", {}).get("builder", [])
        from_stream = image_config.get("from", {}).get("stream")
        referred_streams = {builder.get("stream") for builder in builders if builder.get("stream")}
        if from_stream:
            referred_streams.add(from_stream)
        if referred_streams:
            message["streams"] = {stream: streams[stream] for stream in referred_streams}

        # Avoid non serializable objects. Known to occur for PosixPath objects in content.source.modifications.
        default = lambda o: f"<<non-serializable: {type(o).__qualname__}>>"

        digest = hashlib.sha256(json.dumps(message, sort_keys=True, default=default).encode("utf-8")).hexdigest()
        return "sha256:" + digest

    def default_brew_target(self):
        if self.runtime.hotfix:
            target = f"{self.branch()}-containers-hotfix"
        else:
            target = f"{self.branch()}-containers-candidate"
        return target


def is_image_older_than_package_build_tagging(image_meta, image_build_event_id, package_build, newest_image_event_ts, oldest_image_event_ts) -> RebuildHint:
    """
    Determines if a given rpm is part of a package that has been tagged in a relevant tag AFTER image_build_event_id
    :param image_meta: The image meta object for the image.
    :param image_build_event_id: The build event for the image.
    :param package_build: The package build to assess.
    :param newest_image_event_ts: The build timestamp of the most recently built image in this group.
    :param oldest_image_event_ts: The build timestamp of the oldest build in this group from getLatestBuild of each component.
    :return: A rebuild hint with information on whether a rebuild should be performed and why
    """

    # If you are considering changing this code, you are going to have to contend with
    # complex scenarios like: what if we pulled in this RPM by tagging it, intentionally
    # backlevel, from another product, and we want to keep like that? Or, what if we tagged
    # in a non-released version of another product to get a fix pre-release of that package,
    # but subsequently want to inherit later versions the original product ships.
    # This blunt approach isn't trying to be perfect, but it will rarely do an unnecessary
    # rebuild and handles those complex scenarios by erring on the side of doing the rebuild.

    runtime = image_meta.runtime

    with runtime.pooled_koji_client_session() as koji_api:
        package_build_id = package_build['build_id']
        package_name = package_build['package_name']

        # At the time this NEWEST image in the group was built, what were the active tags by which our image may have
        # pulled in the build in question.
        # We could make this simple by looking for  beforeEvent=image_build_event_id , but this would require
        # a new, relatively large koji api for each image. By using  newest_image_event_ts,
        # we have a single, cached call which can be used for all subsequent analysis of this package.
        possible_active_tag_events = koji_api.queryHistory(brew.KojiWrapperOpts(caching=True),
                                                           table='tag_listing', build=package_build_id,
                                                           active=True, before=newest_image_event_ts)['tag_listing']

        # Now filter down the list to just the tags which might have contributed to our image build.
        contemporary_active_tag_names = set()
        for event in possible_active_tag_events:
            tag_name = event['tag.name']
            # There are some tags that are guaranteed not to be the way our image found the package.
            # Exclude them from the list of relevant tags.
            if tag_name == 'trashcan' or '-private' in tag_name or 'deleted' in tag_name:
                continue
            if tag_name.endswith(('-released', '-set', '-pending', '-backup')):
                # Ignore errata tags (e.g. RHBA-2020:2309-released, RHBA-2020:3027-pending) and tags like rhel-8.0.0-z-batch-0.3-set
                continue
            if tag_name.endswith(('-candidate', '-build')):
                # Eliminate candidate tags (we will add this image's -candidate tag back in below)
                continue
            # Finally, see if the event happened after THIS image was created
            if event['create_event'] < image_build_event_id:
                contemporary_active_tag_names.add(tag_name)

        image_meta.logger.info(f'Checking for tagging changes for {package_name} which old build may have received through: {contemporary_active_tag_names}')

        # Given an RPM X with a history of tags {x}, we know we received & installed
        # the RPM through one of the tags in {x}. What happens if a new RPM Y had a set of tags {y} that
        # is fully disjoint from {x}. In short, it came in through a completely independent vector, but we
        # would still find it through that vector if a build was triggered.
        # This could happen if:
        # 1. group.yml repos are changed and Y would be pulled from a new source
        # 2. The new Y is available through a different repo than we found it in last time.
        # To mitigate #1, we should force build after changing group.yml repos.
        # For #2, the only way this would typically happen would be if we were pulling from an official
        # repo for rpm X and then Y was tagged into our candidate tag as an override. To account for
        # this, always check for changes in our tags.
        contemporary_active_tag_names.add(image_meta.branch())
        contemporary_active_tag_names.add(image_meta.candidate_brew_tag())

        # Now let's look for tags that were applied to this package AFTER the oldest image in the group.
        # We could make this simple by looking for  afterEvent=image_build_event_id , but this would require
        # a new, relatively large koji api for each image. By looking all the way back to  eldest_image_event_ts,
        # we have a single, cached call which can be used for all subsequent analysis of this package.
        active_tag_events = koji_api.queryHistory(brew.KojiWrapperOpts(caching=True),
                                                  table='tag_listing', package=package_name,
                                                  active=True, after=oldest_image_event_ts)['tag_listing']

        subsequent_active_tag_names = set()
        for event in active_tag_events:
            # See if the event happened after THIS image was created
            if event['create_event'] > image_build_event_id:
                subsequent_active_tag_names.add(event['tag.name'])

        image_meta.logger.info(f'Checking for tagging changes for {package_name} where tags have been modified since build: {subsequent_active_tag_names}')

        # Here's the magic, hopefully. If the tags when the image was built and subsequent tag names intersect,
        # we know that a tag that may have delivered a build into our image, has subsequently been updated to point to
        # a different build of that package. This means, if we build again, we MIGHT pull in that newly
        # tagged package.
        intersection_set = subsequent_active_tag_names.intersection(contemporary_active_tag_names)

        if intersection_set:
            return RebuildHint(RebuildHintCode.PACKAGE_CHANGE, f'Package {package_name} has been retagged by potentially relevant tags since image build: {intersection_set}')

    return RebuildHint(RebuildHintCode.BUILD_IS_UP_TO_DATE, 'No package change detected')
