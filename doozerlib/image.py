
import asyncio
import hashlib
import json
import random
from typing import Any, Awaitable, Dict, List, Optional, Set, Tuple, Union

import doozerlib
from doozerlib import brew, coverity, exectools
from doozerlib.distgit import pull_image
from doozerlib.metadata import Metadata, RebuildHint, RebuildHintCode
from doozerlib.model import Missing, Model
from doozerlib.pushd import Dir
from doozerlib.rpm_utils import compare_nvr, parse_nvr
from doozerlib.util import (brew_arch_for_go_arch, go_arch_for_brew_arch,
                            isolate_el_version_in_release)


class ImageMetadata(Metadata):

    def __init__(self, runtime: "doozerlib.Runtime", data_obj: Dict, commitish: Optional[str] = None, clone_source: Optional[bool] = False, prevent_cloning: Optional[bool] = False):
        super(ImageMetadata, self).__init__('image', runtime, data_obj, commitish, prevent_cloning=prevent_cloning)
        self.image_name = self.config.name
        self.required = self.config.get('required', False)
        self.image_name_short = self.image_name.split('/')[-1]
        self.parent = None
        self.children = []  # list of ImageMetadata which use this image as a parent.
        self.dependencies: Set[str] = set()
        dependents = self.config.get('dependents', [])
        for d in dependents:
            dependent = self.runtime.late_resolve_image(d, add=True, required=False)
            if not dependent:
                continue
            dependent.dependencies.add(self.distgit_key)
            self.children.append(dependent)
        if clone_source:
            runtime.resolve_source(self)

    def get_assembly_rpm_package_dependencies(self, el_ver: int) -> Tuple[Dict[str, str], Dict[str, str]]:
        """
        An assembly can define RPMs which should be installed into a given member
        image. Those dependencies can be specified at either the individual member
        (higher priority) or at the group level. This method computes a
        Dict[package_name] -> nvr for any package that should be installed due to
        these overrides.
        :param el_ver: Which version of RHEL to check
        :return: Returns a tuple with two Dict[package_name] -> nvr for any assembly induced overrides.
                 The first entry in the Tuple are dependencies directly specified in the member.
                 The second entry are dependencies specified in the group and member.
        """
        direct_member_deps: Dict[str, str] = dict()  # Map package_name -> nvr
        aggregate_deps: Dict[str, str] = dict()  # Map package_name -> nvr
        eltag = f'el{el_ver}'
        group_deps = self.runtime.get_group_config().dependencies.rpms or []
        member_deps = self.config.dependencies.rpms or []

        for rpm_entry in group_deps:
            if eltag in rpm_entry:  # This entry has something for the requested RHEL version
                nvr = rpm_entry[eltag]
                package_name = parse_nvr(nvr)['name']
                aggregate_deps[package_name] = nvr

        # Perform the same process, but only for dependencies directly listed for the member
        for rpm_entry in member_deps:
            if eltag in rpm_entry:  # This entry has something for the requested RHEL version
                nvr = rpm_entry[eltag]
                package_name = parse_nvr(nvr)['name']
                direct_member_deps[package_name] = nvr
                aggregate_deps[package_name] = nvr  # Override anything at the group level

        return direct_member_deps, aggregate_deps

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
        val = self.config.get('for_payload', False)
        if val and self.base_only:
            raise ValueError(f'{self.distgit_key} claims to be for_payload and base_only')
        return val

    @property
    def for_release(self):
        val = self.config.get('for_release', True)
        if val and self.base_only:
            raise ValueError(f'{self.distgit_key} claims to be for_release and base_only')
        return val

    def get_payload_tag_info(self) -> Tuple[str, bool]:
        """
        If this image is destined for the OpenShift release payload, it will have a tag name
        associated within it within the release payload's originating imagestream.
        :return: Returns the tag name to use for this image in an
                OpenShift release imagestream and whether the payload name was explicitly included in the
                image metadata. See https://issues.redhat.com/browse/ART-2823 . i.e. (tag_name, explicitly_declared)
        """
        if not self.is_payload:
            raise ValueError('Attempted to derive payload name for non-payload image: ' + self.distgit_key)

        payload_name = self.config.get("payload_name")
        if payload_name:
            return payload_name, True
        else:
            payload_name = self.image_name_short[4:] if self.image_name_short.startswith("ose-") else self.image_name_short   # it _should_ but... to be safe
            return payload_name, False

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

    def does_image_need_change(self, changing_rpm_packages=None, buildroot_tag=None, newest_image_event_ts=None, oldest_image_event_ts=None) -> Tuple[Metadata, RebuildHint]:
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

        if not changing_rpm_packages:
            changing_rpm_packages = []

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

            # Build up a map of RPMs built by this group. It is the 'latest' builds of these RPMs
            # relative to the current assembly that matter in the forthcoming search -- not
            # necessarily the true-latest. i.e. if we are checking for changes in a 'stream' image,
            # we do not want to react because of an RPM build in the 'test' assembly.
            group_rpm_builds_nvrs: Dict[str, str] = dict()  # Maps package name to latest brew build nvr
            for rpm_meta in self.runtime.rpm_metas():
                latest_rpm_build = rpm_meta.get_latest_build(default=None)
                if latest_rpm_build:
                    group_rpm_builds_nvrs[rpm_meta.get_package_name()] = latest_rpm_build['nvr']

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

                    latest_assembly_build_nvr = group_rpm_builds_nvrs.get(package_name, None)
                    if latest_assembly_build_nvr and latest_assembly_build_nvr == build['nvr']:
                        # The latest RPM build for this assembly is already installed and we know the RPM
                        # is not about to change. Ignore the installed package.
                        self.logger.debug(f'Found latest assembly specific build ({latest_assembly_build_nvr}) for group package {package_name} is already installed in {dgk} archive; no tagging change search will occur')
                        continue

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

    def _default_brew_target(self):
        """ Returns derived brew target name from the distgit branch name
        """
        return f"{self.branch()}-containers-candidate"


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


class ArchiveImageInspector:
    """
    Represents and returns information about an archive image associated with a brew build.
    """

    def __init__(self, runtime: "doozerlib.Runtime", archive: Dict, brew_build_inspector: 'BrewBuildImageInspector' = None):
        """
        :param runtime: The brew build inspector associated with the build that created this archive.
        :param archive: The raw archive dict from brew.
        :param brew_build_inspector: If the BrewBuildImageInspector is known, pass it in.
        """
        self.runtime = runtime
        self._archive = archive
        self._cache = {}
        self.brew_build_inspector = brew_build_inspector

        if self.brew_build_inspector:
            assert (self.brew_build_inspector.get_brew_build_id() == self.get_brew_build_id())

    def image_arch(self) -> str:
        """
        Returns the CPU architecture (brew build nomenclature) for which this archive was created.
        """
        return self._archive['extra']['image']['arch']

    def get_archive_id(self) -> int:
        """
        :return: Returns this archive's unique brew ID.
        """
        return self._archive['id']

    def get_image_envs(self) -> Dict[str, Optional[str]]:
        """
        :return: Returns a dictionary of environment variables set for this image.
        """
        env_list: List[str] = self._archive['extra']['docker']['config']['config']['Env']
        envs_dict: Dict[str, Optional[str]] = dict()
        for env_entry in env_list:
            if '=' in env_entry:
                components = env_entry.split('=', 1)
                envs_dict[components[0]] = components[1]
            else:
                # Something odd about entry, so include it by don't try to parse
                envs_dict[env_entry] = None
        return envs_dict

    def get_image_labels(self) -> Dict[str, str]:
        """
        :return: Returns a dictionary of labels set for this image.
        """
        return dict(self._archive['extra']['docker']['config']['config']['Labels'])

    def get_archive_dict(self) -> Dict:
        """
        :return: Returns the raw brew archive object associated with this object.
                 listArchives output: https://gist.github.com/jupierce/a28a53e4057b550b3c8e5d6a8ac5198c. This method returns a single entry.
        """
        return self._archive

    def get_brew_build_id(self) -> int:
        """
        :return: Returns the brew build id for the build which created this archive.
        """
        return self._archive['build_id']

    def get_brew_build_inspector(self):
        """
        :return: Returns a brew build inspector for the build which created this archive.
        """
        if not self.brew_build_inspector:
            self.brew_build_inspector = BrewBuildImageInspector(self.runtime, self.get_brew_build_id())
        return self.brew_build_inspector

    def get_image_meta(self) -> ImageMetadata:
        """
        :return: Returns the imagemeta associated with this archive's component if there is one. None if no imagemeta represents it.
        """
        return self.get_brew_build_inspector().get_image_meta()

    async def find_non_latest_rpms(self) -> List[Tuple[str, str, str]]:
        """
        If the packages installed in this image archive overlap packages in the configured YUM repositories,
        return NVRs of the latest avaiable rpms that are not also installed in this archive.
        This indicates that the image has not picked up the latest from configured repos.

        Note that this is completely normal for non-STREAM assemblies. In fact, it is
        normal for any assembly other than the assembly used for nightlies. In the age of
        a thorough config:scan-sources, if this method returns anything, scan-sources is
        likely broken.

        :return: Returns a list of (installed_rpm, latest_rpm, repo_name)
        """
        # Get available rpms in configured repos
        group_repos = self.runtime.repos
        meta = self.get_image_meta()
        assert meta is not None
        arch = self.image_arch()
        enabled_repos = sorted({r.name for r in group_repos.values() if r.enabled} | set(meta.config.get("enabled_repos", [])))
        if not enabled_repos:
            self.runtime.logger.warning("Skipping non-latest rpms check for image %s because it doesn't have enabled_repos configured.", meta.distgit_key)
            return []
        tasks = []
        for repo_name in enabled_repos:
            repo = group_repos[repo_name]
            if arch in repo.arches:
                tasks.append(repo.list_rpms(arch))
        # Find the newest rpms for each rpm name among those configured repos
        candidate_rpms = {}
        candidate_rpm_repos = {}
        for repo, rpms in zip(enabled_repos, await asyncio.gather(*tasks)):
            for rpm in rpms:
                name = rpm["name"]
                if name not in candidate_rpms or compare_nvr(rpm, candidate_rpms[name]) > 0:
                    candidate_rpms[name] = rpm
                    candidate_rpm_repos[name] = repo

        # Compare installed rpms to candidate rpms found from configured repos
        archive_rpms = {rpm['name']: rpm for rpm in self.get_installed_rpm_dicts()}
        results: List[Tuple[str, str, str]] = []
        for name, archive_rpm in archive_rpms.items():
            candidate_rpm = candidate_rpms.get(name)
            if not candidate_rpm:
                continue
            # Well, in theory epoch=None (missing epoch) is less than epoch='0'.
            # However, yum converts epoch=None to epoch='0' internally when comparing EVRs.
            # We have to follow yum's tradition.
            if archive_rpm.get('epoch') is None:
                archive_rpm['epoch'] = '0'
            # For each RPM installed in the image, we want it to match what is in the configured repos
            # UNLESS the installed NVR is newer than what is in the configured repos.
            if compare_nvr(archive_rpm, candidate_rpm) < 0:
                results.append((archive_rpm['nvr'], candidate_rpm['nvr'], candidate_rpm_repos[name]))
        return results

    def get_installed_rpm_dicts(self) -> List[Dict]:
        """
        :return: Returns listRPMs for this archive
        (e.g. https://gist.github.com/jupierce/a8798858104dcf6dfa4bd1d6dd99d2d8)
        IMPORTANT: these are different from brew
        build records; use get_installed_build_dicts for those.
        """
        cn = 'get_installed_rpms'
        if cn not in self._cache:
            with self.runtime.pooled_koji_client_session() as koji_api:
                rpm_entries = koji_api.listRPMs(brew.KojiWrapperOpts(caching=True), imageID=self.get_archive_id())
                self._cache[cn] = rpm_entries
        return self._cache[cn]

    def get_installed_package_build_dicts(self) -> Dict[str, Dict]:
        """
        :return: Returns a Dict containing records for package builds corresponding to
                 RPMs used by this archive. A single package can build multiple RPMs.
                 Dict[package_name] -> raw brew build dict for package.
        """
        cn = 'get_installed_package_build_dicts'

        if cn not in self._cache:
            aggregate: Dict[str, Dict] = dict()
            with self.runtime.pooled_koji_client_session() as koji_api:
                # Example results of listing RPMs in an given imageID:
                # https://gist.github.com/jupierce/a8798858104dcf6dfa4bd1d6dd99d2d8
                for rpm_entry in self.get_installed_rpm_dicts():
                    rpm_build_id = rpm_entry['build_id']
                    # Now retrieve records for the package build which created this RPM. Turn caching
                    # on as the archives (and other images being analyzed) likely reference the
                    # exact same builds.
                    package_build = koji_api.getBuild(rpm_build_id, brew.KojiWrapperOpts(caching=True))
                    package_name = package_build['package_name']
                    aggregate[package_name] = package_build

            self._cache[cn] = aggregate

        return self._cache[cn]

    def get_archive_pullspec(self):
        """
        :return: Returns an internal pullspec for a specific archive image.
                 e.g. 'registry-proxy.engineering.redhat.com/rh-osbs/openshift-ose-openshift-controller-manager:rhaos-4.6-rhel-8-containers-candidate-53809-20210722091236-x86_64'
        """
        return self._archive['extra']['docker']['repositories'][0]

    def get_archive_digest(self):
        """
        :return Returns the archive image's sha digest (e.g. 'sha256:1f3ebef02669eca018dbfd2c5a65575a21e4920ebe6a5328029a5000127aaa4b')
        """
        digest = self._archive["extra"]['docker']['digests']['application/vnd.docker.distribution.manifest.v2+json']
        if not digest.startswith("sha256:"):  # It should start with sha256: for now. Let's raise an error if this changes.
            raise ValueError(f"Received unrecognized digest {digest} for archive {self.get_archive_id()}")
        return digest


class BrewBuildImageInspector:
    """
    Provides an API for common queries we perform against brew built images.
    """

    def __init__(self, runtime: "doozerlib.Runtime", pullspec_or_build_id_or_nvr: Union[str, int]):
        """
        :param runtime: The koji client session to use.
        :param pullspec_or_build_id_or_nvr: A pullspec to the brew image (it is fine if this is a manifest list OR a single archive image), a brew build id, or an NVR.
        """
        self.runtime = runtime

        with self.runtime.pooled_koji_client_session() as koji_api:
            self._nvr: Optional[str] = None  # Will be resolved to the NVR for the image/manifest list
            self._brew_build_obj: Optional[Dict] = None  # Will be populated with a brew build dict for the NVR

            self._cache = dict()

            if '/' not in str(pullspec_or_build_id_or_nvr):
                # Treat the parameter as an NVR or build_id.
                self._brew_build_obj = koji_api.getBuild(pullspec_or_build_id_or_nvr, strict=True)
                self._nvr = self._brew_build_obj['nvr']
            else:
                # Treat as a full pullspec
                self._build_pullspec = pullspec_or_build_id_or_nvr  # This will be reset to the official brew pullspec, but use it for now
                image_info = self.get_image_info()  # We need to find the brew build, so extract image info
                image_labels = image_info['config']['config']['Labels']
                self._nvr = image_labels['com.redhat.component'] + '-' + image_labels['version'] + '-' + image_labels['release']
                self._brew_build_obj = koji_api.getBuild(self._nvr, strict=True)

            self._build_pullspec = self._brew_build_obj['extra']['image']['index']['pull'][0]
            self._brew_build_id = self._brew_build_obj['id']

    def get_manifest_list_digest(self) -> str:
        """
        :return: Returns  'sha256:....' for the manifest list associated with this brew build.
        """
        return self._brew_build_obj['extra']['image']['index']['digests']['application/vnd.docker.distribution.manifest.list.v2+json']

    def get_brew_build_id(self) -> int:
        """
        :return: Returns the koji build id for this image.
        """
        return self._brew_build_id

    def get_brew_build_webpage_url(self):
        """
        :return: Returns a link for humans to go look at details for this brew build.
        """
        return f'https://brewweb.engineering.redhat.com/brew/buildinfo?buildID={self._brew_build_id}'

    def get_brew_build_dict(self) -> Dict:
        """
        :return: Returns the koji getBuild dictionary for this iamge.
        """
        return self._brew_build_obj

    def get_nvr(self) -> str:
        return self._nvr

    def __str__(self):
        return f'BrewBuild:{self.get_brew_build_id()}:{self.get_nvr()}'

    def __repr__(self):
        return f'BrewBuild:{self.get_brew_build_id()}:{self.get_nvr()}'

    def get_image_info(self, arch='amd64') -> Dict:
        """
        :return Returns the parsed output of oc image info for the specified arch.
        """
        go_arch = go_arch_for_brew_arch(arch)  # Ensure it is a go arch
        cn = f'get_image_info-{go_arch}'
        if cn not in self._cache:
            image_raw_str, _ = exectools.cmd_assert(f'oc image info --filter-by-os={go_arch} -o=json {self._build_pullspec}', retries=3)
            self._cache[cn] = json.loads(image_raw_str)
        return self._cache[cn]

    def get_labels(self, arch='amd64') -> Dict[str, str]:
        """
        :return: Returns a dictionary of labels associated with the image. If the image is a manifest list,
                 these will be the amd64 labels.
        """
        return self.get_image_archive_inspector(arch).get_image_labels()

    def get_envs(self, arch='amd64') -> Dict[str, str]:
        """
        :param arch: The image architecture to check.
        :return: Returns a dictionary of environment variables set for the image.
        """
        return self.get_image_archive_inspector(arch).get_image_envs()

    def get_component_name(self) -> str:
        return self.get_labels()['com.redhat.component']

    def get_package_name(self) -> str:
        return self.get_component_name()

    def get_image_meta(self) -> Optional[ImageMetadata]:
        """
        :return: Returns the ImageMetadata object associated with this component. Returns None if the component is not in ocp-build-data.
        """
        return self.runtime.component_map.get(self.get_component_name(), None)

    def get_version(self) -> str:
        """
        :return: Returns the 'version' field of this image's NVR.
        """
        return self._brew_build_obj['version']

    def get_release(self) -> str:
        """
        :return: Returns the 'release' field of this image's NVR.
        """
        return self._brew_build_obj['release']

    def get_rhel_base_version(self) -> Optional[int]:
        """
        Determines whether this image is based on RHEL 8, 9, ... May return None if no
        RPMS are installed (e.g. FROM scratch)
        """
        # OS metadata has changed a bit over time (i.e. there may be newer/cleaner ways
        # to determine this), but one thing that seems backwards compatible
        # is finding 'el' information in RPM list.
        for brew_dict in self.get_all_installed_rpm_dicts():
            nvr = brew_dict['nvr']
            el_ver = isolate_el_version_in_release(nvr)
            if el_ver:
                return el_ver

        raise None

    def get_source_git_url(self) -> Optional[str]:
        """
        :return: Returns SOURCE_GIT_URL from the image environment. This is a URL for the
                public source a customer should be able to find the source of the component.
                If the component does not have a ART-style SOURCE_GIT_URL, None is returned.
        """
        return self.get_envs().get('SOURCE_GIT_URL', None)

    def get_source_git_commit(self) -> Optional[str]:
        """
        :return: Returns SOURCE_GIT_COMMIT from the image environment. This is a URL for the
                public source a customer should be able to find the source of the component.
                If the component does not have a ART-style SOURCE_GIT_COMMIT, None is returned.
        """
        return self.get_envs().get('SOURCE_GIT_COMMIT', None)

    def get_arch_archives(self) -> Dict[str, ArchiveImageInspector]:
        """
        :return: Returns a map of architectures -> brew archive Dict  within this brew build.
        """
        return {a.image_arch(): a for a in self.get_image_archive_inspectors()}

    def get_build_pullspec(self) -> str:
        """
        :return: Returns an internal pullspec for the overall build. Usually this would be a manifest list with architecture specific archives.
                    To get achive pullspecs, use get_archive_pullspec.
        """
        return self._build_pullspec

    def get_all_archive_dicts(self) -> List[Dict]:
        """
        :return: Returns all archives associated with the build. This includes entries for
        things like cachito.
        Example listArchives output: https://gist.github.com/jupierce/a28a53e4057b550b3c8e5d6a8ac5198c
        """
        cn = 'get_all_archives'  # cache entry name
        if cn not in self._cache:
            with self.runtime.pooled_koji_client_session() as koji_api:
                self._cache[cn] = koji_api.listArchives(self._brew_build_id)
        return self._cache[cn]

    def get_image_archive_dicts(self) -> List[Dict]:
        """
        :return: Returns only raw brew archives for images within the build.
        """
        return list(filter(lambda a: a['btype'] == 'image', self.get_all_archive_dicts()))

    def get_image_archive_inspectors(self) -> List[ArchiveImageInspector]:
        """
        Example listArchives output: https://gist.github.com/jupierce/a28a53e4057b550b3c8e5d6a8ac5198c
        :return: Returns only image archives from the build.
        """
        cn = 'get_image_archives'
        if cn not in self._cache:
            image_archive_dicts = self.get_image_archive_dicts()
            inspectors = [ArchiveImageInspector(self.runtime, archive, brew_build_inspector=self) for archive in image_archive_dicts]
            self._cache[cn] = inspectors

        return self._cache[cn]

    def get_rpms_in_pkg_build(self, build_id: int) -> List[Dict]:
        """
        :return: Returns a list of brew RPM records from a single package build.
        """
        with self.runtime.pooled_koji_client_session() as koji_api:
            return koji_api.listRPMs(buildID=build_id)

    def get_all_installed_rpm_dicts(self) -> List[Dict]:
        """
        :return: Returns an aggregate set of all brew rpm definitions
        for RPMs installed on ALL architectures
        of this image build. IMPORTANT: these are different from brew
        build records; use get_installed_build_dicts for those.
        """
        cn = 'get_all_installed_rpm_dicts'
        if cn not in self._cache:
            dedupe: Dict[str, Dict] = dict()  # Maps nvr to rpm definition. This is because most archives will have similar RPMS installed.
            for archive_inspector in self.get_image_archive_inspectors():
                for rpm_dict in archive_inspector.get_installed_rpm_dicts():
                    dedupe[rpm_dict['nvr']] = rpm_dict
            self._cache[cn] = list(dedupe.values())
        return self._cache[cn]

    def get_all_installed_package_build_dicts(self) -> Dict[str, Dict]:
        """
        :return: Returns a Dict[package name -> brew build dict] for all
        packages installed on ANY architecture of this image build.
        (OSBS enforces that all image archives install the same package NVR
        if they install it at all.)
        """
        cn = 'get_all_installed_package_build_dicts'
        if cn not in self._cache:
            dedupe: Dict[str, Dict] = dict()  # Maps nvr to build dict. This is because most archives will have the similar packages installed.
            for archive_inspector in self.get_image_archive_inspectors():
                dedupe.update(archive_inspector.get_installed_package_build_dicts())
            self._cache[cn] = dedupe

        return self._cache[cn]

    def get_image_archive_inspector(self, arch: str) -> Optional[ArchiveImageInspector]:
        """
        Example listArchives output: https://gist.github.com/jupierce/a28a53e4057b550b3c8e5d6a8ac5198c
        :return: Returns the archive inspector for the specified arch    OR    None if the build does not possess one.
        """
        arch = brew_arch_for_go_arch(arch)  # Make sure this is a brew arch
        found = filter(lambda ai: ai.image_arch() == arch, self.get_image_archive_inspectors())
        return next(found, None)

    def is_under_embargo(self) -> bool:
        """
        :return: Returns whether this image build contains currently embargoed content.
        """
        if not self.runtime.group_config.public_upstreams:
            # when public_upstreams are not configured, we assume there is no private content.
            return False

        cn = 'is_private'
        if cn not in self._cache:
            with self.runtime.shared_build_status_detector() as bs_detector:
                # determine if the image build is embargoed (or otherwise "private")
                self._cache[cn] = len(bs_detector.find_embargoed_builds([self._brew_build_obj], [self.get_image_meta().candidate_brew_tag()])) > 0

        return self._cache[cn]

    def list_brew_tags(self) -> List[Dict]:
        """
        :return: Returns a list of tag definitions which are applied on this build.
        """
        cn = 'list_brew_tags'
        if cn not in self._cache:
            with self.runtime.pooled_koji_client_session() as koji_api:
                self._cache[cn] = koji_api.listTags(self._brew_build_id)

        return self._cache[cn]

    def list_brew_tag_names(self) -> List[str]:
        """
        :return: Returns the list of tag names which are applied to this build.
        """
        return [t['name'] for t in self.list_brew_tags()]

    async def find_non_latest_rpms(self) -> Dict[str, List[Tuple[str, str, str]]]:
        """
        If the packages installed in this image build overlap packages in the configured YUM repositories,
        return NVRs of the latest avaiable rpms that are not also installed in this image.
        This indicates that the image has not picked up the latest from configured repos.

        Note that this is completely normal for non-STREAM assemblies. In fact, it is
        normal for any assembly other than the assembly used for nightlies. In the age of
        a thorough config:scan-sources, if this method returns anything, scan-sources is
        likely broken.

        :return: Returns a dict. Keys are arch names; values are lists of (installed_rpm, latest_rpm, repo) tuples
        """
        meta = self.get_image_meta()
        assert meta is not None
        arches = meta.get_arches()
        tasks: List[Awaitable[List[Tuple[str, str, str]]]] = []
        for arch in arches:
            iar = self.get_image_archive_inspector(arch)
            assert iar is not None
            tasks.append(iar.find_non_latest_rpms())
        result = dict(zip(arches, await asyncio.gather(*tasks)))
        return result
