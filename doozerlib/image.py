from __future__ import absolute_import, print_function, unicode_literals
from typing import Dict, Optional
from doozerlib.distgit import pull_image
from doozerlib.metadata import Metadata
from doozerlib.model import Missing, Model
from doozerlib import assertion, exectools
from doozerlib import kojihelp
import json


class ImageMetadata(Metadata):

    # Mapping of pullspec => creation brew event id. Hold runtime.mutex while accessing.
    builder_image_event_cache = dict()

    # Mapping of package build_id to set of build creation_event_ids for that package.
    package_build_ids_build_events_cache = dict()

    def __init__(self, runtime, data_obj: Dict, commitish: Optional[str] = None):
        super(ImageMetadata, self).__init__('image', runtime, data_obj, commitish)
        self.image_name = self.config.name
        self.required = self.config.get('required', False)
        self.image_name_short = self.image_name.split('/')[-1]
        self.parent = None
        self.children = []  # list of ImageMetadata which use this image as a parent.
        dependents = self.config.get('dependents', [])
        for d in dependents:
            self.children.append(self.runtime.late_resolve_image(d, add=True))

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
        if 'from' in self.config:
            if 'member' in self.config['from']:
                base = self.config['from']['member']
                try:
                    self.parent = self.runtime.resolve_image(base)
                except:
                    self.parent = None

                if self.parent:
                    self.parent.add_child(self)

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

    def get_latest_build_info(self, default=-1):

        """
        Queries brew to determine the most recently built release of the component
        associated with this image. This method does not rely on the "release"
        label needing to be present in the Dockerfile.
        :param default: A value to return if no latest is found (if not specified, an exception will be thrown)
        :return: A tuple: (component name, version, release); e.g. ("registry-console-docker", "v3.6.173.0.75", "1")
        """

        component_name = self.get_component_name()

        rc, stdout, stderr = exectools.cmd_gather(["brew", "latest-build", self.candidate_brew_tag(), component_name])

        assertion.success(rc, "Unable to search brew builds: %s" % stderr)

        latest = stdout.strip().splitlines()[-1].split(' ')[0]

        if not latest.startswith(component_name):
            if default != -1:
                return default
            # If no builds found, `brew latest-build` output will appear as:
            # Build                                     Tag                   Built by
            # ----------------------------------------  --------------------  ----------------
            raise IOError("No builds detected for %s using tag: %s" % (self.qualified_name, self.candidate_brew_tag()))

        # latest example: "registry-console-docker-v3.6.173.0.75-1""
        name, version, release = latest.rsplit("-", 2)  # [ "registry-console-docker", "v3.6.173.0.75", "1"]

        return name, version, release

    def pull_url(self):
        # Don't trust what is the Dockerfile for version & release. This field may not even be present.
        # Query brew to find the most recently built release for this component version.
        _, version, release = self.get_latest_build_info()

        # we need to pull images from proxy since https://mojo.redhat.com/docs/DOC-1204856 if 'brew_image_namespace'
        # is enabled.
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

    def does_image_need_change(self, changing_rpm_packages=[], buildroot_tag_ids=None):
        """
        Answers the question of whether the latest built image needs to be rebuilt based on
        the RPMs it is dependent on, the state of brew tags of those RPMs, and the list
        of RPM package names about to change.
        :param changing_rpm_packages: A list of package names that are about to change.
        :param buildroot_tag_ids: A list of build tag id's the contribute to this image's build root.
        :return: True if the image should be rebuilt based on these criteria.
        """

        dgk = self.distgit_key
        runtime = self.runtime

        builder_creation_event_ids = set()
        with runtime.shared_koji_client_session() as koji_api:

            # Collect build times from any build images
            builders = self.config['from'].builder or []
            for builder in builders:
                if builder.member:
                    # We can't determine if images are about to change. Defer to scan-sources.
                    continue

                if builder.image:
                    builder_image_name = builder.image
                elif builder.stream:
                    builder_image_name = runtime.resolve_stream(builder.stream).image
                else:
                    raise IOError(f'Unable to determine builder image pullspec from {builder}')

                # builder_image_name example: "openshift/ose-base:ubi8"
                brew_image_url = self.runtime.resolve_brew_url(builder_image_name)

                with self.runtime.mutex:
                    brew_creation_event_id = ImageMetadata.builder_image_event_cache.get(brew_image_url, None)

                    if not brew_creation_event_id:
                        out, err = exectools.cmd_assert(f'oc image info {brew_image_url} --filter-by-os amd64 -o=json', retries=5, pollrate=10)
                        latest_builder_image_info = Model(json.loads(out, encoding='utf-8'))
                        builder_info_labels = latest_builder_image_info.config.config.Labels
                        builder_nvr_list = [builder_info_labels['com.redhat.component'], builder_info_labels['version'], builder_info_labels['release']]

                        if not all(builder_nvr_list):
                            raise IOError(f'Unable to find nvr in {builder_info_labels}')

                        builder_image_nvr = '-'.join(builder_nvr_list)
                        brew_creation_event_id = koji_api.getBuild(builder_image_nvr)['creation_event_id']
                        ImageMetadata.builder_image_event_cache[brew_image_url] = brew_creation_event_id
                        runtime.logger.debug(f'Found that builder image {brew_image_url} has creation event id {brew_creation_event_id}')

                    builder_creation_event_ids.add(brew_creation_event_id)

            image_nvr = '-'.join(self.get_latest_build_info(default=''))
            if not image_nvr:
                # Seems this have never been built. Mark it as needing change.
                return True

            runtime.logger.debug(f'Image {dgk} latest is {image_nvr}')

            # find the build that created this nvr. strict means throw exception if not found
            image_build = koji_api.getBuild(image_nvr, strict=True)
            image_build_event_id = image_build['creation_event_id']  # the brew even that created this build

            for builder_creation_event_id in builder_creation_event_ids:
                if image_build_event_id < builder_creation_event_id:
                    self.logger.info(f'{dgk} will be rebuilt because a builder image changed')

            build_root_changes = kojihelp.tags_changed_since_build(runtime, koji_api, image_build, buildroot_tag_ids)
            if build_root_changes:
                runtime.logger.info(f'{dgk} ({image_build}) will be rebuilt because it has not been built (last build event={image_build_event_id}) since a buildroot change: {build_root_changes}')
                return True

            scanned_pacakge_build_ids = set()
            for archive in koji_api.listArchives(image_build['id']):
                # Example results of listing RPMs in an given imageID:
                # https://gist.github.com/jupierce/a8798858104dcf6dfa4bd1d6dd99d2d8
                rpm_entries = koji_api.listRPMs(imageID=archive['id'])
                for ri, rpm_entry in enumerate(rpm_entries):

                    rpm_name = rpm_entry['name']
                    package_build_id = rpm_entry['build_id']
                    pacakge_build = koji_api.getBuild(package_build_id, strict=True)
                    rpm_package_name = pacakge_build['package_name']
                    runtime.logger.info(f'Checking whether rpm package {rpm_package_name} (rpm {rpm_name}) should cause rebuild for {dgk} [{ri+1} of {len(rpm_entries)}]')

                    if package_build_id in scanned_pacakge_build_ids:
                        # Since we are checking all arches, we may hit the same package build several times
                        # just for different arches. Ignore repeats.
                        continue
                    else:
                        scanned_pacakge_build_ids.add(package_build_id)

                    if rpm_package_name in changing_rpm_packages:
                        runtime.logger.info(f'{dgk} must change because of forthcoming RPM build {rpm_package_name}')
                        return True

                    with runtime.get_named_lock(f'#{package_build_id}'):
                        # See if we have already computed relevant builds for this package build id
                        build_event_ids = ImageMetadata.package_build_ids_build_events_cache.get(package_build_id, None)

                        if not build_event_ids:
                            # If you are considering changing this code, you are going to have to contend with
                            # complex scenarios like: what if we pulled in this RPM by tagging it, intentionally
                            # backlevel, from another product and want to keep like that? Or, what if we tagged
                            # in a non-released version of another product to get a fix pre-release of that package,
                            # but subsequently want to inherit later versions the original product ships.
                            # This blunt approach isn't trying to be perfect, but it will rarely do an unnecessary
                            # rebuild and handles those complex scenarios by erring on the side of doing the rebuild.

                            # To begin...
                            # Acquire a history of tags that have been applied to this build. These could be
                            # our group's tag (e.g. rhaos-...), or other product tags (e.g. if RHEL shipped the package and
                            # we pulled it from a repo or from a base image).
                            # Example output: https://gist.github.com/jupierce/c2f7311cc976e8cb5da4040c89356899
                            rpm_tag_history = koji_api.tagHistory(build=package_build_id, queryOpts={'order': 'create_event'})

                            relevant_tags = set()
                            for tag_event in rpm_tag_history:
                                tag_name = tag_event['tag_name']

                                if tag_name.endswith(('-released', '-set')):
                                    # Ignore errata tags (e.g. RHBA-2020:2309-released) and tags like rhel-8.0.0-z-batch-0.3-set
                                    continue

                                if tag_name.startswith(self.branch()):
                                    # If this was tagged with our brew_tag or brew_tag-candidate, we want to
                                    # check it.
                                    relevant_tags.add(tag_name)
                                    continue

                                # Now, there are other product's candidate tags to contend with. Since we don't care about
                                # non-release builds from other products, we can filter out those tags. We do this by
                                # querying the tags themselves and seeing if they are 'perm':'trusted'. Trusted tags are those
                                # that seem to be related to shipped advisories.
                                # See examples: https://gist.github.com/jupierce/adeb7b2b10f5d225c8090bab80640011
                                tag = koji_api.getTag(tag_name)
                                if not tag:
                                    # Some sort of pseudo tag like rhel-8.0.0-z-batch-0.3-set
                                    continue
                                tag_perm = tag.get('perm', None)

                                if not tag_perm:
                                    # Looks to be someone else's candidate tag or other non-shipment related tag.
                                    # rhaos-4.4-rhel-7 has tag_perm=='trusted'
                                    # kpatch-kernel-4.18.0-193.6.3.el8_2-build  has tag_perm='admin'
                                    continue

                                # tag_name now represents indicates another product shipped this build. Through various
                                # methods, our image build could have pick it up from this location. We aren't going to
                                # try to figure it out for certain -- just add it to the list of possibilities.
                                relevant_tags.add(tag_name)

                            if not relevant_tags:
                                # Just a sanity check
                                raise IOError(f'Found no relevant tags which make {rpm_package_name} available to {dgk}')

                            # Now we have a list of tags that were conceivably the source of our image build's RPM.
                            for rel_tag_name in relevant_tags:
                                # Let's determine if a later package has been tagged with this relevant tag.
                                # Example output: https://gist.github.com/jupierce/0e4a0d1e97b1a764fb5b7af08429a75c
                                latest_tagged = koji_api.getLatestRPMS(tag=rel_tag_name, package=rpm_package_name)[1]
                                build_event_ids = set([rel_build['creation_event_id'] for rel_build in latest_tagged])
                                # Record this list of events against the package build id so we don't have to do this
                                # calculation again (many images likely use similar rpm package builds).
                                ImageMetadata.package_build_ids_build_events_cache[package_build_id] = build_event_ids

                    # We have a list of build events that might have affected what we will pull in from tags
                    # which have previously been applied to a package build we pulled into the latest image.
                    # See if those builds happened more recently than our latest image. If they have, when we
                    # rebuild, those tags might contribute the new RPM to the new image. If they don't that is
                    # also fine. The new latest image will have a creation_event_id newer than the build events,
                    # and we will not retrigger this logic.
                    for rel_build_event_id in build_event_ids:
                        if rel_build_event_id > image_build_event_id:
                            # This build occurred after our image was built and was tagged with
                            # a tag that may have affected the image's inputs. Rebuild to be
                            # certain.
                            runtime.logger.info(f'{dgk} possible change because of changed rpm {rpm_package_name} and tag {rel_tag_name}')
                            # For the humans reading the output, let's output the tag names that mattered.
                            return True

        runtime.logger.info(f'Independent scan found no rebuild necessary for {dgk} ; it may still be rebuilt as an dependent image.')
        # The gauntlet has been passed
        return False
