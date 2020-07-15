from __future__ import absolute_import, print_function, unicode_literals

from typing import Dict, Optional
from doozerlib.distgit import pull_image
from doozerlib.metadata import Metadata
from doozerlib.model import Missing, Model
import hashlib
import pathlib
import os
import json
import re
from multiprocessing import Lock

from doozerlib.pushd import Dir
from doozerlib import assertion, exectools
from dockerfile_parse import DockerfileParser
from doozerlib import brew


class ImageMetadata(Metadata):

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

    def get_component_name(self, default=-1):
        """
        :param default: Not used. Here to stay consistent with similar rpmcfg.get_component_name
        :return: Returns the component name of the image. This is the name in the nvr
        that brew assigns to this image's build.
        """
        # By default, the bugzilla component is the name of the distgit,
        # but this can be overridden in the config yaml.
        component_name = self.name

        # For apbs, component name seems to have -apb appended.
        # ex. http://dist-git.host.prod.eng.bos.redhat.com/cgit/apbs/openshift-enterprise-mediawiki/tree/Dockerfile?h=rhaos-3.7-rhel-7
        if self.namespace == "apbs":
            component_name = "%s-apb" % component_name

        if self.namespace == "containers":
            component_name = "%s-container" % component_name

        if self.config.distgit.component is not Missing:
            component_name = self.config.distgit.component

        return component_name

    def get_brew_image_name_short(self):
        # Get image name in the Brew pullspec. e.g. openshift3/ose-ansible --> openshift3-ose-ansible
        return self.image_name.replace("/", "-")

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

    # Class methods to speed up computation of does_image_need_change if multiple
    # images will be assessed.
    # Mapping of brew pullspec => most recent brew build dict.
    builder_image_builds = dict()
    # Mapping of package build_id to set of build creation_event_ids for that package.
    package_build_ids_build_events_cache = dict()

    def does_image_need_change(self, changing_rpm_packages=[], buildroot_tag_ids=None):
        """
        Answers the question of whether the latest built image needs to be rebuilt based on
        the RPMs it is dependent on, the state of brew tags of those RPMs, and the list
        of RPM package names about to change. This method uses threads to accomplish its mission.
        There is no point in calling it with separate threads as the implementation will block
        all other callers until the first has completed. The reason is that the internal
        implementation is not thread-safe.
        :param changing_rpm_packages: A list of package names that are about to change.
        :param buildroot_tag_ids: A list of build tag id's the contribute to this image's build root.
        :return: (<bool>, messsage). If True, the image might need to be rebuilt -- the message will say
                why. If False, message will be None.
        """

        dgk = self.distgit_key
        runtime = self.runtime

        with runtime.shared_koji_client_session() as koji_api:  # Use shared to block all other callers

            image_build = self.get_latest_build(default='')
            if not image_build:
                # Seems this have never been built. Mark it as needing change.
                return True, 'Image has never been built before'

            self.logger.debug(f'Image {dgk} latest is {image_build}')

            image_nvr = image_build['nvr']
            image_build_event_id = image_build['creation_event_id']  # the brew even that created this build

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
                brew_image_url = self.runtime.resolve_brew_image_url(builder_image_name)
                builder_brew_build = ImageMetadata.builder_image_builds.get(brew_image_url, None)

                if not builder_brew_build:
                    out, err = exectools.cmd_assert(f'oc image info {brew_image_url} --filter-by-os amd64 -o=json', retries=5, pollrate=10)
                    latest_builder_image_info = Model(json.loads(out, encoding='utf-8'))
                    builder_info_labels = latest_builder_image_info.config.config.Labels
                    builder_nvr_list = [builder_info_labels['com.redhat.component'], builder_info_labels['version'], builder_info_labels['release']]

                    if not all(builder_nvr_list):
                        raise IOError(f'Unable to find nvr in {builder_info_labels}')

                    builder_image_nvr = '-'.join(builder_nvr_list)
                    builder_brew_build = koji_api.getBuild(builder_image_nvr)
                    ImageMetadata.builder_image_builds[brew_image_url] = builder_brew_build
                    self.logger.debug(f'Found that builder image {brew_image_url} has event {builder_brew_build}')

                if image_build_event_id < builder_brew_build['creation_event_id']:
                    self.logger.info(f'will be rebuilt because a builder image changed: {builder_image_name}')
                    return True, f'A builder image {builder_image_name} has changed since {image_nvr} was built'

            build_root_changes = brew.tags_changed_since_build(runtime, koji_api, image_build, buildroot_tag_ids)
            if build_root_changes:
                changing_tag_names = [brc['tag_name'] for brc in build_root_changes]
                self.logger.info(f'Image will be rebuilt due to buildroot change since {image_nvr} (last build event={image_build_event_id}). Build root changes changes: [{changing_tag_names}]')
                self.logger.debug(f'Image will be rebuilt due to buildroot change since ({image_build}) (last build event={image_build_event_id}). Build root changes: {build_root_changes}')
                return True, f'Buildroot tag changes in [{changing_tag_names}] since {image_nvr}'

            for archive in koji_api.listArchives(image_build['id']):
                # Example results of listing RPMs in an given imageID:
                # https://gist.github.com/jupierce/a8798858104dcf6dfa4bd1d6dd99d2d8
                rpm_entries = koji_api.listRPMs(imageID=archive['id'])

                changes_res = runtime.parallel_exec(
                    f=lambda idx, terminate_event: is_image_older_than_rpm(self, image_build_event_id, idx, rpm_entries, changing_rpm_packages),
                    args=range(len(rpm_entries)),
                    n_threads=20
                )

                for changed, msg in changes_res.get():
                    if changed:
                        return True, msg

        return False, None


# used for mutex in is_image_older_than_rpm()
older_than_lock = Lock()


def is_image_older_than_rpm(image_meta, image_build_event_id, ri, rpm_entries, changing_rpm_packages):
    """
    Determines if a given rpm has builds newer than a build event for an image.
    :param image_meta: The image meta object for the image
    :param image_build_event_id: The build event for the image
    :param ri: An index into the list of rpm_entries to check
    :param rpm_entries: A set of rpms to analyze (return of koji_api.listRPMs)
    :param changing_rpm_packages: A list of package name that the caller knows to be changing in the future
    :return: (<bool>, message) . If True, the message will describe the change reason. If False, message will
            will be None.
    """
    dgk = image_meta.distgit_key
    runtime = image_meta.runtime

    with runtime.pooled_koji_client_session() as koji_api:

        rpm_entry = rpm_entries[ri]
        rpm_name = rpm_entry['name']
        package_build_id = rpm_entry['build_id']
        pacakge_build = koji_api.getBuild(package_build_id, strict=True)
        rpm_package_name = pacakge_build['package_name']
        runtime.logger.info(
            f'Checking whether rpm package {rpm_package_name} (rpm {rpm_name}) should cause rebuild for {dgk} [{ri + 1} of {len(rpm_entries)}]')

        if rpm_package_name in changing_rpm_packages:
            runtime.logger.info(f'{dgk} must change because of forthcoming RPM build {rpm_package_name}')
            return True, f'image depends on {rpm_package_name}, which was identified as about to change'

        with older_than_lock:
            # See if we have already computed relevant builds for this package build id
            build_event_ids = ImageMetadata.package_build_ids_build_events_cache.get(package_build_id, None)
            if build_event_ids is None:
                # This is why the does_image_need_change is not thread safe. We register that
                # the current thread is going to be looking at this package build FOR THIS image.
                # If other threads were checking other images, this logic would break.
                ImageMetadata.package_build_ids_build_events_cache[package_build_id] = []
            else:
                # Another thread is already assessing this package. Let it do the work. This
                # thread would arrive at the same conclusion and waste time doing it.
                # We return False because we don't know the outcome and don't want to
                # affect the overall decision
                return False, None

        if not build_event_ids:
            # THIS thread has been chosen to do the work of finding build events and determining
            # whether those builds affect this image.

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

                if tag_name.startswith(image_meta.branch()):
                    # If this was tagged with our brew_tag or brew_tag-candidate, we want to
                    # check it.
                    relevant_tags.add(tag_name)
                    continue

                if tag_name.endswith('-candidate'):
                    # Now we can eliminate any other candidate tags
                    continue

                if re.match(r".+-rhel-\d+$", tag_name):
                    # Check for released tag naming convention. e.g. ends with rhel-7 or rhel-8.
                    relevant_tags.add(tag_name)
                    continue

                # Now for a fuzzier match assuming naming conventions are not followed.
                # We don't care about non-release builds from other products / oddball tags, so we can
                # filter out those tags. We do this by querying the tags themselves and seeing
                # if they are 'perm':'trusted'. Trusted tags are those
                # that seem to be related to shipped advisories.
                # See examples: https://gist.github.com/jupierce/adeb7b2b10f5d225c8090bab80640011
                tag = koji_api.getTag(tag_name)
                if not tag:
                    # Some sort of pseudo tag like rhel-8.0.0-z-batch-0.3-set
                    continue
                tag_perm = tag.get('perm', None)

                if not tag_perm:
                    # Looks to be someone else's unconventional candidate tag or other non-shipment related tag.
                    # rhaos-4.4-rhel-7 has tag_perm=='trusted'
                    # kpatch-kernel-4.18.0-193.6.3.el8_2-build  has tag_perm='admin'
                    continue

                # tag_name now represents indicates another product shipped this build. Through various
                # methods, our image build could have pick it up from this location. We aren't going to
                # try to figure it out for certain -- just add it to the list of possibilities.
                relevant_tags.add(tag_name)

            if not relevant_tags:
                # Just a sanity check
                raise IOError(f'Found no relevant tags which make rpm package name {rpm_package_name} available to image {dgk}; rpm_tag_history: {rpm_tag_history}')

            # Now we have a list of tags that were conceivably the source of our image build's RPM.
            for rel_tag_name in relevant_tags:
                # Let's determine if a later package has been tagged with this relevant tag.
                # Example output: https://gist.github.com/jupierce/0e4a0d1e97b1a764fb5b7af08429a75c
                latest_tagged = koji_api.getLatestRPMS(tag=rel_tag_name, package=rpm_package_name)[1]
                build_event_ids = set([rel_build['creation_event_id'] for rel_build in latest_tagged])
                # Record this list of events against the package build id so we don't have to do this
                # calculation again (many images likely use similar rpm package builds).
                with older_than_lock:
                    # This is not perfect -- another thread may also have computed this for another
                    # image, but we don't want to hold the lock for that long. The race condition is
                    # safe to allow.
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
                runtime.logger.info(f'{dgk} possible change because of changed rpm {rpm_package_name} in tag {rel_tag_name}')
                # For the humans reading the output, let's output the tag names that mattered.
                return True, f'A new build of {rpm_package_name} from {rel_tag_name} might be pulled into the image'

    return False, None

    def covscan(self, result_archive, repo_type='unsigned', local_repo=[]):
        self.logger.info('Setting up for coverity scan')
        all_js = 'all_results.js'
        diff_js = 'diff_results.js'
        all_html = 'all_results.html'
        diff_html = 'diff_results.html'
        waived_flag = 'waived.flag'

        archive_path = pathlib.Path(result_archive)
        dg_archive_path = archive_path.joinpath(self.distgit_key)
        dg_archive_path.mkdir(parents=True, exist_ok=True)  # /<archive-dir>/<dg-key>

        builders = self.config['from'].builder
        if builders is Missing:
            self.logger.info('No builder images -- does not appear to be container first. Skipping.')
            return

        dgr = self.distgit_repo()
        with Dir(dgr.distgit_dir):
            dg_commit_hash, _ = exectools.cmd_assert('git rev-parse HEAD', strip=True)
            archive_commit_results_path = dg_archive_path.joinpath(dg_commit_hash)  # /<archive-dir>/<dg-key>/<hash>
            archive_all_results_js_path = archive_commit_results_path.joinpath(all_js)  # /<archive-dir>/<dg-key>/<hash>/all_results.js
            archive_all_results_html_path = archive_commit_results_path.joinpath(all_html)  # /<archive-dir>/<dg-key>/<hash>/all_results.html
            archive_diff_results_js_path = archive_commit_results_path.joinpath(diff_js)
            archive_diff_results_html_path = archive_commit_results_path.joinpath(diff_html)
            archive_waived_flag_path = archive_commit_results_path.joinpath(waived_flag)

            def write_record():
                diff = json.loads(archive_diff_results_js_path.read_text(encoding='utf-8'))
                diff_count = len(diff['issues'])
                if diff_count == 0:
                    self.logger.info('No new issues found during scan')
                    archive_waived_flag_path.write_text('')  # No new differences, mark as waived

                owners = ",".join(self.config.owners or [])
                self.runtime.add_record('covscan',
                                        distgit=self.qualified_name,
                                        distgit_key=self.distgit_key,
                                        commit_results_path=str(archive_commit_results_path),
                                        all_results_js_path=str(archive_all_results_js_path),
                                        all_results_html_path=str(archive_all_results_html_path),
                                        diff_results_js_path=str(archive_diff_results_js_path),
                                        diff_results_html_path=str(archive_diff_results_html_path),
                                        diff_count=str(diff_count),
                                        waive_path=str(archive_waived_flag_path),
                                        waived=str(archive_waived_flag_path.exists()).lower(),
                                        owners=owners,
                                        image=self.config.name,
                                        commit_hash=dg_commit_hash)

            if archive_diff_results_html_path.exists():
                self.logger.info(f'This commit already has scan results ({str(archive_all_results_js_path)}; skipping scan')
                write_record()
                return

            archive_commit_results_path.mkdir(parents=True, exist_ok=True)

            dg_path = pathlib.Path(Dir.getcwd())
            cov_path = dg_path.joinpath('cov')

            dockerfile_path = dg_path.joinpath('Dockerfile')
            if not dockerfile_path.exists():
                self.logger.error('Dockerfile does not exist in distgit; not rebased yet?')
                return

            dfp = DockerfileParser(str(dockerfile_path))
            covscan_builder_df_path = dg_path.joinpath('Dockerfile.covscan.builder')

            with covscan_builder_df_path.open(mode='w+') as df_out:
                first_parent = dfp.parent_images[0]
                ns, repo_tag = first_parent.split(
                    '/')  # e.g. openshift/golang-builder:latest => [openshift, golang-builder:latest]
                if '@' in repo_tag:
                    repo, tag = repo_tag.split(
                        '@')  # e.g. golang-builder@sha256:12345 =>  golang-builder & tag=sha256:12345
                    tag = '@' + tag
                else:
                    if ':' in repo_tag:
                        repo, tag = repo_tag.split(':')  # e.g. golang-builder:latest =>  golang-builder & tag=latest
                    else:
                        repo = repo_tag
                        tag = 'latest'
                    tag = ':' + tag

                first_parent_url = f'registry-proxy.engineering.redhat.com/rh-osbs/{ns}-{repo}{tag}'

                # build a local image name we can use.
                # We will build a local scanner image based on the target distgit's first parent.
                # It will have coverity tools installed.
                m = hashlib.md5()
                m.update(first_parent.encode('utf-8'))
                local_builder_tag = f'{repo}-{m.hexdigest()}'

                vol_mount_arg = ''
                make_image_repo_files = ''

                if local_repo:
                    for idx, lr in enumerate(local_repo):
                        make_image_repo_files += f"""
# Create a repo able to pull from the local filesystem and prioritize it for speed.
RUN echo '[covscan_local_{idx}]' > /etc/yum.repos.d/covscan_local_{idx}.repo
RUN echo 'baseurl=file:///covscan_local_{idx}' >> /etc/yum.repos.d/covscan_local_{idx}.repo
RUN echo skip_if_unavailable=True >> /etc/yum.repos.d/covscan_local_{idx}.repo
RUN echo gpgcheck=0 >> /etc/yum.repos.d/covscan_local_{idx}.repo
RUN echo enabled=1 >> /etc/yum.repos.d/covscan_local_{idx}.repo
RUN echo enabled_metadata=1 >> /etc/yum.repos.d/covscan_local_{idx}.repo
RUN echo priority=1 >> /etc/yum.repos.d/covscan_local_{idx}.repo
"""
                        vol_mount_arg += f' -mount {lr}:/covscan_local_{idx}'
                else:
                    make_image_repo_files = 'RUN wget https://cov01.lab.eng.brq.redhat.com/coverity/install/covscan/covscan-rhel-7.repo -O /etc/yum.repos.d/covscan.repo\n'

                df_out.write(f'''FROM {first_parent_url}
LABEL DOOZER_COVSCAN_PARENT_IMAGE=true
LABEL DOOZER_COVSCAN_FIRST_PARENT={local_builder_tag}
LABEL DOOZER_COVSCAN_GROUP={self.runtime.group_config.name}

{make_image_repo_files}

RUN yum install -y wget

RUN wget {self.cgit_url(".oit/" + repo_type + ".repo")} -O /etc/yum.repos.d/oit.repo
RUN yum install -y python36

# Enable epel for csmock
RUN wget https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
RUN yum -y install epel-release-latest-7.noarch.rpm

# Certs necessary to install from covscan repos
RUN wget https://password.corp.redhat.com/RH-IT-Root-CA.crt -O /etc/pki/ca-trust/source/anchors/RH-IT-Root-CA.crt --no-check-certificate
RUN wget https://password.corp.redhat.com/legacy.crt -O /etc/pki/ca-trust/source/anchors/legacy.crt --no-check-certificate
RUN wget https://engineering.redhat.com/Eng-CA.crt -O /etc/pki/ca-trust/source/anchors/Eng-CA.crt --no-check-certificate
RUN update-ca-trust
RUN update-ca-trust enable

RUN yum install -y cov-sa csmock csmock-plugin-coverity csdiff
''')

            rc, out, err = exectools.cmd_gather(f'docker image inspect {local_builder_tag}')
            if rc != 0:
                rc, _, _ = exectools.cmd_gather(f'imagebuilder {vol_mount_arg} -t {local_builder_tag} -f {str(covscan_builder_df_path)} {str(dg_path)}')
                if rc != 0:
                    self.logger.error(f'Unable to create scanner image based on builder image: {first_parent_url}')
                    # TODO: log this as a record and make sure the pipeline warns artist.
                    # until covscan can be installed on rhel-8, this is expected for some images.
                    return

            # We should now have an image tagged local_builder_tag based on the original image's builder image
            # but also with covscan tools installed.

            covscan_df_path = dg_path.joinpath('Dockerfile.covscan')

            runs = ['#!/bin/bash', 'set -o xtrace']
            setup = []
            first_from = False
            for entry in dfp.structure:
                content = entry['content']
                instruction = entry['instruction'].upper()
                if instruction.upper() == 'FROM':
                    if first_from:
                        break
                    first_from = True

                if instruction == 'COPY':
                    setup.append(content)

                if instruction == 'ENV':
                    setup.append(content)

                if instruction == 'RUN':
                    runs.append(content.strip()[4:])

                if instruction == 'WORKDIR':
                    setup.append(content)  # Pass into setup so things like ADD work as expected
                    path = content.strip()[7:]
                    runs.append('mkdir -p ' + path)  # Also pass into RUN so they have the correct working dir
                    runs.append('cd ' + path)

            run_file = '\n'.join(runs)
            self.logger.info(f'Constructed run file:\n{run_file}')

            build_script_name = 'doozer_covscan_runit.sh'
            with dg_path.joinpath(build_script_name).open(mode='w+') as runit:
                runit.write(run_file)

            with covscan_df_path.open(mode='w+') as df_out:
                df_out.write(f'FROM {local_builder_tag}\n')
                df_out.write(f'LABEL DOOZER_COVSCAN_GROUP={self.runtime.group_config.name}\n')
                df_out.write(f'ADD {build_script_name} /\n')
                df_out.write(f'RUN chmod +x /{build_script_name}\n')
                df_out.write('\n'.join(setup) + '\n')
                df_out.write('ENV PATH=/opt/coverity/bin:${PATH}\n')

            run_tag = f'{local_builder_tag}.{self.image_name_short}'
            rc, out, err = exectools.cmd_gather(f'docker image inspect {run_tag}')
            if rc != 0:
                exectools.cmd_assert(f'imagebuilder -t {run_tag} -f {str(covscan_df_path)} {str(dg_path)}')

            cov_path.mkdir(exist_ok=True)
            emit_path = cov_path.joinpath('emit')
            if not emit_path.exists():
                rc, out, err = exectools.cmd_gather(f'docker run --hostname=covscan --rm -v {str(cov_path)}:/cov:z {run_tag} cov-build --dir=/cov /{build_script_name}')
                if rc != 0:
                    self.logger.error('Did not achieve full compilation')

                builg_log_path = cov_path.joinpath('build-log.txt')
                build_log = builg_log_path.read_text(encoding='utf-8')
                if '[WARNING] No files were emitted' in build_log:
                    self.logger.error(f'Build did not emit anything. Check out the build-log.txt: {builg_log_path}')
                    # TODO: log this as a record and make sure the pipeline warns artitst
                    return

            else:
                self.logger.info('covscan emit already exists -- skipping this step')

            def run_docker_cov(cmd):
                return exectools.cmd_assert(f'docker run --hostname=covscan --rm -v {str(cov_path)}:/cov:z {run_tag} {cmd}')

            summary_path = cov_path.joinpath('output', 'summary.txt')
            if not summary_path.exists() or 'Time taken by analysis' not in summary_path.read_text(encoding='utf-8'):
                # This can take an extremely long time and use virtually all CPU
                run_docker_cov('cov-analyze  --dir=/cov "--wait-for-license" "-co" "ASSERT_SIDE_EFFECT:macro_name_lacks:^assert_(return|se)\\$" "-co" "BAD_FREE:allow_first_field:true" "--include-java" "--fb-max-mem=4096" "--security" "--concurrency" --allow-unmerged-emits')
            else:
                self.logger.info('covscan analysis already exists -- skipping this step')

            all_results_js, _ = run_docker_cov('cov-format-errors --json-output-v2 /dev/stdout --dir=/cov')
            all_results_js_path = cov_path.joinpath(all_js)
            all_results_js_path.write_text(all_results_js, encoding='utf-8')

            all_results_html, _ = run_docker_cov(f'cshtml /cov/{all_js}')
            all_results_html_path = cov_path.joinpath(all_html)
            all_results_html_path.write_text(all_results_html, encoding='utf-8')

            run_docker_cov(f'chown -R {os.getuid()}:{os.getgid()} /cov')  # Otherwise, root will own these files

            # Write out the files to the archive directory as well
            archive_all_results_js_path.write_text(all_results_js, encoding='utf-8')
            archive_all_results_html_path.write_text(all_results_html, encoding='utf-8')

            # Now on to computing diffs

            # Search backwards through commit history; try to find a hash for this distgit that has been scanned before
            diff_results_js = all_results_js  # Unless we find an old has, diff results matches all results
            commit_log, _ = exectools.cmd_assert("git --no-pager log --pretty='%H' -1000")
            for old_commit in commit_log.split()[1:]:
                old_all_results_js_path = dg_archive_path.joinpath(old_commit, all_js)
                old_are_results_waived_path = dg_archive_path.joinpath(old_commit, waived_flag)
                # Only compute diff from commit if results were actually waived
                # This file should be created by the Jenkins / scanning pipeline.
                if old_are_results_waived_path.exists():
                    diff_results_js, _ = exectools.cmd_assert(f'csdiff {str(archive_all_results_js_path)} {str(old_all_results_js_path)}')
                    break

            archive_diff_results_js_path.write_text(diff_results_js, encoding='utf-8')
            diff_results_html, _ = exectools.cmd_assert(f'cshtml {str(archive_diff_results_js_path)}')
            archive_diff_results_html_path.write_text(diff_results_html, encoding='utf-8')

            write_record()
