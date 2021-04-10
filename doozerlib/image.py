from __future__ import absolute_import, print_function, unicode_literals

import hashlib
import json
import os
import pathlib
import random
from typing import Any, Dict, Optional

from dockerfile_parse import DockerfileParser

from doozerlib import brew, exectools, util
from doozerlib.distgit import pull_image
from doozerlib.metadata import Metadata
from doozerlib.model import Missing, Model
from doozerlib.pushd import Dir


class ImageMetadata(Metadata):

    def __init__(self, runtime, data_obj: Dict, commitish: Optional[str] = None, clone_source: Optional[bool] = False):
        super(ImageMetadata, self).__init__('image', runtime, data_obj, commitish)
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

    def does_image_need_change(self, changing_rpm_packages=[], buildroot_tag=None, newest_image_event_ts=None, oldest_image_event_ts=None):
        """
        Answers the question of whether the latest built image needs to be rebuilt based on
        the packages (and therefore RPMs) it is dependent on might have changed in tags
        relevant to the image. A check is also made if the image depends on a package
        we know is changing because we are about to rebuild it.
        :param changing_rpm_packages: A list of package names that are about to change.
        :param buildroot_tag: The build root for this image
        :param newest_image_event_ts: The build timestamp of the most recently built image in this group.
        :param oldest_image_event_ts: The build timestamp of the oldest build in this group from getLatestBuild of each component.
        :return: (meta, <bool>, messsage). If True, the image might need to be rebuilt -- the message will say
                why. If False, message will be None.
        """

        dgk = self.distgit_key
        runtime = self.runtime

        builds_contained_in_archives = {}  # build_id => result of koji.getBuild(build_id)
        with runtime.pooled_koji_client_session() as koji_api:

            image_build = self.get_latest_build(default='')
            if not image_build:
                # Seems this have never been built. Mark it as needing change.
                return self, True, 'Image has never been built before'

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
                    # Example output: https://gist.github.com/jupierce/3bbc8be7265348a8f549d401664c9972
                    extra_latest_tagging_infos = koji_api.queryHistory(table='tag_listing', tag=extra_package_brew_tag, package=extra_package_name, active=True)['tag_listing']

                    if extra_latest_tagging_infos:
                        # We have information about the most recent time this package was tagged into the
                        # relevant tag. Why the tagging event and not the build time? Well, the build could have been
                        # made long ago, but only tagged into the relevant tag recently.
                        extra_latest_tagging_event = extra_latest_tagging_infos[0]['create_event']
                        self.logger.debug(f'Checking image creation time against extra_packages {extra_package_name} in tag {extra_package_brew_tag} @ tagging event {extra_latest_tagging_event}')
                        if extra_latest_tagging_event > image_build_event_id:
                            return self, True, f'Image {dgk} is sensitive to extra_packages {extra_package_name} which changed at event {extra_latest_tagging_event}'
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
                    return self, True, f'A builder or parent image {builder_image_name} has changed since {image_nvr} was built'

            build_root_change = brew.has_tag_changed_since_build(runtime, koji_api, image_build, buildroot_tag, inherit=True)
            if build_root_change:
                self.logger.info(f'Image will be rebuilt due to buildroot change since {image_nvr} (last build event={image_build_event_id}). Build root change: [{build_root_change}]')
                return self, True, f'Buildroot tag changes since {image_nvr} was built'

            archives = koji_api.listArchives(image_build['id'])

            # Compare to the arches in runtime
            build_arches = set()
            for a in archives:
                # When running with cachito, not all archives returned are images. Filter out non-images.
                if a['btype'] == 'image':
                    build_arches.add(a['extra']['image']['arch'])

            target_arches = set(self.get_arches())
            if target_arches != build_arches:
                # The latest brew build does not exactly match the required arches as specified in group.yml
                return self, True, f'Arches of {image_nvr}: ({build_arches}) does not match target arches {target_arches}'

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
                        return self, True, f'Image includes {package_name} which is also about to change'
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

        for changed, msg in changes_res.get():
            if changed:
                return self, True, msg

        return self, False, None

    def covscan(self, result_archive, repo_type='unsigned', local_repo_rhel_7=[], local_repo_rhel_8=[]):
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
                diff_count = len(diff.get('issues', []))
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

                if local_repo_rhel_7:
                    for idx, lr in enumerate(local_repo_rhel_7):
                        make_image_repo_files += f"""
# Create a repo able to pull from the local filesystem and prioritize it for speed.
RUN if cat /etc/redhat-release | grep "release 7"; then echo '[covscan_local_{idx}]' > /etc/yum.repos.d/covscan_local_{idx}.repo && \
    echo 'baseurl=file:///covscan_local_{idx}_rhel_7' >> /etc/yum.repos.d/covscan_local_{idx}.repo && \
    echo skip_if_unavailable=True >> /etc/yum.repos.d/covscan_local_{idx}.repo && \
    echo gpgcheck=0 >> /etc/yum.repos.d/covscan_local_{idx}.repo && \
    echo enabled=1 >> /etc/yum.repos.d/covscan_local_{idx}.repo && \
    echo enabled_metadata=1 >> /etc/yum.repos.d/covscan_local_{idx}.repo && \
    echo priority=1 >> /etc/yum.repos.d/covscan_local_{idx}.repo; fi
    """
                        vol_mount_arg += f' -mount {lr}:/covscan_local_{idx}_rhel_7'
                else:
                    make_image_repo_files = 'RUN if cat /etc/redhat-release | grep "release 7"; then wget --no-check-certificate https://cov01.lab.eng.brq.redhat.com/coverity/install/covscan/covscan-rhel-7.repo -O /etc/yum.repos.d/covscan.repo; fi\n'

                if local_repo_rhel_8:
                    for idx, lr in enumerate(local_repo_rhel_8):
                        make_image_repo_files += f"""
# Create a repo able to pull from the local filesystem and prioritize it for speed.
RUN if cat /etc/redhat-release | grep "release 8"; then echo '[covscan_local_{idx}]' > /etc/yum.repos.d/covscan_local_{idx}.repo && \
    echo 'baseurl=file:///covscan_local_{idx}_rhel_8' >> /etc/yum.repos.d/covscan_local_{idx}.repo && \
    echo skip_if_unavailable=True >> /etc/yum.repos.d/covscan_local_{idx}.repo && \
    echo gpgcheck=0 >> /etc/yum.repos.d/covscan_local_{idx}.repo && \
    echo enabled=1 >> /etc/yum.repos.d/covscan_local_{idx}.repo && \
    echo enabled_metadata=1 >> /etc/yum.repos.d/covscan_local_{idx}.repo && \
    echo priority=1 >> /etc/yum.repos.d/covscan_local_{idx}.repo; fi
    """
                        vol_mount_arg += f' -mount {lr}:/covscan_local_{idx}_rhel_8'
                else:
                    make_image_repo_files = 'RUN if cat /etc/redhat-release | grep "release 8"; then wget --no-check-certificate https://cov01.lab.eng.brq.redhat.com/coverity/install/covscan/covscan-rhel-8.repo -O /etc/yum.repos.d/covscan.repo; fi\n'

                df_out.write(f'''FROM {first_parent_url}
LABEL DOOZER_COVSCAN_PARENT_IMAGE=true
LABEL DOOZER_COVSCAN_FIRST_PARENT={local_builder_tag}
LABEL DOOZER_COVSCAN_GROUP={self.runtime.group_config.name}

{make_image_repo_files}

RUN yum install -y wget

RUN wget {self.cgit_url(".oit/" + repo_type + ".repo")} -O /etc/yum.repos.d/oit.repo
RUN yum install -y python36

# Enable epel for csmock
RUN if cat /etc/redhat-release | grep "release 7"; then wget https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm && yum -y install epel-release-latest-7.noarch.rpm; fi
RUN if cat /etc/redhat-release | grep "release 8"; then wget https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm && yum -y install epel-release-latest-8.noarch.rpm; fi


# Certs necessary to install from covscan repos
RUN wget https://password.corp.redhat.com/RH-IT-Root-CA.crt -O /etc/pki/ca-trust/source/anchors/RH-IT-Root-CA.crt --no-check-certificate
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
                # cov-capture will search for files like .js, typescript, python in the source directory;
                # cov-analyze will then search for issues within those non-compiled files.
                # https://community.synopsys.com/s/question/0D52H000054zcvZSAQ/i-would-like-to-know-the-coverity-static-analysis-process-for-node-js-could-you-please-provide-me-the-sample-steps-to-run-coverity-for-node-js
                rc, out, err = exectools.cmd_gather(f'docker run --hostname=covscan --rm -v {str(cov_path)}:/cov:z -v {str(dg_path)}:/covscan-src:z {run_tag} cov-capture --dir /cov --source-dir /covscan-src')
                if rc != 0:
                    self.logger.error('Error running source detection')

                rc, out, err = exectools.cmd_gather(f'docker run --hostname=covscan --rm -v {str(cov_path)}:/cov:z {run_tag} cov-build --dir=/cov /{build_script_name}')
                if rc != 0:
                    self.logger.error('Did not achieve full compilation')

                builg_log_path = cov_path.joinpath('build-log.txt')
                build_log = builg_log_path.read_text(encoding='utf-8')
                if '[WARNING] No files were emitted' in build_log:
                    self.logger.error(f'Build did not emit anything. Check out the build-log.txt: {builg_log_path}')
                    # TODO: log this as a record and make sure the pipeline warns artist
                    return

            else:
                self.logger.info('covscan emit already exists -- skipping this step')

            def run_docker_cov(cmd):
                return exectools.cmd_assert(f'docker run --hostname=covscan --rm -v {str(cov_path)}:/cov:z {str(dg_path)}:/covscan-src:z {run_tag} {cmd}')

            summary_path = cov_path.joinpath('output', 'summary.txt')
            if not summary_path.exists() or 'Time taken by analysis' not in summary_path.read_text(encoding='utf-8'):
                # This can take an extremely long time and use virtually all CPU
                run_docker_cov('cov-analyze  --dir=/cov "--wait-for-license" "-co" "ASSERT_SIDE_EFFECT:macro_name_lacks:^assert_(return|se)\\$" "-co" "BAD_FREE:allow_first_field:true" "--include-java" "--fb-max-mem=4096" "--all" "--security" "--concurrency" --allow-unmerged-emits')
            else:
                self.logger.info('covscan analysis already exists -- skipping this step')

            all_results_js, _ = run_docker_cov('cov-format-errors --json-output-v2 /dev/stdout --dir=/cov')
            all_results_js_path = cov_path.joinpath(all_js)
            all_results_js_path.write_text(all_results_js, encoding='utf-8')

            all_results_html = "<html>Error generating HTML report.</html>"
            try:
                # Rarely, cshtml just outputs empty html and rc==1; just ignore it.
                all_results_html, _ = run_docker_cov(f'cshtml /cov/{all_js}')
            except:
                self.logger.warning(f'Error generating HTML report for {str(archive_all_results_js_path)}')
                pass

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

            diff_results_html = "<html>Error generating HTML report.</html>"
            try:
                # Rarely, cshtml just outputs empty html and rc==1; just ignore it.
                diff_results_html, _ = exectools.cmd_assert(f'cshtml {str(archive_diff_results_js_path)}')
            except:
                self.logger.warning(f'Error generating HTML report for {str(archive_diff_results_js_path)}')
                pass

            archive_diff_results_html_path.write_text(diff_results_html, encoding='utf-8')

            write_record()

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


def is_image_older_than_package_build_tagging(image_meta, image_build_event_id, package_build, newest_image_event_ts, oldest_image_event_ts):
    """
    Determines if a given rpm is part of a package that has been tagged in a relevant tag AFTER image_build_event_id
    :param image_meta: The image meta object for the image.
    :param image_build_event_id: The build event for the image.
    :param package_build: The package build to assess.
    :param newest_image_event_ts: The build timestamp of the most recently built image in this group.
    :param oldest_image_event_ts: The build timestamp of the oldest build in this group from getLatestBuild of each component.
    :return: (<bool>, message) . If True, the message will describe the change reason. If False, message will
            will be None.
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
            return True, f'Package {package_name} has been retagged by potentially relevant tags since image build: {intersection_set}'

    return False, None
