import glob
import io
import json
import os
import re
import threading
import yaml

from dockerfile_parse import DockerfileParser
from doozerlib import brew, exectools, logutil, pushd


class OLMBundle(object):
    """This class is responsible for generating bundle containers out of previously built operators

    Every OLM Operator image should have a corresponding bundle container, which is mostly empty,
    carrying only the operator's manifests and some special Dockerfile labels, that allows the
    bundle container to publish those manifests on operator's behalf

    Even though each bundle container has its dedicated distgit repo, they are not meant to be
    independently built, due to their tight coupling to corresponding operators
    """

    def __init__(self, runtime):
        self.runtime = runtime

    def find_bundle_for(self, operator_nvr):
        """Check if a bundle already exists for a given `operator_nvr`.

        :param string operator_nvr: Operator NVR (format: my-operator-v4.2.30-202004200449)
        :return string: NVR of latest found bundle build, or None if there is no bundle build nvr
                        corresponding to the last operator build's nvr.
        """
        self.operator_nvr = operator_nvr
        self.get_operator_buildinfo()

        builds = self.brew_session.listTagged(tag=self.target, package=self.bundle_brew_component)
        vr = self.operator_nvr.replace(self.operator_brew_component, '')[1:].replace('-', '.', 1)
        found = list(build for build in builds if vr == build['version'])
        found.sort(reverse=True, key=lambda build: int(build['release']))
        return found[0]['nvr'] if found else None

    def get_latest_bundle_build(self):
        """
        Return information about the latest build of bundle associated with the current operator.
        Note that this may not correspond to the latest build of the operator itself IF the operator was:
        1. Built again after this bundle
        2. The latest operator build failed to successfully build its bundle
        :return: A dict containing koji build information; None if no build is found
        """
        builds = self.brew_session.getLatestBuilds(tag=self.target, package=self.bundle_brew_component)
        if builds:
            return builds[0]
        else:
            return None

    def rebase(self, operator_nvr):
        """Update bundle distgit contents with manifests from given operator NVR
        Perform image SHA replacement on manifests before commit & push
        Annotations and Dockerfile labels are re-generated with info from operator's package YAML

        :param string operator_nvr: Operator NVR (format: my-operator-v4.2.30-202004200449)
        :return bool True if rebase succeeds, False if there was nothing new to commit
        """
        self.operator_nvr = operator_nvr
        self.get_operator_buildinfo()
        self.clone_operator()
        self.checkout_operator_to_build_commit()
        self.clone_bundle()
        self.clean_bundle_contents()
        self.get_operator_package_yaml_info()
        self.copy_operator_manifests_to_bundle()
        self.replace_image_references_by_sha_on_bundle_manifests()
        self.generate_bundle_annotations()
        self.generate_bundle_dockerfile()
        self.create_container_yaml()
        return self.commit_and_push_bundle(commit_msg="Update bundle manifests")

    def build(self, operator_name=None):
        """Trigger a brew build of operator's bundle

        :param string operator_name: Operator name (as in ocp-build-data file name, not brew component)
        :return bool True if build succeeds, False otherwise
        """
        if operator_name:
            self.operator_repo_name = 'containers/{}'.format(operator_name)

        self.clone_bundle()

        if not self.trigger_bundle_container_build():
            return False

        return self.watch_bundle_container_build()

    def get_latest_bundle_build_nvr(self):
        """Get NVR of latest bundle build tagged on given target

        :return string: NVR of latest bundle build, or "" if there is none.
        """
        build = self.get_latest_bundle_build()
        if not build or 'nvr' not in build:
            return None
        return build['nvr']

    def get_bundle_image_name(self):
        return 'openshift/ose-{}'.format(self.bundle_name)

    def get_operator_buildinfo(self, nvr=None):
        """Get operator distgit repository name and commit hash used to build given operator NVR
        :param nvr: If specified, used to set self.operator_nvr.
        """
        if nvr:
            self.operator_nvr = nvr

        operator_buildinfo = brew.get_build_objects([self.operator_nvr], self.brew_session)[0]
        match = re.search(r'([^#]+)#(\w+)', operator_buildinfo['source'])

        self.operator_repo_name = '/'.join(match.group(1).split('/')[-2:])
        self.operator_build_commit = match.group(2)

    def clone_operator(self):
        """Clone operator distgit repository to doozer working dir
        """
        exectools.cmd_assert('rm -rf {}'.format(self.operator_clone_path))
        exectools.cmd_assert('mkdir -p {}'.format(os.path.dirname(self.operator_clone_path)))
        exectools.cmd_assert('rhpkg{}clone --branch {} {} {}'.format(
            self.rhpkg_opts, self.branch, self.operator_repo_name, self.operator_clone_path
        ), retries=3)

    def checkout_operator_to_build_commit(self):
        """Checkout clone of operator repository to specific commit used to build given operator NVR
        """
        with pushd.Dir(self.operator_clone_path):
            exectools.cmd_assert('git checkout {}'.format(self.operator_build_commit))

    def clone_bundle(self):
        """Clone corresponding bundle distgit repository of given operator NVR
        """
        exectools.cmd_assert('rm -rf {}'.format(self.bundle_clone_path))
        exectools.cmd_assert('mkdir -p {}'.format(os.path.dirname(self.bundle_clone_path)))
        exectools.cmd_assert('rhpkg{}clone --branch {} {} {}'.format(
            self.rhpkg_opts, self.branch, self.bundle_repo_name, self.bundle_clone_path
        ), retries=3)

    def clean_bundle_contents(self):
        """Delete all files currently present in the bundle repository
        Generating bundle files is an idempotent operation, so it is much easier to clean up
        everything and re-create them instead of parsing and figuring out what changed

        At the end, only relevant diff, if any, will be committed.
        """
        exectools.cmd_assert('git -C {} rm -rf *'.format(self.bundle_clone_path))

    def get_operator_package_yaml_info(self):
        """Get operator package name and channel from its package YAML
        This info will be used to generate bundle's Dockerfile labels and metadata/annotations.yaml
        """
        file_path = glob.glob('{}/*package.yaml'.format(self.operator_manifests_dir))[0]
        package_yaml = yaml.safe_load(io.open(file_path, encoding='utf-8'))

        self.package = package_yaml['packageName']
        self.channel = str(package_yaml['channels'][0]['name'])

    def copy_operator_manifests_to_bundle(self):
        """Copy all manifests from the operator distgit repository over to its corresponding bundle
        repository (except image-references file)
        We can be sure that the manifests contents are exactly what we expect, because our copy of
        operator repository is checked out to the specific commit used to build given operator NVR
        """
        exectools.cmd_assert('mkdir -p {}'.format(self.bundle_manifests_dir))
        exectools.cmd_assert('cp -r {} {}/'.format(
            ' '.join(self.list_of_manifest_files_to_be_copied),
            self.bundle_manifests_dir
        ))
        exectools.cmd_assert('rm -f {}/image-references'.format(self.bundle_manifests_dir))

    def replace_image_references_by_sha_on_bundle_manifests(self):
        """Iterate through all bundle manifests files, replacing any image reference tag by its
        corresponding SHA
        That is used to allow disconnected installs, where a cluster can't reach external registries
        in order to translate image tags into something "pullable"
        """
        for file in glob.glob('{}/*'.format(self.bundle_manifests_dir)):
            with io.open(file, 'r+', encoding='utf-8') as f:
                contents = self.find_and_replace_image_references_by_sha(f.read())
                f.seek(0)
                f.truncate()
                f.write(contents)

    def generate_bundle_annotations(self):
        """Create an annotations YAML file for the bundle, using info extracted from operator's
        package YAML
        """
        annotations_file = '{}/metadata/annotations.yaml'.format(self.bundle_clone_path)
        exectools.cmd_assert('mkdir -p {}'.format(os.path.dirname(annotations_file)))

        with io.open(annotations_file, 'w', encoding='utf-8') as writer:
            writer.write(yaml.dump({'annotations': self.operator_framework_tags}))

    def generate_bundle_dockerfile(self):
        """Create a Dockerfile with instructions to build the bundle container and a set of LABELs
        that allow the bundle to publish its manifests on operator's behalf.
        """
        operator_df = DockerfileParser('{}/Dockerfile'.format(self.operator_clone_path))
        bundle_df = DockerfileParser('{}/Dockerfile'.format(self.bundle_clone_path))

        bundle_df.content = 'FROM scratch\nCOPY ./manifests /manifests\nCOPY ./metadata /metadata'
        bundle_df.labels = operator_df.labels
        bundle_df.labels['com.redhat.component'] = self.bundle_brew_component
        bundle_df.labels['com.redhat.delivery.appregistry'] = False
        bundle_df.labels['name'] = self.get_bundle_image_name()
        bundle_df.labels['version'] = '{}.{}'.format(
            operator_df.labels['version'],
            operator_df.labels['release']
        )
        bundle_df.labels = {
            **bundle_df.labels,
            **self.redhat_delivery_tags,
            **self.operator_framework_tags
        }
        del(bundle_df.labels['release'])

    def create_container_yaml(self):
        """Use container.yaml to disable unnecessary multiarch
        """
        filename = '{}/container.yaml'.format(self.bundle_clone_path)
        with io.open(filename, 'w', encoding='utf-8') as writer:
            writer.write('# metadata containers are not functional and do not need to be multiarch')
            writer.write('\n\n')
            writer.write(yaml.dump({
                'platforms': {'only': ['x86_64']},
                'operator_manifests': {'manifests_dir': 'manifests'},
            }))

    def commit_and_push_bundle(self, commit_msg):
        """Try to commit and push bundle distgit repository if there were any content changes.

        :param string commit_msg: Commit message
        :return bool True if new changes were committed and pushed, False otherwise
        """
        with pushd.Dir(self.bundle_clone_path):
            try:
                exectools.cmd_assert('git add .')
                exectools.cmd_assert('rhpkg{}commit -m "{}"'.format(self.rhpkg_opts, commit_msg))
                rc, out, err = exectools.cmd_gather('rhpkg{}push'.format(self.rhpkg_opts))
                return True
            except Exception:
                return False  # Bundle repository might be already up-to-date, nothing new to commit

    def trigger_bundle_container_build(self):
        """Ask brew for a container-build of operator's bundle

        :return bool True if brew task was successfully created, False otherwise
        """
        with pushd.Dir(self.bundle_clone_path):
            rc, out, err = exectools.cmd_gather(
                'rhpkg{}container-build --nowait --target {}'.format(self.rhpkg_opts, self.target)
            )

        if rc != 0:
            msg = 'Unable to create brew task: rc={} out={} err={}'.format(rc, out, err)
            self.runtime.logger.info(msg)
            return False

        self.task_url = re.search(r'Task info:\s(.+)', out).group(1)
        self.task_id = re.search(r'Created task:\s(\d+)', out).group(1)
        return True

    def watch_bundle_container_build(self):
        """Log brew task URL and eventual task states until task completion (or failure)

        :return bool True if brew task was successfully completed, False otherwise
        """
        self.runtime.logger.info('Build running: {}'.format(self.task_url))
        error = brew.watch_task(
            self.runtime.build_retrying_koji_client(),
            self.runtime.logger.info,
            self.task_id,
            threading.Event()
        )
        if error:
            self.runtime.logger.info(error)
            return False
        return True

    def find_and_replace_image_references_by_sha(self, contents):
        """Search image references (<registry>/<image>:<tag>) on given contents (usually YAML),
        replace them with corresponding (<registry>/<image>@<sha>) and collect such replacements to
        list them as "relatedImages" under "spec" section of contents (should it exist)

        :param string contents: File contents that potentially contains image references
        :return string: Same contents, with aforementioned modifications applied
        """
        found_images = {}

        def collect_replaced_image(match):
            image = '{}/{}@{}'.format(
                'registry.redhat.io',  # hardcoded until appregistry is dead
                match.group(1).replace('openshift/', 'openshift4/'),
                self.fetch_image_sha('{}:{}'.format(match.group(1), match.group(2)))
            )
            key = u'{}'.format(re.search(r'([^\/]+)\/(.+)', match.group(1)).group(2))
            found_images[key] = u'{}'.format(image)
            return image

        new_contents = re.sub(
            r'{}\/([^:]+):([^\'"\s]+)'.format(self.operator_csv_config['registry']),
            collect_replaced_image,
            contents,
            flags=re.MULTILINE
        )

        return self.append_related_images_spec(new_contents, found_images)

    def fetch_image_sha(self, image):
        """Get corresponding SHA of given image (using `oc image info`)

        OCP 4.3+ supports "manifest-lists", which is a SHA that doesn't represent an actual image,
        but a list of images per architecture instead. OCP 4.3+ is smart enough to read that list
        and pick the correct architecture.

        Unfortunately, OCP 4.2 is multi-arch and does not support manifest-lists. It is still unclear
        how we want to handle this case on this new bundle workflow. Previously it was easy to simply
        generate manifests for all arches in a single run, since all manifests were living together
        under the same branch.

        Possible solutions:
        * Build multiple sets of operator bundles, one per architecture
        Caveats: More manual work, and maybe more advisories, since I'm not sure if Errata Tool will
        let us attach multiple builds of the same brew component to a single advisory.

        * Have a multi-arch build of the bundle container
        Caveats: Not sure if appregistry/IIB will know what to do with that, publishing each arch in
        a different channel

        For now, simply assuming x86_64 (aka amd64 in golang land)

        :param string image: Image reference (format: <registry>/<image>:<tag>)
        :return string: SHA of corresponding <tag> (format: sha256:a1b2c3d4...)
        """
        registry = self.runtime.group_config.urls.brew_image_host.rstrip('/')
        ns = self.runtime.group_config.urls.brew_image_namespace
        image = '{}/{}'.format(ns, image.replace('/', '-')) if ns else image

        pull_spec = '{}/{}'.format(registry, image)
        cmd = 'oc image info --filter-by-os=linux/amd64 -o json {}'.format(pull_spec)
        try:
            out, err = exectools.cmd_assert(cmd, retries=3)
        except:
            self.runtime.logger.error(f'Unable to find image from CSV: {pull_spec}. Image may have failed to build after CSV rebase.')
            raise

        if self.runtime.group_config.operator_image_ref_mode == 'manifest-list':
            return json.loads(out)['listDigest']

        # @TODO: decide how to handle 4.2 multi-arch. hardcoding amd64 for now
        return json.loads(out)['contentDigest']

    def append_related_images_spec(self, contents, images):
        """Create a new section under contents' "spec" called "relatedImages", listing all given
        images in the following format:

        spec:
          relatedImages:
            - name: image-a
              image: registry/image-a@sha256:....
            - name: image-b
              image: registry/image-b@sha256:....

        If list of images is empty or "spec" section is not found, return contents as-is

        :param string contents: File contents that potentially contains a "spec" section
        :param list images: List of image info dictionaries (format: [{"name": "...", "image": "..."}])
        :return string: Given contents with aforementioned modifications applied
        """
        if not images:
            return contents

        related_images = [
            '    - name: {}\n      image: {}'.format(name, image)
            for name, image in images.items()
        ]
        related_images.sort()

        return re.sub(
            r'^spec:\n',
            'spec:\n  relatedImages:\n{}\n'.format('\n'.join(related_images)),
            contents,
            flags=re.MULTILINE
        )

    @property
    def brew_session(self):
        if not hasattr(self, '_brew_session'):
            self._brew_session = brew.koji.ClientSession(
                self.runtime.group_config.urls.brewhub,
            )
        return self._brew_session

    @property
    def operator_name(self):
        return self.operator_repo_name.split('/')[-1]

    @property
    def operator_csv_config(self):
        return self.runtime.image_map[self.operator_name].config['update-csv']

    @property
    def operator_clone_path(self):
        return '{}/distgits/{}'.format(self.runtime.working_dir, self.operator_repo_name)

    @property
    def operator_manifests_dir(self):
        return '{}/{}'.format(
            self.operator_clone_path,
            self.operator_csv_config['manifests-dir'].rstrip('/')
        )

    @property
    def operator_bundle_dir(self):
        return '{}/{}'.format(
            self.operator_manifests_dir,
            self.operator_csv_config['bundle-dir'].rstrip('/')
        )

    @property
    def operator_brew_component(self):
        config = self.runtime.image_map[self.operator_name].config

        if 'distgit' in config and 'component' in config['distgit']:
            return config['distgit']['component']

        return '{}-container'.format(self.operator_name)

    @property
    def bundle_name(self):
        return '{}-bundle'.format(self.operator_name)

    @property
    def bundle_repo_name(self):
        return '{}-bundle'.format(self.operator_repo_name)

    @property
    def bundle_clone_path(self):
        return '{}-bundle'.format(self.operator_clone_path)

    @property
    def bundle_manifests_dir(self):
        return '{}/manifests'.format(self.bundle_clone_path)

    @property
    def bundle_brew_component(self):
        config = self.runtime.image_map[self.operator_name].config

        if 'distgit' in config and 'bundle_component' in config['distgit']:
            return config['distgit']['bundle_component']

        return self.operator_brew_component.replace('-container', '-metadata-container')

    @property
    def branch(self):
        return self.runtime.group_config.branch.format(**self.runtime.group_config.vars)

    @property
    def rhpkg_opts(self):
        opts = self.runtime.rhpkg_config
        if hasattr(self.runtime, 'user') and self.runtime.user is not None:
            opts += ' --user {} '.format(self.runtime.user)
        return opts

    @property
    def list_of_manifest_files_to_be_copied(self):
        files = glob.glob('{}/*'.format(self.operator_bundle_dir))
        if not files:
            # 4.1 channel in package YAML is "preview" or "stable", but directory name is "4.1"
            files = glob.glob('{}/{}/*'.format(
                self.operator_manifests_dir,
                '{MAJOR}.{MINOR}'.format(**self.runtime.group_config.vars)
            ))
        return files

    @property
    def redhat_delivery_tags(self):
        versions = '=v{MAJOR}.{MINOR}'.format(**self.runtime.group_config.vars)

        return {
            # 'com.redhat.delivery.backport': 'true',
            'com.redhat.delivery.operator.bundle': 'true',
            'com.redhat.openshift.versions': versions,
        }

    @property
    def operator_framework_tags(self):
        return {
            'operators.operatorframework.io.bundle.channel.default.v1': self.channel,
            'operators.operatorframework.io.bundle.channels.v1': self.channel,
            'operators.operatorframework.io.bundle.manifests.v1': 'manifests/',
            'operators.operatorframework.io.bundle.mediatype.v1': 'registry+v1',
            'operators.operatorframework.io.bundle.metadata.v1': 'metadata/',
            'operators.operatorframework.io.bundle.package.v1': self.package,
        }

    @property
    def target(self):
        return self.runtime.get_default_candidate_brew_tag() or '{}-candidate'.format(self.branch)
