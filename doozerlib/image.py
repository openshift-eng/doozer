from __future__ import absolute_import, print_function, unicode_literals
from doozerlib.distgit import pull_image
from doozerlib.metadata import Metadata
from doozerlib.model import Missing, Model
from doozerlib.pushd import Dir
from doozerlib import assertion, exectools
from dockerfile_parse import DockerfileParser
import hashlib
import pathlib
import os
import json

class ImageMetadata(Metadata):

    def __init__(self, runtime, data_obj):
        super(ImageMetadata, self).__init__('image', runtime, data_obj)
        self.image_name = self.config.name
        self.required = self.config.get('required', False)
        self.image_name_short = self.image_name.split('/')[-1]
        self.parent = None
        self.children = []
        dependents = self.config.get('dependents', [])
        for d in dependents:
            self.children.append(self.runtime.late_resolve_image(d, add=True))

    def is_ancestor(self, image):
        if isinstance(image, Metadata):
            image = image.distgit_key

        parent = self.parent
        while parent:
            if parent.distgit_key == image:
                return True
            parent = parent.parent

        return False

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
        self.children.append(child)

    @property
    def base_only(self):
        """
        Some images are marked base-only.  Return the flag from the config file
        if present.
        """
        return self.config.base_only

    def get_latest_build_info(self):

        """
        Queries brew to determine the most recently built release of the component
        associated with this image. This method does not rely on the "release"
        label needing to be present in the Dockerfile.

        :return: A tuple: (component name, version, release); e.g. ("registry-console-docker", "v3.6.173.0.75", "1")
        """

        component_name = self.get_component_name()

        rc, stdout, stderr = exectools.cmd_gather(["brew", "latest-build", self.brew_tag(), component_name])

        assertion.success(rc, "Unable to search brew builds: %s" % stderr)

        latest = stdout.strip().splitlines()[-1].split(' ')[0]

        if not latest.startswith(component_name):
            # If no builds found, `brew latest-build` output will appear as:
            # Build                                     Tag                   Built by
            # ----------------------------------------  --------------------  ----------------
            raise IOError("No builds detected for %s using tag: %s" % (self.qualified_name, self.brew_tag()))

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
            archive_all_results_html_path = archive_commit_results_path.joinpath(all_html) # /<archive-dir>/<dg-key>/<hash>/all_results.html
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
                    runs.append('mkdir -p ' + path) # Also pass into RUN so they have the correct working dir
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
                run_docker_cov(f'cov-analyze  --dir=/cov "--wait-for-license" "-co" "ASSERT_SIDE_EFFECT:macro_name_lacks:^assert_(return|se)\$" "-co" "BAD_FREE:allow_first_field:true" "--include-java" "--fb-max-mem=4096" "--security" "--concurrency" --allow-unmerged-emits')
            else:
                self.logger.info('covscan analysis already exists -- skipping this step')

            all_results_js, _ = run_docker_cov(f'cov-format-errors --json-output-v2 /dev/stdout --dir=/cov')
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
