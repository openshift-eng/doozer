from __future__ import absolute_import, print_function, unicode_literals

import os
import hashlib
import pathlib
import json

from dockerfile_parse import DockerfileParser
from doozerlib import exectools
from .pushd import Dir

COVSCAN_ALL_JS_FILENAME = 'all_results.js'
COVSCAN_DIFF_JS_FILENAME = 'diff_results.js'
COVSCAN_ALL_HTML_FILENAME = 'all_results.html'
COVSCAN_DIFF_HTML_FILENAME = 'diff_results.html'
COVSCAN_WAIVED_FILENAME = 'waived.flag'


class CoverityContext(object):

    def __init__(self, image, dg_commit_hash, result_archive, repo_type='unsigned',
                 local_repo_rhel_7=[], local_repo_rhel_8=[], force_analysis=False,
                 ignore_waived=False):
        self.image = image
        self.dg_commit_hash = dg_commit_hash
        self.result_archive_path = pathlib.Path(result_archive)
        self.tmp_path = self.result_archive_path.joinpath('tmp')
        self.tmp_path.mkdir(exist_ok=True, parents=True)
        self.dg_archive_path = self.result_archive_path.joinpath(image.distgit_key)
        self.dg_archive_path.mkdir(parents=True, exist_ok=True)  # /<archive-dir>/<dg-key>
        self.archive_commit_results_path = self.dg_archive_path.joinpath(dg_commit_hash)  # /<archive-dir>/<dg-key>/<hash>
        self.repo_type = repo_type
        self.local_repo_rhel_7 = local_repo_rhel_7
        self.local_repo_rhel_8 = local_repo_rhel_8
        self.logger = image.logger
        self.runtime = image.runtime
        self.force_analysis = force_analysis
        self.ignore_waived = ignore_waived

        # Podman is going to create a significant amount of container image data
        # Make sure there is plenty of space. Override TMPDIR, because podman
        # uses this environment variable.
        self.podman_tmpdir = self.tmp_path.joinpath('podman')
        self.podman_tmpdir.mkdir(exist_ok=True)
        self.podman_cmd = 'sudo podman '
        self.podman_env = {
            'TMPDIR': str(self.podman_tmpdir)
        }

        # Runtime coverity scanning output directory; each stage will have an entry beneath this.
        self.cov_root_path: pathlib.Path = image.distgit_repo().dg_path.joinpath('cov')
        self.cov_root_path.mkdir(exist_ok=True, parents=True)
        self.dg_path = image.distgit_repo().dg_path

    def find_nearest_waived_cov_root_path(self) -> pathlib.Path:
        # Search backwards through commit history; try to find a has for this distgit that has been scanned before
        if self.ignore_waived:
            return None

        with Dir(self.dg_path):
            commit_log, _ = exectools.cmd_assert("git --no-pager log --pretty='%H' -1000")
            for old_commit in commit_log.split()[1:]:
                # Check for waived hash for in the commit history. The second check is for legacy style of waiver where we only checked
                # the first Dockerfile stage.
                cov_root_path = self.dg_archive_path.joinpath(old_commit)
                if cov_root_path.joinpath('1', COVSCAN_WAIVED_FILENAME).exists() or cov_root_path.joinpath(COVSCAN_WAIVED_FILENAME):
                    return cov_root_path
        return None

    def get_nearest_waived_cov_path(self, nearest_waived_cov_root_path: pathlib.Path, stage_number) -> pathlib.Path:
        if not nearest_waived_cov_root_path:
            return None

        if nearest_waived_cov_root_path.joinpath('1').exists():
            # If this is the new style where we scan every stage
            stage_cov_path = nearest_waived_cov_root_path.joinpath(str(stage_number))
            if stage_cov_path.exists():
                return stage_cov_path
        else:
            # This is a legacy run where only the first stage was scanned
            if stage_number == 1:
                # The root path will correspond to the new stage cov_path results.
                # Other stages will have nothing to diff against.
                return nearest_waived_cov_root_path

    def container_stage_cov_path(self, stage_number, filename=None):
        """
        Where coverity output should go for a given stage within the context
        of the coverity runner container.
        """
        path = pathlib.Path('/cov').joinpath(str(stage_number))
        if filename:
            path = path.joinpath(filename)
        return path

    def host_stage_cov_path(self, stage_number, filename=None):
        """
        Where coverity output should be on the host filesystem.
        """
        path = self.cov_root_path.joinpath(str(stage_number))
        if filename:
            path = path.joinpath(filename)
        return path

    def get_stage_results_waive_path(self, stage_number):
        """
        The flag that indicates archived results have been waived for a given stage
        """
        return self.get_stage_results_path(stage_number=stage_number).joinpath(COVSCAN_WAIVED_FILENAME)

    def get_results_done_flag_path(self):
        """
        A path to a flag file in the archive directory. If it exists for a given commit hash, then
        we successfully completed scanning on the commit.
        """
        return self.archive_commit_results_path.joinpath('done.flag')

    def get_stage_results_path(self, stage_number):
        """
        The results directory for this images/commit/stage_number.
        """
        p = self.archive_commit_results_path.joinpath(str(stage_number))
        p.mkdir(exist_ok=True, parents=True)
        return p

    def are_results_done(self):
        """
        Returns whether this commit has complete scan results in the archive.
        """
        return not self.force_analysis and self.get_results_done_flag_path().exists()

    def mark_results_done(self):
        """
        Call to mark the results for this commit complete.
        """
        self.get_results_done_flag_path().touch(exist_ok=True)

    def parent_repo_injection_info(self) -> (str, str):
        """
        Parent images built for coverity scanning must install the coverity tools. For
        speed purposes, the repos for these tools are typically built by the user and
        the paths passed in on the command line.
        If they are passed in on the command line, we need to create repo files in a
        derivative of the parent image and mount volumes from the host to expose
        the repos.
        :returns: (string to inject into parent derivative Dockerfile, mounts to use when building that dockerfile)
        """
        make_image_repo_files = ''
        vol_mount_arg = ''
        if self.local_repo_rhel_7:
            for idx, lr in enumerate(self.local_repo_rhel_7):
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
                vol_mount_arg += f' -v {lr}:/covscan_local_{idx}_rhel_7:z'
        else:
            make_image_repo_files += 'RUN if cat /etc/redhat-release | grep "release 7"; then curl -k https://cov01.lab.eng.brq.redhat.com/coverity/install/covscan/covscan-rhel-7.repo --output /etc/yum.repos.d/covscan.repo; fi\n'

        if self.local_repo_rhel_8:
            for idx, lr in enumerate(self.local_repo_rhel_8):
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
                vol_mount_arg += f' -v {lr}:/covscan_local_{idx}_rhel_8:z'
        else:
            make_image_repo_files += 'RUN if cat /etc/redhat-release | grep "release 8"; then curl -k https://cov01.lab.eng.brq.redhat.com/coverity/install/covscan/covscan-rhel-8.repo --output /etc/yum.repos.d/covscan.repo; fi\n'

        return make_image_repo_files, vol_mount_arg


def _covscan_prepare_parent(cc: CoverityContext, parent_image_name, parent_tag) -> bool:
    """
    Builds an image for the specified parent image and layers coverity tools on top of it.
    This image is called the parent image derivative and will be used during the actual
    coverity scan.
    :param cc: The coverity scan context
    :param parent_image_name: The name of the image as found in the distgit Dockerfile
    :param parent_tag: A string with which to tag the image after building.
    :return: Returns True if the image is built successfully
    """
    dg_path = cc.dg_path
    rc, _, _ = exectools.cmd_gather(f'{cc.podman_cmd} inspect {parent_tag}', set_env=cc.podman_env)
    if rc != 0:
        cc.logger.info(f'Creating parent image derivative with covscan tools installed as {parent_tag} for parent {parent_image_name}')
        df_parent_path = dg_path.joinpath(f'Dockerfile.{parent_tag}')

        repo_injection_lines, mount_args = cc.parent_repo_injection_info()

        rhel_repo_gen_sh = '_rhel_repo_gen.sh'
        with dg_path.joinpath(rhel_repo_gen_sh).open(mode='w') as f:
            f.write('''
#!/bin/sh
set -o xtrace

if cat /etc/redhat-release | grep "release 8"; then
    cp /tmp/oit.repo /etc/yum.repos.d/oit.repo

    # For an el8 layer, make sure baseos & appstream are
    # available for tools like python to install.
    cat <<EOF > /etc/yum.repos.d/el8.repo
[rhel-8-appstream-rpms-x86_64]
baseurl = http://rhsm-pulp.corp.redhat.com/content/dist/rhel8/8/x86_64/appstream/os/
enabled = 1
name = rhel-8-appstream-rpms-x86_64
gpgcheck = 0
gpgkey = file:///etc/pki/rpm-gpg/RPM-GPG-KEY-redhat-release

[rhel-8-baseos-rpms-x86_64]
baseurl = http://rhsm-pulp.corp.redhat.com/content/dist/rhel8/8/x86_64/baseos/os/
enabled = 1
name = rhel-8-baseos-rpms-x86_64
gpgcheck = 0
gpgkey = file:///etc/pki/rpm-gpg/RPM-GPG-KEY-redhat-release
EOF

    # Enable epel for csmock
    curl https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm --output epel8.rpm
    yum -y install epel8.rpm
else
    # For rhel-7, just enable the basic rhel repos so that we
    # can install python and other dependencies.
    cat <<EOF > /etc/yum.repos.d/el7.repo
[rhel-server-rpms-x86_64]
baseurl = http://rhsm-pulp.corp.redhat.com/content/dist/rhel/server/7/7Server/x86_64/os/
enabled = 1
name = rhel-server-rpms-x86_64
gpgcheck = 0
gpgkey = file:///etc/pki/rpm-gpg/RPM-GPG-KEY-redhat-release

[rhel-server-optional-rpms-x86_64]
baseurl = http://rhsm-pulp.corp.redhat.com/content/dist/rhel/server/7/7Server/x86_64/optional/os/
enabled = 0
name = rhel-server-optional-rpms-x86_64
gpgcheck = 0
gpgkey = file:///etc/pki/rpm-gpg/RPM-GPG-KEY-redhat-release

[rhel-server-extras-rpms-x86_64]
baseurl = http://rhsm-pulp.corp.redhat.com/content/dist/rhel/server/7/7Server/x86_64/extras/os/
enabled = 0
name = rhel-server-extras-rpms-x86_64
gpgcheck = 0
gpgkey = file:///etc/pki/rpm-gpg/RPM-GPG-KEY-redhat-release
EOF

    # Enable epel for csmock
    curl https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm --output epel7.rpm
    yum -y install epel7.rpm
fi
''')

        with df_parent_path.open(mode='w+', encoding='utf-8') as df_parent_out:
            parent_image_url = parent_image_name
            if 'redhat.registry' not in parent_image_name:
                parent_image_url = cc.runtime.resolve_brew_image_url(parent_image_name)

            df_parent_out.write(f'''
FROM {parent_image_url}
LABEL DOOZER_COVSCAN_GROUP_PARENT={cc.runtime.group_config.name}
USER 0

# Add typical build repos to the image, but don't add to /etc/yum.repos.d
# until we know whether we are on el7 or el8. As of 4.8, repos are only
# appropriate for el8, so this repo file should only be installed in el8.
ADD .oit/{cc.repo_type}.repo /tmp/oit.repo

# Install covscan repos
{repo_injection_lines}

# Act on oit.repo and enable rhel repos
ADD {rhel_repo_gen_sh} .
RUN chmod +x {rhel_repo_gen_sh} && ./{rhel_repo_gen_sh}

RUN yum install -y python36

# Certs necessary to install from covscan repos
RUN curl -k https://password.corp.redhat.com/RH-IT-Root-CA.crt --output /etc/pki/ca-trust/source/anchors/RH-IT-Root-CA.crt
RUN curl -k https://engineering.redhat.com/Eng-CA.crt --output /etc/pki/ca-trust/source/anchors/Eng-CA.crt
RUN update-ca-trust
RUN update-ca-trust enable

RUN yum install -y cov-sa csmock csmock-plugin-coverity csdiff
''')
            df_parent_out.write('ENV PATH=/opt/coverity/bin:${PATH}\n')  # Ensure coverity is in the path

        # This will have prepared a parent image we can use during the actual covscan Dockerfile build
        rc, stdout, stderr = exectools.cmd_gather(f'{cc.podman_cmd} build {mount_args} -t {parent_tag} -f {str(df_parent_path)} {str(dg_path)}', set_env=cc.podman_env)
        cc.logger.info(f'''Output from covscan build for {cc.image.distgit_key}
stdout: {stdout}
stderr: {stderr}
''')
        if rc != 0:
            cc.logger.error(f'Error preparing builder image derivative {parent_tag} from {parent_image_name} with {str(df_parent_path)}')
            # TODO: log this as a record and make sure the pipeline warns artist
            return False

    else:
        cc.logger.info(f'Parent image already exists with covscan tools {parent_tag} for {parent_image_name}')

    return True


def run_covscan(cc: CoverityContext) -> bool:

    dg_path = cc.dg_path
    with Dir(dg_path):

        dockerfile_path = dg_path.joinpath('Dockerfile')
        if not dockerfile_path.exists():
            cc.logger.error('Dockerfile does not exist in distgit; not rebased yet?')
            return False

        dfp = DockerfileParser(str(dockerfile_path))

        if cc.are_results_done():
            cc.logger.info(f'Scan results already exist for {cc.dg_commit_hash}; skipping scan')
            # Even if it is complete, write a record for Jenkins so that results can be sent to prodsec.
            for i in range(len(dfp.parent_images)):
                records_results(cc, stage_number=i + 1, waived_cov_path_root=None, write_only=True)
            return True

        def compute_parent_tag(parent_image_name):
            parent_sig = hashlib.md5(parent_image_name.encode("utf-8")).hexdigest()
            return f'parent-{parent_sig}'

        covscan_df = dg_path.joinpath('Dockerfile.covscan')

        with covscan_df.open(mode='w+') as df_out:

            df_line = 0
            stage_number = 0
            for entry in dfp.structure:

                def append_analysis(stage_number):
                    # We will have monitored compilation processes, but we need to scan for non-compiled code
                    # like python / nodejs.
                    # cov-capture will search for files like .js, typescript, python in the source directory;
                    # cov-analyze will then search for issues within those non-compiled files.
                    # https://community.synopsys.com/s/question/0D52H000054zcvZSAQ/i-would-like-to-know-the-coverity-static-analysis-process-for-node-js-could-you-please-provide-me-the-sample-steps-to-run-coverity-for-node-js

                    # Why cov-manage-emit?
                    # coverity requires a consistent hostname for each tool run. podman does not allow the hostname to be
                    # set and it varies over the course of the build. That is why we reset the hostname in emit before each
                    # tool run.

                    container_stage_cov_dir = str(cc.container_stage_cov_path(stage_number))

                    analysis_script_name = f'_gen_{cc.image.image_name_short}_stage_{stage_number}_analysis.sh'
                    with open(dg_path.joinpath(analysis_script_name), mode='w+', encoding='utf-8') as sh:
                        sh.write(f'''
#!/bin/sh
set -o xtrace
set -eo pipefail

if [[ -f "{container_stage_cov_dir}/all_results.js" ]]; then
    echo "Results have already been analyzed for this this stage -- found all_results.js; skipping analysis"
    exit 0
fi

if [[ "{stage_number}" == "1" ]]; then
    # hostname changes between steps in the Dockerfile; reset to current before running coverity tools.
    # use || true because it is possible nothing has been emitted before this step
    cov-manage-emit --dir={container_stage_cov_dir} reset-host-name || true
    echo "Running un-compiled source search as hostname: $(hostname)"
    timeout 3h cov-capture --dir {container_stage_cov_dir} --source-dir /covscan-src || echo "Error running source detection"
fi

if ls {container_stage_cov_dir}/emit/*/config; then
    echo "Running analysis phase as hostname: $(hostname)"
    # hostname changes between steps in the Dockerfile; reset to current before running coverity tools.
    cov-manage-emit --dir={container_stage_cov_dir} reset-host-name || true
    if timeout 3h cov-analyze  --dir={container_stage_cov_dir} "--wait-for-license" "-co" "ASSERT_SIDE_EFFECT:macro_name_lacks:^assert_(return|se)\\$" "-co" "BAD_FREE:allow_first_field:true" "--include-java" "--fb-max-mem=4096" "--all" "--security" "--concurrency" --allow-unmerged-emits > /tmp/analysis.txt 2>&1 ; then
        echo "Analysis completed successfully"
        cat /tmp/analysis.txt
    else
        # In some cases, no translation units were emitted and analyze will exit with an error; ignore that error
        # if it is because nothing was emitted.
        cat /tmp/analysis.txt
        if cat /tmp/analysis.txt | grep "contains no translation units"; then
            echo "Nothing was emitted; ignoring analyze failure."
            exit 0
        else
            echo "Analysis failed for unknown reason!"
            exit 1
        fi
    fi
    cov-format-errors --json-output-v2 /dev/stdout --dir={container_stage_cov_dir} > {container_stage_cov_dir}/{COVSCAN_ALL_JS_FILENAME}
else
    echo "No units have been emitted for analysis by this stage; skipping analysis"
fi
''')
                    df_out.write(f'''
ADD {analysis_script_name} /
RUN chmod +x /{analysis_script_name}
# Finally, run the analysis step script.
# Route stderr to stdout so everything is in one stream; otherwise, it is hard to correlate a command with its stderr.
RUN /{analysis_script_name} 2>&1
''')

                    # Before running cov-analyze, make sure that all_js doesn't exist (i.e. we haven't already run it
                    # in this workspace AND summary.txt exist (i.e. at least one item in this stage emitted results).
                    df_out.write(f'''
# Dockerfile steps run as root; chang permissions back to doozer user before leaving stage
RUN chown -R {os.getuid()}:{os.getgid()} {container_stage_cov_dir}
''')

                df_line += 1
                content = entry['content']
                instruction = entry['instruction'].upper()

                if instruction == 'USER':
                    # Stay as root
                    continue

                if instruction == 'FROM':
                    stage_number += 1

                    if stage_number > 1:
                        # We are about to transition stages, do the analysis first.
                        append_analysis(stage_number - 1)

                    image_name_components = content.split()  # [ 'FROM', image-name, (possible 'AS', ...) ]
                    image_name = image_name_components[1]
                    parent_tag = compute_parent_tag(image_name)
                    if not _covscan_prepare_parent(cc, image_name, parent_tag):
                        return False

                    image_name_components[1] = parent_tag
                    df_out.write(' '.join(image_name_components) + '\n')
                    # Label these images so we can find a delete them later
                    df_out.write(f'LABEL DOOZER_COVSCAN_RUNNER={cc.runtime.group_config.name}\n')
                    df_out.write(f'LABEL DOOZER_COVSCAN_COMPONENT={cc.image.distgit_key}\n')
                    df_out.write('ENTRYPOINT []\n')  # Ensure all invocations use /bin/sh -c, the default docker entrypoint
                    df_out.write('USER 0\n')  # Just make sure all images are consistent

                    # Each stage will have its own cov output directory
                    df_out.write(f'''
RUN mkdir -p {cc.container_stage_cov_path(stage_number)}
# If we are reusing a workspace, coverity cannot pick up where it left off; clear anything already emitted
RUN rm -rf {cc.container_stage_cov_path(stage_number)}/emit
''')

                    # For each new stage, we also need to make sure we have the appropriate repos enabled for this image
                    df_out.write(f'''
# Ensure that the build process can access the same RPMs that the build can during a brew build
RUN curl {cc.image.cgit_url(".oit/" + cc.repo_type + ".repo")} --output /etc/yum.repos.d/oit.repo 2>&1
''')
                    continue

                if instruction in ('ENTRYPOINT', 'CMD'):
                    df_out.write(f'# Disabling: {content}')
                    continue

                if instruction == 'RUN':
                    container_stage_cov_dir = str(cc.container_stage_cov_path(stage_number))

                    # For RUN commands, we need to execute the command under the watchful eye of coverity
                    # tools. Create a batch file that will wrap the command
                    command_to_run = content.strip()[4:]  # Remove 'RUN '
                    temp_script_name = f'_gen_{cc.image.image_name_short}_stage_{stage_number}_line_{df_line}.sh'
                    with open(dg_path.joinpath(temp_script_name), mode='w+', encoding='utf-8') as sh:
                        sh.write(f'''
#!/bin/sh
set -o xtrace
set -eo pipefail
echo "Running build as hostname: $(hostname)"
{command_to_run}
''')
                    df_out.write(f'''
ADD {temp_script_name} .
RUN chmod +x {temp_script_name}
# Finally, run the script while coverity is watching. If there is already a summary file, assume we have already run
# the build in this working directory.
# The hostname changes with each run, so reset-host-name before cov-build.
# Route stderr to stdout so everything is in one stream; otherwise, it is hard to tell which command failed.
RUN cov-manage-emit --dir={container_stage_cov_dir} reset-host-name; timeout 3h cov-build --dir={container_stage_cov_dir} ./{temp_script_name} 2>&1
''')
                else:  # e.g. COPY, ENV, WORKDIR...
                    # Just pass it straight through to the covscan Dockerfile
                    df_out.write(f'{content}\n')

            append_analysis(stage_number)

        # The dockerfile which will run the coverity builds and analysis for each stage has been created.
        # Now, run the build (and execute those steps). The output will be to <cov_path>/<stage_number>
        run_tag = cc.image.image_name_short
        rc, stdout, stderr = exectools.cmd_gather(
            f'{cc.podman_cmd} build -v {str(cc.cov_root_path)}:/cov:z -v {str(dg_path)}:/covscan-src:z -t {run_tag} -f {str(covscan_df)} {str(dg_path)}',
            set_env=cc.podman_env)
        cc.logger.info(f'''Output from covscan build for {cc.image.distgit_key}
stdout: {stdout}
stderr: {stderr}

''')
        if rc != 0:
            cc.logger.error(f'Error running covscan build for {cc.image.distgit_key} ({str(covscan_df)})')
            # TODO: log this as a record and make sure the pipeline warns artist
            return False

        # For each stage, let's now compute diffs & store results
        waived_cov_path_root = cc.find_nearest_waived_cov_root_path()
        for i in range(len(dfp.parent_images)):
            records_results(cc, stage_number=i + 1, waived_cov_path_root=waived_cov_path_root)

    return True


def records_results(cc: CoverityContext, stage_number, waived_cov_path_root=None, write_only=False):
    dest_result_path = cc.get_stage_results_path(stage_number)
    dest_all_js_path = dest_result_path.joinpath(COVSCAN_ALL_JS_FILENAME)
    dest_diff_js_path = dest_result_path.joinpath(COVSCAN_DIFF_JS_FILENAME)

    dest_all_results_html_path = dest_result_path.joinpath(COVSCAN_ALL_HTML_FILENAME)
    dest_diff_results_html_path = dest_result_path.joinpath(COVSCAN_DIFF_HTML_FILENAME)

    def write_record():
        owners = ",".join(cc.image.config.owners or [])
        all_json = json.loads(dest_all_js_path.read_text(encoding='utf-8'))
        diff_json = json.loads(dest_diff_js_path.read_text(encoding='utf-8'))
        diff_count = len(diff_json.get('issues', []))
        all_count = len(all_json.get('issues', []))
        host_stage_waived_flag_path = cc.get_stage_results_waive_path(stage_number)
        cc.image.runtime.add_record('covscan',
                                    distgit=cc.image.qualified_name,
                                    distgit_key=cc.image.distgit_key,
                                    commit_results_path=str(dest_result_path),
                                    all_results_js_path=str(dest_all_js_path),
                                    all_results_html_path=str(dest_all_results_html_path),
                                    diff_results_js_path=str(dest_diff_js_path),
                                    diff_results_html_path=str(dest_diff_results_html_path),
                                    diff_count=str(diff_count),
                                    all_count=str(all_count),
                                    stage_number=str(stage_number),
                                    waive_path=str(host_stage_waived_flag_path),
                                    waived=str(host_stage_waived_flag_path.exists()).lower(),
                                    owners=owners,
                                    image=cc.image.config.name,
                                    commit_hash=cc.dg_commit_hash)

    if write_only:
        if dest_all_js_path.exists():
            # Results are already computed and in results; just write the record
            write_record()
        else:
            # This stage was analyzed previously, but had no results (i.e. it did not build code).
            pass
        return

    host_stage_cov_path = cc.host_stage_cov_path(stage_number)

    source_all_js_path: pathlib.Path = host_stage_cov_path.joinpath(COVSCAN_ALL_JS_FILENAME)
    if not source_all_js_path.exists():
        # No results for this stage; nothing to report
        return

    source_all_js = source_all_js_path.read_text(encoding='utf-8')
    if not source_all_js.strip():
        return

    dest_all_js_path.write_text(source_all_js, encoding='utf-8')

    source_summary_path = host_stage_cov_path.joinpath('output', 'summary.txt')
    if source_summary_path.exists():
        dest_summary_path = dest_result_path.joinpath('summary.txt')
        # The only reason there would not be a summary.txt is if we are testing with fake data.
        # If we find it, copy it into the results directory.
        dest_summary_path.write_text(source_summary_path.read_text(encoding='utf-8'), encoding='utf-8')

    source_buildlog_path = host_stage_cov_path.joinpath('build-log.txt')
    if source_buildlog_path.exists():
        dest_buildlog_path = dest_result_path.joinpath('build-log.txt')
        # The only reason there would not be a build-log.txt is if we are testing with fake data.
        # If we find it, copy it into the results directory.
        dest_buildlog_path.write_text(source_buildlog_path.read_text(encoding='utf-8'), encoding='utf-8')

    dest_diff_js_path = dest_result_path.joinpath(COVSCAN_DIFF_JS_FILENAME)
    waived_stage_cov_path = cc.get_nearest_waived_cov_path(waived_cov_path_root, stage_number)

    if not waived_stage_cov_path or not waived_stage_cov_path.joinpath(COVSCAN_ALL_JS_FILENAME).exists():
        # No previous commit results to compare against; diff will be same as all
        dest_diff_js_path.write_text(source_all_js, encoding='utf-8')
    else:
        waived_all_results_js_path = waived_stage_cov_path.joinpath(COVSCAN_ALL_JS_FILENAME)
        diff_results_js, _ = exectools.cmd_assert(f'csdiff {str(source_all_js_path)} {str(waived_all_results_js_path)}')
        dest_diff_js_path.write_text(diff_results_js, encoding='utf-8')

    for entry in ((source_all_js_path, dest_all_results_html_path), (dest_diff_js_path, dest_diff_results_html_path)):
        js_path, html_out_path = entry
        rc, html, stderr = exectools.cmd_gather(f'cshtml {str(js_path)}')
        if rc != 0:
            # Rarely, cshtml just outputs empty html and rc==1; just ignore it.
            html = "<html>Error generating HTML report.</html>"
            cc.logger.warning(f'Error generating HTML report for {str(js_path)}: {stderr}')
            pass
        html_out_path.write_text(html, encoding='utf-8')

    write_record()
