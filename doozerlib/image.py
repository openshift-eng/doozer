import os
import json
import bashlex
from dockerfile_parse import DockerfileParser
from distgit import pull_image
from metadata import Metadata
from model import Missing
from pushd import Dir
from exceptions import DoozerFatalError

import assertion
import logutil
import exectools
import logutil

logger = logutil.getLogger(__name__)


YUM_NON_FLAGS = [
    '-c', '--config',
    '--installroot',
    '--enableplugin',
    '--disableplugin',
    '--setopt',
    '-R', '--randomwait',
    '-d', '--debuglevel',
    '-e', '--errorlevel',
    '--rpmverbosity',
    '--enablerepo',
    '--disablerepo',
    '--repo', '--repoid',
    '-x', '--exclude', '--excludepkgs',
    '--disableexcludes', '--disableexcludepkgs',
    '--repofrompath',
    '--destdir', '--downloaddir',
    '--comment',
    '--advisory', '--advisories',
    '--bzs', '--cves', '--sec-severity',
    '--forearch'
]


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

    def get_rpm_install_list(self, valid_pkg_list=None):
        """Parse dockerfile and find any RPMs that are being installed
        It will automatically do any bash variable replacement during this parse
        """
        if self._distgit_repo:
            # Already cloned, load from there
            with Dir(self._distgit_repo.distgit_dir):
                dfp = DockerfileParser('Dockerfile')

        else:
            # not yet cloned, just download it
            dfp = DockerfileParser()
            dfp.content = self.fetch_cgit_file("Dockerfile")

        def env_replace(envs, val):
            # find $VAR and ${VAR} style replacements
            for k, v in envs.iteritems():
                opts = ['${}'.format(k), '${{{}}}'.format(k)]
                for opt in opts:
                    if opt in val:
                        val = val.replace(opt, v)
            return val

        def is_assignment(line):
            # if this is an assignment node we need to add
            # to env dict for later replacement in commands
            try:
                parts = bashlex.parse(line)
            except:
                # bashlex does not get along well with some inline
                # conditionals and may emit ParsingError
                # if that's the case, it's not an assigment, so move along
                return None
            for ast in parts:
                if ast.kind != 'compound':  # ignore multi part commands
                    for part in ast.parts:
                        if part.kind == 'assignment':
                            return part.word.split('=')
            return None

        envs = dict(dfp.envs)
        run_lines = []
        for entry in json.loads(dfp.json):
            if isinstance(entry, dict) and 'RUN' in entry:
                line = entry['RUN']
                for line in line.split("&"):
                    line = line.strip()
                    if line:
                        line = env_replace(envs, line)
                        assign = is_assignment(line)
                        if assign:
                            envs[assign[0]] = assign[1]
                        run_lines.append(line)

        rpms = []
        for line in run_lines:
            split = list(bashlex.split(line))
            if 'yum' in split and 'install' in split:
                # remove as to not mess with checking below
                split.remove('yum')
                split.remove('install')

                i = 0
                rpm_start = 0
                while i < len(split):
                    sub = split[i]
                    if sub.startswith('-'):
                        if sub in YUM_NON_FLAGS:
                            i += 1
                            continue
                    else:
                        rpm_start = i
                        break  # found start of rpm names, exit
                    i += 1

                rpms.extend(split[rpm_start:])

        return [str(r) for r in rpms]  # strip unicode

    def get_latest_build_info(self):

        """
        Queries brew to determine the most recently built release of the component
        associated with this image. This method does not rely on the "release"
        label needing to be present in the Dockerfile.

        :return: A tuple: (component name, version, release); e.g. ("registry-console-docker", "v3.6.173.0.75", "1")
        """

        component_name = self.get_component_name()

        tag = "{}-candidate".format(self.branch())

        rc, stdout, stderr = exectools.cmd_gather(["brew", "latest-build", tag, component_name])

        assertion.success(rc, "Unable to search brew builds: %s" % stderr)

        latest = stdout.strip().splitlines()[-1].split(' ')[0]

        if not latest.startswith(component_name):
            # If no builds found, `brew latest-build` output will appear as:
            # Build                                     Tag                   Built by
            # ----------------------------------------  --------------------  ----------------
            raise IOError("No builds detected for %s using tag: %s" % (self.qualified_name, tag))

        # latest example: "registry-console-docker-v3.6.173.0.75-1""
        name, version, release = latest.rsplit("-", 2)  # [ "registry-console-docker", "v3.6.173.0.75", "1"]

        return name, version, release

    def pull_url(self):
        # Don't trust what is the Dockerfile for version & release. This field may not even be present.
        # Query brew to find the most recently built release for this component version.
        _, version, release = self.get_latest_build_info()
        return "{host}/{name}:{version}-{release}".format(
            host=self.runtime.group_config.urls.brew_image_host, name=self.config.name, version=version, release=release)

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
