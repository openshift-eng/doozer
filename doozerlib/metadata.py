from __future__ import absolute_import, print_function, unicode_literals
from future import standard_library
standard_library.install_aliases
import urllib.parse
from retry import retry
import requests
import yaml
from collections import OrderedDict

from .pushd import Dir
from .distgit import ImageDistGitRepo, RPMDistGitRepo
from . import exectools
from . import logutil

from .model import Model, Missing

#
# These are used as labels to index selection of a subclass.
#
DISTGIT_TYPES = {
    'image': ImageDistGitRepo,
    'rpm': RPMDistGitRepo
}

CONFIG_MODES = [
    'enabled',  # business as usual
    'disabled',  # manually disabled from automatically building
    'wip',  # Work in Progress, do not build
]

CONFIG_MODE_DEFAULT = CONFIG_MODES[0]


class Metadata(object):
    def __init__(self, meta_type, runtime, data_obj):
        """
        :param: meta_type - a string. Index to the sub-class <'rpm'|'image'>.
        :param: runtime - a Runtime object.
        :param: name - a filename to load as metadata
        """
        self.meta_type = meta_type
        self.runtime = runtime
        self.data_obj = data_obj
        self.config_filename = data_obj.filename
        self.full_config_path = data_obj.path

        # Some config filenames have suffixes to avoid name collisions; strip off the suffix to find the real
        # distgit repo name (which must be combined with the distgit namespace).
        # e.g. openshift-enterprise-mediawiki.apb.yml
        #      distgit_key=openshift-enterprise-mediawiki.apb
        #      name (repo name)=openshift-enterprise-mediawiki

        self.distgit_key = data_obj.key
        self.name = self.distgit_key.split('.')[0]   # Split off any '.apb' style differentiator (if present)

        self.runtime.logger.debug("Loading metadata from {}".format(self.full_config_path))

        self.config = Model(data_obj.data)

        self.mode = self.config.get('mode', CONFIG_MODE_DEFAULT).lower()
        if self.mode not in CONFIG_MODES:
            raise ValueError('Invalid mode for {}'.format(self.config_filename))

        self.enabled = (self.mode == CONFIG_MODE_DEFAULT)

        # Basic config validation. All images currently required to have a name in the metadata.
        # This is required because from.member uses these data to populate FROM in images.
        # It would be possible to query this data from the distgit Dockerflie label, but
        # not implementing this until we actually need it.
        assert (self.config.name is not Missing)

        # Choose default namespace for config data
        if meta_type == "image":
            self.namespace = "containers"
        else:
            self.namespace = "rpms"

        # Allow config data to override
        if self.config.distgit.namespace is not Missing:
            self.namespace = self.config.distgit.namespace

        self.qualified_name = "%s/%s" % (self.namespace, self.name)
        self.qualified_key = "%s/%s" % (self.namespace, self.distgit_key)

        # Includes information to identify the metadata being used with each log message
        self.logger = logutil.EntityLoggingAdapter(logger=self.runtime.logger, extra={'entity': self.qualified_key})

        self._distgit_repo = None

    def save(self):
        self.data_obj.data = self.config.primitive()
        self.data_obj.save()

    def distgit_remote_url(self):
        pkgs_host = self.runtime.group_config.urls.get('pkgs_host', 'pkgs.devel.redhat.com')
        # rhpkg uses a remote named like this to pull content from distgit
        return f'ssh://{self.runtime.user}@{pkgs_host}/{self.qualified_name}'

    def distgit_repo(self, autoclone=True):
        if self._distgit_repo is None:
            self._distgit_repo = DISTGIT_TYPES[self.meta_type](self, autoclone=autoclone)
        return self._distgit_repo

    def branch(self):
        if self.config.distgit.branch is not Missing:
            return self.config.distgit.branch
        return self.runtime.branch

    def brew_tag(self):
        if self.runtime.brew_tag:
            return self.runtime.brew_tag
        else:
            return '{}-candidate'.format(self.branch())

    def get_arches(self):
        """
        :return: Returns the list of architecture this image/rpm should build for. This is an intersection
        of config specific arches & globally enabled arches in group.yml
        """
        if self.config.arches:
            ca = self.config.arches
            intersection = list(set(self.runtime.get_global_arches()) & set(ca))
            if len(intersection) != len(ca):
                self.logger.info(f'Arches are being pruned by group.yml. Using computed {intersection} vs config list {ca}')
            if not intersection:
                raise ValueError(f'No arches remained enabled in {self.qualified_key}')
            return intersection
        else:
            return list(self.runtime.get_global_arches())

    def cgit_url(self, filename):
        rev = self.branch()
        ret = "/".join((self.runtime.group_config.urls.cgit, self.qualified_name, "plain", filename))
        if rev is not None:
            ret = "{}?h={}".format(ret, rev)
        return ret

    def fetch_cgit_file(self, filename):
        url = self.cgit_url(filename)
        req = exectools.retry(
            3, lambda: urllib.request.urlopen(url),
            check_f=lambda req: req.code == 200)
        return req.read()

    def get_brew_image_name_short(self):
        # Get image name in the Brew pullspec. e.g. openshift3/ose-ansible --> openshift3-ose-ansible
        return self.image_name.replace("/", "-")

    def get_component_name(self):
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

    def get_maintainer_info(self):
        """
        :return: Returns a dict of identifying maintainer information. Dict might be empty if no maintainer information is available.
            fields are generally [ component: '...', subcomponent: '...', and product: '...' ] if available. These
            are coordinates for product security to figure out where to file bugs when an image or RPM has an issue.
        """

        # We are trying to discover some team information that indicates which BZ or Jira board bugs for this
        # component should be filed against. This information can be stored in the doozer metadata OR
        # in upstream source. Metadata overrides, as usual.

        source_dir = self.runtime.resolve_source(self)

        # Maintainer info can be defined in metadata, so try there first.
        maintainer = self.config.maintainer or dict()

        # This tuple will also define key ordering in the returned OrderedDict
        known_fields = ('product', 'component', 'subcomponent')

        # Fill in any missing attributes from upstream source
        if source_dir:
            with Dir(source_dir):
                # Not every repo has a master branch, they may have a different default; detect it.
                remote_info, _ = exectools.cmd_assert(f'git remote show origin')
                head_branch_lines = [i for i in remote_info.splitlines() if i.strip().startswith('HEAD branch:')]  # e.g. [ "  HEAD branch: master" ]
                if not head_branch_lines:
                    raise IOError('Error trying to detect remote default branch')
                default_branch = head_branch_lines[0].strip().split()[-1]  # [ "  HEAD branch: master" ] => "master"

                _, owners_yaml, _ = exectools.cmd_gather(f'git --no-pager show origin/{default_branch}:OWNERS')
                if owners_yaml.strip():
                    owners = yaml.safe_load(owners_yaml)
                    for field in known_fields:
                        if field not in maintainer and field in owners:
                            maintainer[field] = owners[field]

        if 'product' not in maintainer:
            maintainer['product'] = 'OpenShift Container Platform'  # Safe bet - we are ART.

        # Just so we return things in a defined order (avoiding unnecessary changes in git commits)
        sorted_maintainer = OrderedDict()
        for k in known_fields:
            if k in maintainer:
                sorted_maintainer[k] = maintainer[k]

        # Add anything remaining in alpha order
        for k in sorted(maintainer.keys()):
            if k not in sorted_maintainer:
                sorted_maintainer[k] = maintainer[k]

        return sorted_maintainer
