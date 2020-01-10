from __future__ import absolute_import
from __future__ import unicode_literals
from .model import Model, ModelException, Missing
import yaml
import requests
import json

DEFAULT_REPOTYPES = ['unsigned', 'signed']

# This architecture is handled differently in some cases for legacy reasons
ARCH_X86_64 = "x86_64"


class Repo(object):
    """Represents a single yum repository and provides sane ways to
    access each property based on the arch or repo type."""

    def __init__(self, name, data, valid_arches):
        self.name = name
        self._valid_arches = valid_arches
        self._invalid_cs_arches = set()
        self._data = Model(data)
        for req in ['conf', 'content_set']:
            if req not in self._data:
                raise ValueError('Repo definitions must contain "{}" key!'.format(req))
        if self._data.conf.baseurl is Missing:
            raise ValueError('Repo definitions must include conf.baseurl!')

        # fill out default conf values
        conf = self._data.conf
        conf.name = conf.get('name', name)
        conf.enabled = conf.get('enabled', None)
        self.enabled = conf.enabled == 1
        conf.gpgcheck = conf.get('gpgcheck', 0)
        self.cs_optional = self._data.content_set.get('optional', False)

        self.repotypes = DEFAULT_REPOTYPES
        self.baseurl(DEFAULT_REPOTYPES[0], self._valid_arches[0])  # run once just to populate self.repotypes
        self.reposync_enabled = True if self._data.reposync.enabled is Missing else self._data.reposync.enabled

    @property
    def enabled(self):
        """Allows access via repo.enabled"""
        return self._data.conf.enabled == 1

    @property
    def arches(self):
        return list(self._valid_arches)

    @enabled.setter
    def enabled(self, val):
        """Set enabled option without digging direct into the underlying data"""
        self._data.conf.enabled = 1 if val else 0

    def set_invalid_cs_arch(self, arch):
        self._invalid_cs_arches.add(arch)

    def is_reposync_enabled(self):
        return self.reposync_enabled

    def __repr__(self):
        """For debugging mainly, to display contents as a dict"""
        return str(self._data)

    def baseurl(self, repotype, arch):
        if not repotype:
            repotype = 'unsigned'
        """Get baseurl based on repo type, if one was specified for this repo."""
        bu = self._data.conf.baseurl
        if isinstance(bu, str):
            return bu
        elif isinstance(bu, dict):
            if arch in bu:
                bu_sub = bu
            else:
                if repotype not in bu:
                    raise ValueError('{} is not a valid repotype option in {}'.format(repotype, bu.keys()))
                self.repotypes = list(bu.keys())
                bu_sub = bu[repotype]
            if isinstance(bu_sub, str):
                return bu_sub
            elif isinstance(bu_sub, dict):
                if arch not in self._valid_arches:
                    raise ValueError('{} is not a valid arch option!'.format(arch))
                if arch not in bu_sub:
                    raise ValueError('No baseurl available for arch {}'.format(arch))
                return bu_sub[arch]
            return bu[repotype]
        else:
            raise ValueError('baseurl must be str or dict!')

    def content_set(self, arch):
        """Return content set name for given arch with sane fallbacks and error handling."""

        if arch not in self._valid_arches:
            raise ValueError('{} is not a valid arch!')
        if arch in self._invalid_cs_arches:
            return None
        if self._data.content_set[arch] is Missing:
            if self._data.content_set['default'] is Missing:
                raise ValueError('{} does not contain a content_set for {} and no default was provided.'.format(self.name, arch))
            return self._data.content_set['default']
        else:
            return self._data.content_set[arch]

    def conf_section(self, repotype, arch=ARCH_X86_64, enabled=None, section_name=None):
        """
        Returns a str that represents a yum repo configuration section corresponding
        to this repo in group.yml.

        :param repotype: Whether to use signed or unsigned repos from group.yml
        :param arch: The architecture this section if being generated for (e.g. ppc64le or x86_64).
        :param enabled: If True|False, explicitly set 'enabled = 1|0' in section. If None, inherit group.yml setting.
        :param section_name: The section name to use if not the repo name in group.yml.
        :return: Returns a string representing a repo section in a yum configuration file. e.g.
            [rhel-7-server-ansible-2.4-rpms]
            gpgcheck = 0
            enabled = 0
            baseurl = http://pulp.dist.pr.../x86_64/ansible/2.4/os/
            name = rhel-7-server-ansible-2.4-rpms
        """
        if not repotype:
            repotype = 'unsigned'

        if arch not in self._valid_arches:
            raise ValueError('{} does not identify a yum repository for arch: {}'.format(self.name, arch))

        if not section_name:  # If the caller has not specified a name, use group.yml name.
            section_name = self.name

        result = '[{}]\n'.format(section_name)

        for k, v in self._data.conf.iteritems():
            line = '{} = {}\n'
            if k == 'baseurl':
                line = line.format(k, self.baseurl(repotype, arch))
            elif k == 'name':
                line = line.format(k, section_name)
            else:
                if k == 'enabled' and enabled is not None:
                    v = 1 if enabled else 0
                line = line.format(k, v)

            result += line
        result += '\n'

        return result


# base empty repo section for disabling repos in Dockerfiles
EMPTY_REPO = """
[{0}]
baseurl = http://download.lab.bos.redhat.com/rcm-guest/puddles/RHAOS/AtomicOpenShift_Empty/
enabled = 1
gpgcheck = 0
name = {0}
"""

# Base header for all content_sets.yml output
CONTENT_SETS = """
# This file is managed by the doozer build tool: https://github.com/openshift/doozer,
# by the OpenShift Automated Release Team (#aos-art on CoreOS Slack).
# Any manual changes will be overwritten by doozer on the next build.
#
# This is a file defining which content sets (yum repositories) are needed to
# update content in this image. Data provided here helps determine which images
# are vulnerable to specific CVEs. Generally you should only need to update this
# file when:
#    1. You start depending on a new product
#    2. You are preparing new product release and your content sets will change
#
# See https://mojo.redhat.com/docs/DOC-1023066 for more information on
# maintaining this file and the format and examples
#
# You should have one top level item for each architecture being built. Most
# likely this will be x86_64 and ppc64le initially.
---
"""


class Repos(object):
    """
    Represents the entire collection of repos and provides
    automatic content_set and repo conf file generation.
    """
    def __init__(self, repos, arches):
        self._arches = arches
        self._repos = {}
        repotypes = []
        names = []
        for name, repo in repos.iteritems():
            names.append(name)
            self._repos[name] = Repo(name, repo, self._arches)
            repotypes.extend(self._repos[name].repotypes)
        self.names = tuple(names)
        self.repotypes = list(set(repotypes))  # leave only unique values

    def __getitem__(self, item):
        """Allows getting a Repo() object simply by name via repos[repo_name]"""
        if item not in self._repos:
            raise ValueError('{} is not a valid repo name!'.format(item))
        return self._repos[item]

    def iteritems(self):
        return self._repos.iteritems()

    def itervalues(self):
        return self._repos.itervalues()

    def __repr__(self):
        """Mainly for debugging to dump a dict representation of the collection"""
        return str(self._repos)

    def repo_file(self, repo_type, enabled_repos=[], empty_repos=[], arch=None):
        """
        Returns a str defining a list of repo configuration secions for a yum configuration file.
        :param repo_type: Whether to prefer signed or unsigned repos.
        :param enabled_repos: A list of group.yml repo names which should be enabled. If a repo is enabled==1
            in group.yml, that setting takes precedence over this list. If not enabled==1 in group.yml and not
            found in this list, the repo will be returned as enabled==0.
        :param empty_repos: A list of repo names to return defined as no-op yum repos.
        :param arch: The architecture for which this repository should be generated. If None, all architectures
            will be included in the returned str.
        """

        result = ''
        for r in self._repos.itervalues():

            enabled = r.enabled  # If enabled in group.yml, it will always be enabled.
            if enabled_repos and r.name in enabled_repos:
                enabled = True

            if arch:  # Generating a single arch?
                # Just use the configured name for the. This behavior needs to be preserved to
                # prevent changing mirrored repos by reposync.
                result += r.conf_section(repo_type, enabled=enabled, arch=arch, section_name=r.name)
            else:
                # When generating a repo file for builds, we need all arches in the same repo file.
                for iarch in r.arches:
                    section_name = '{}-{}'.format(r.name, iarch)
                    result += r.conf_section(repo_type, enabled=enabled, arch=iarch, section_name=section_name)

        for er in empty_repos:
            result += EMPTY_REPO.format(er)

        return result

    def empty_repo_file_from_list(self, repos, odcs=False):
        full_cs = self.full_content_sets_list()
        result = ''
        for er in repos:
            if not odcs or er not in full_cs:
                result += EMPTY_REPO.format(er)
        return result

    def full_content_sets_dict(self):
        result = {}
        for a in self._arches:
            result[a] = {}
            for r in self._repos.itervalues():
                result[a][r.name] = r.content_set(a)
        return result

    def full_content_sets_list(self):
        result = []
        for a in self._arches:
            for r in self._repos.itervalues():
                result.append(r.content_set(a))
        return list(set(result))  # make unique list

    def content_sets(self, enabled_repos=[]):
        """Generates a valid content_sets.yml file based on the currently
        configured and enabled repos in the collection. Using the correct
        name for each arch."""

        result = {}
        for a in self._arches:
            result[a] = []
            for r in self._repos.itervalues():
                if r.enabled or r.name in enabled_repos:
                    cs = r.content_set(a)
                    if cs:  # possible to be forced off by setting to null
                        result[a].append(cs)

        return CONTENT_SETS + yaml.dump(result, default_flow_style=False)

    def _validate_content_sets(self, arch, names):
        url = "https://pulp.dist.prod.ext.phx2.redhat.com/pulp/api/v2/repositories/search/"
        payload = {
            "criteria": {
                "fields": [
                    "id",
                    "notes"
                ],
                "filters": {
                    "notes.arch": {
                        "$in": [
                            arch
                        ]
                    },
                    "notes.content_set": {
                        "$in": names
                    }
                }
            }
        }

        headers = {
            'Content-Type': "application/json",
            'Authorization': "Basic cWE6cWE=",  # qa:qa
            'Cache-Control': "no-cache"
        }

        response = requests.request("POST", url, data=json.dumps(payload), headers=headers)

        resp_dict = response.json()

        result = []
        for cs in resp_dict:
            result.append(Model(cs).notes.content_set)

        return set(result)

    def validate_content_sets(self):
        invalid = []
        for arch in self._arches:
            cs_names = {}
            for name, repo in self._repos.iteritems():
                cs = repo.content_set(arch)
                cs_names[name] = cs

            arch_cs_values = cs_names.values()
            if arch_cs_values:
                # no point in making empty call
                valid = self._validate_content_sets(arch, arch_cs_values)
                for name, cs in cs_names.iteritems():
                    if cs not in valid:
                        if not self._repos[name].cs_optional:
                            invalid.append('{}/{}'.format(arch, cs))
                        self._repos[name].set_invalid_cs_arch(arch)

        if invalid:
            cs_lst = ', '.join(invalid)
            raise ValueError('The following content set names are given, do not exist, and are not optional: ' + cs_lst)
