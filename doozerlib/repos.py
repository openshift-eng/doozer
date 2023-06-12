import asyncio
import json
import os
import tempfile
import time
from pathlib import Path
from typing import Dict, List, cast

import requests
import yaml

from doozerlib import exectools, rpm_utils

from .model import Missing, Model

DEFAULT_REPOTYPES = ['unsigned', 'signed']

# This architecture is handled differently in some cases for legacy reasons
ARCH_X86_64 = "x86_64"


class Repo(object):
    """Represents a single yum repository and provides sane ways to
    access each property based on the arch or repo type."""

    _list_rpms_cache = {}  # used to cache list_rpms calls. (repo_name, arch) => List[nvr]
    _list_rpms_cache_lock = asyncio.Lock()

    def __init__(self, name, data, valid_arches, gpgcheck=True):
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
        self.gpgcheck = gpgcheck

        self.cs_optional = self._data.content_set.get('optional', False)

        self.repotypes = DEFAULT_REPOTYPES
        self.baseurl(DEFAULT_REPOTYPES[0], self._valid_arches[0])  # run once just to populate self.repotypes
        self.reposync_enabled = True if self._data.reposync.enabled is Missing or self._data.reposync.enabled else self._data.reposync.enabled
        self.reposync_latest_only = True if self._data.reposync.latest_only is Missing or self._data.reposync.latest_only else False

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

    def is_reposync_latest_only(self):
        return self.reposync_latest_only

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
                    raise ValueError('{} is not a valid repotype option in {}'.format(repotype, list(bu.keys())))
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
            raise ValueError(f'{arch} is not a valid arch!')
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

        # Sort keys so they are always in the same order, makes unit
        # testing much easier
        for k in sorted(self._data.conf.keys()):
            v = self._data.conf[k]

            if k == 'ci_alignment':
                # Special keyword that does not translate to yum conf content.
                continue

            line = '{} = {}\n'
            if k == 'baseurl':
                line = line.format(k, self.baseurl(repotype, arch))
            elif k == 'name':
                line = line.format(k, section_name)
            elif k == 'extra_options':
                opt_lines = ''
                for opt, val in v.items():
                    opt_lines += "{} = {}\n".format(opt, val)
                line = opt_lines
            else:
                if k == 'enabled' and enabled is not None:
                    v = 1 if enabled else 0
                line = line.format(k, v)

            result += line

        # Usually, gpgcheck will not be specified, in build metadata, but don't override if it is there
        if self._data.conf.get('gpgcheck', None) is None and self._data.conf.get('extra_options', {}).get('gpgcheck', None) is None:
            # If we are building a signed repo file, and overall gpgcheck is desired
            if repotype == 'signed' and self.gpgcheck:
                result += 'gpgcheck = 1\n'
            else:
                result += 'gpgcheck = 0\n'

        if self._data.conf.get('gpgkey', None) is None and self._data.conf.get('extra_options', {}).get('gpgkey', None) is None:
            # This key will bed used only if gpgcheck=1
            result += 'gpgkey = file:///etc/pki/rpm-gpg/RPM-GPG-KEY-redhat-release\n'

        result += '\n'

        return result

    # Map arch to compatible arches
    # (https://github.com/rpm-software-management/yum/blob/4ed25525ee4781907bd204018c27f44948ed83fe/rpmUtils/arch.py#L21)
    ARCH_LISTS = {
        "x86_64": ["x86_64", "athlon", "i686", "i586", "i486", "i386", "noarch"],
        "s390x": ["s390x", "s390", "noarch"],
        "ppc64le": ["ppc64le", "noarch"],
        "aarch64": ["aarch64", "noarch"],
        "noarch": ["noarch"],
    }

    async def list_rpms(self, arch: str):
        """ List all available rpms in this rpm repository
        """
        if arch not in self._valid_arches:
            raise ValueError(f'{arch} is not a valid arch!')

        arch_list = self.ARCH_LISTS.get(arch)
        if not arch_list:
            raise ValueError(f'Arch {arch} is not supported')

        # Attempt to get the cached result before actually querying the repo
        cache_key = (self.name, arch)
        async with Repo._list_rpms_cache_lock:
            cached_result = Repo._list_rpms_cache.get(cache_key)
            if cached_result is not None:  # cache hit
                return cached_result

        # Cache miss. Will actually query the repo.
        # Note this code path is not protected inside the lock.
        # Putting repoquery inside the lock will prevent from multiple queries over different repos.
        # Currently there is a possibility to run multiple queries over the same repo, but it should be acceptable.
        result = []
        # Use a custom section name for repoquery to avoid conflict with system repos
        section_name = f"doozer-{self.name}-{arch}"
        # Create a temporary .repo file for `rpmquery` command
        with tempfile.TemporaryDirectory(prefix="doozer-") as tmp_dir, \
             tempfile.NamedTemporaryFile(prefix=f"doozer-repo-{self.name}-{arch}-", dir=tmp_dir) as fp:
            fp.write(self.conf_section("unsigned", arch, enabled=True, section_name=section_name).encode())
            fp.flush()
            # Please be aware that RHEL8+'s `repoquery` is actually `dnf repoquery`,
            # which has different flags or behaves differently comparing with the original `repoquery`.
            cmd = ["repoquery", "--config", fp.name, "--repoid", section_name, "--all", "--archlist", ",".join(arch_list + ["src"])]
            # It keeps a user specific cache
            # https://unix.stackexchange.com/questions/92257/yum-user-temp-files-var-tmp-yum-fills-up-with-repo-data
            # override the location using TMPDIR
            yum_tmp_dir = Path(tmp_dir, "yum-cache")
            yum_tmp_dir.mkdir(parents=True, exist_ok=True)
            env = os.environ.copy()
            env['TMPDIR'] = str(yum_tmp_dir.absolute())
            out, _ = await exectools.cmd_assert_async(cmd, set_env=env)
            out = cast(str, out)
            for line in out.splitlines():
                nevra = line.strip()
                if not nevra:  # skip empty line
                    continue
                # example nevra: zziplib-0:0.13.62-12.el7.x86_64
                nevr, arch = nevra.rsplit(".", 1)  # nevr: zziplib-0:0.13.62-12.el7, arch: x86_64
                rpm_info = rpm_utils.parse_nvr(nevr)
                rpm_info["arch"] = arch
                rpm_info["nvr"] = f"{rpm_info['name']}-{rpm_info['version']}-{rpm_info['release']}"
                result.append(rpm_info)

        # Cache the result
        async with Repo._list_rpms_cache_lock:
            Repo._list_rpms_cache[cache_key] = result
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
# This file is managed by the doozer build tool: https://github.com/openshift-eng/doozer,
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
# See https://source.redhat.com/groups/public/container-build-system/container_build_system_wiki/guide_to_layered_image_build_service_osbs
# for more information on maintaining this file and the format and examples.
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
    def __init__(self, repos: Dict[str, Dict], arches: List[str], gpgcheck=True):
        self._arches = arches
        self._repos: Dict[str, Repo] = {}
        repotypes = []
        names = []
        for name, repo in repos.items():
            names.append(name)
            self._repos[name] = Repo(name, repo, self._arches, gpgcheck=gpgcheck)
            repotypes.extend(self._repos[name].repotypes)
        self.names = tuple(names)
        self.repotypes = list(set(repotypes))  # leave only unique values

    def __getitem__(self, item: str) -> Repo:
        """Allows getting a Repo() object simply by name via repos[repo_name]"""
        if item not in self._repos:
            raise ValueError('{} is not a valid repo name!'.format(item))
        return self._repos[item]

    def items(self):
        return self._repos.items()

    def values(self):
        return self._repos.values()

    def __repr__(self):
        """Mainly for debugging to dump a dict representation of the collection"""
        return str(self._repos)

    def repo_file(self, repo_type, enabled_repos=[], empty_repos=[], arch=None):
        """
        Returns a str defining a list of repo configuration secions for a yum configuration file.
        :param repo_type: Whether to prefer signed or unsigned repos.
        :param enabled_repos: A list of group.yml repo names which should be enabled. If a repo is enabled==1
            in group.yml, that setting takes precedence over this list. If not enabled==1 in group.yml and not
            found in this list, the repo will be returned as enabled==0. If '*' is included in the list,
            all repos will be enabled.
        :param empty_repos: A list of repo names to return defined as no-op yum repos.
        :param arch: The architecture for which this repository should be generated. If None, all architectures
            will be included in the returned str.
        """

        result = ''
        for r in self._repos.values():

            enabled = r.enabled  # If enabled in group.yml, it will always be enabled.
            if enabled_repos and (r.name in enabled_repos or '*' in enabled_repos):
                enabled = True

            if arch:  # Generating a single arch?
                # Just use the configured name for the set. This behavior needs to be preserved to
                # prevent changing mirrored repos by reposync.
                result += r.conf_section(repo_type, enabled=enabled, arch=arch, section_name=r.name)
            else:
                # When generating a repo file for multi-arch builds, we need all arches in the same repo file.
                for iarch in r.arches:
                    section_name = '{}-{}'.format(r.name, iarch)
                    result += r.conf_section(repo_type, enabled=enabled, arch=iarch, section_name=section_name)

        for er in empty_repos:
            result += EMPTY_REPO.format(er)

        return result

    def content_sets(self, enabled_repos=[], non_shipping_repos=[]):
        """Generates a valid content_sets.yml file based on the currently
        configured and enabled repos in the collection. Using the correct
        name for each arch."""
        # check for missing repos
        missing_repos = set(enabled_repos) - self._repos.keys()
        if missing_repos:
            raise ValueError(f"enabled_repos references undefined repo(s): {missing_repos}")
        result = {}
        globally_enabled_repos = {r.name for r in self._repos.values() if r.enabled}
        shipping_repos = (set(globally_enabled_repos) | set(enabled_repos)) - set(non_shipping_repos)
        for a in sorted(self._arches):
            content_sets = []
            for r in shipping_repos:
                cs = self._repos[r].content_set(a)
                if cs:  # possible to be forced off by setting to null
                    content_sets.append(cs)
            if content_sets:
                result[a] = sorted(content_sets)
        return CONTENT_SETS + yaml.dump(result, default_flow_style=False)

    def _validate_content_sets(self, arch, names):
        url = "https://rhsm-pulp.corp.redhat.com/pulp/api/v2/repositories/search/"
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
                    # per CLOUDWF-4852 content sets may now be specified as pulp repo names.
                    "$or": [
                        {
                            "notes.content_set": {
                                "$in": names
                            }
                        }, {
                            "id": {
                                "$in": names
                            }
                        }
                    ]
                }
            }
        }

        headers = {
            'Content-Type': "application/json",
            'Authorization': "Basic cWE6cWE=",  # qa:qa
            'Cache-Control': "no-cache"
        }

        # as of 2023-06-09 authentication is required to validate content sets with rhsm-pulp
        cs_auth_key = os.environ.get("RHSM_PULP_KEY")
        cs_auth_cert = os.environ.get("RHSM_PULP_CERT")

        retry_count = 4
        for i in range(retry_count):
            try:
                response = requests.request(
                    "POST", url, data=json.dumps(payload), headers=headers,
                    cert=(cs_auth_cert, cs_auth_key),
                )
                break
            except:
                if i == retry_count - 1:
                    raise
                time.sleep(5)

        resp_dict = response.json()

        result = set()
        for repo in [Model(repo) for repo in resp_dict]:
            # per CLOUDWF-4852 content sets may now be specified as pulp repo names.
            # since we may be searching by either, return both to be compared against the request.
            result.update([repo.id, repo.notes.content_set])

        return result

    def validate_content_sets(self):
        # Determine repos that have no content sets defined at all; we will give these a pass if nothing tries to use them.
        # This is one reason to accept it if no content_set is defined at all: https://github.com/openshift-eng/ocp-build-data/pull/594

        content_set_defined = {}
        for name, repo in self._repos.items():
            content_set_defined[name] = False
            for arch in self._arches + ['default']:
                if repo._data.content_set[arch] is not Missing:
                    content_set_defined[name] = True

        invalid = []
        for arch in self._arches:
            cs_names = {}
            for name, repo in self._repos.items():
                if content_set_defined[name]:
                    cs = repo.content_set(arch)
                    cs_names[name] = cs

            arch_cs_values = list(cs_names.values())
            if arch_cs_values:
                # no point in making empty call
                valid = self._validate_content_sets(arch, arch_cs_values)
                for name, cs in cs_names.items():
                    if cs not in valid:
                        if not self._repos[name].cs_optional:
                            invalid.append('{}/{}'.format(arch, cs))
                        self._repos[name].set_invalid_cs_arch(arch)

        if invalid:
            cs_lst = ', '.join(invalid)
            raise ValueError('The following content set names are given, do not exist, and are not optional: ' + cs_lst)
