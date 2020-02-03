#!/usr/bin/env python
"""
Test the ImageMetadata class
"""
from __future__ import absolute_import, print_function, unicode_literals
import unittest
import os
import logging
import tempfile
import shutil
import flexmock
try:
    from importlib import reload
except ImportError:
    pass
from doozerlib import image, exectools
from doozerlib.repos import Repo

EXPECTED_BASIC_REPO = """[rhaos-4.4-rhel-8-build]
baseurl = http://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.4-rhel-8-build/latest/x86_64/
enabled = 1
gpgcheck = 0
name = rhaos-4.4-rhel-8-build

"""

EXPECTED_EXTRA_OPTIONS_REPO = """[rhaos-4.4-rhel-8-build]
baseurl = http://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.4-rhel-8-build/latest/x86_64/
enabled = 1
module_hotfixes = 1
gpgcheck = 0
name = rhaos-4.4-rhel-8-build

"""


class MockRuntime(object):

    def __init__(self, logger):
        self.logger = logger


class TestRepos(unittest.TestCase):
    repo_config = {
        'conf': {
            'enabled': 1,
            'baseurl': {
                'ppc64le': 'http://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.4-rhel-8-build/latest/ppc64le/',
                's390x': 'http://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.4-rhel-8-build/latest/s390x/',
                'x86_64': 'http://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.4-rhel-8-build/latest/x86_64/',
            },
        },
        'content_set': {
            'default': 'rhel-7-server-ose-4.2-rpms',
            'ppc64le': 'rhel-7-for-power-le-ose-4.2-rpms',
            's390x': 'rhel-7-for-system-z-ose-4.2-rpms',
            'optional': True,
        }
    }
    repo_config_extras = {
        'conf': {
            'extra_options': {
                'module_hotfixes': 1
            },
            'enabled': 1,
            'baseurl': {
                'ppc64le': 'http://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.4-rhel-8-build/latest/ppc64le/',
                's390x': 'http://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.4-rhel-8-build/latest/s390x/',
                'x86_64': 'http://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.4-rhel-8-build/latest/x86_64/',
            },
        },
        'content_set': {
            'default': 'rhel-7-server-ose-4.2-rpms',
            'ppc64le': 'rhel-7-for-power-le-ose-4.2-rpms',
            's390x': 'rhel-7-for-system-z-ose-4.2-rpms',
            'optional': True,
        }
    }
    arches = ['x86_64', 'ppc64le', 's390x']
    test_repo = 'rhaos-4.4-rhel-8-build'

    def setUp(self):
        self.repo = Repo(self.test_repo, self.repo_config, self.arches)
        self.repo_extras = Repo(self.test_repo, self.repo_config_extras, self.arches)

    def testRepoEnabled(self):
        """see if this repo correctly reports being enabled"""
        self.assertTrue(self.repo.enabled)

    def test_conf_section_basic(self):
        """ensure we can print a correct expected repo string"""
        conf_str = self.repo.conf_section('signed')
        # The two basic repo files are the same
        self.assertEqual(EXPECTED_BASIC_REPO, conf_str)

    def test_conf_section_extra_options(self):
        """ensure we can print a correct expected repo string with extra options"""
        conf_str_extra = self.repo_extras.conf_section('signed')
        # The repo with an 'extra_options' section is correct
        self.assertEqual(EXPECTED_EXTRA_OPTIONS_REPO, conf_str_extra)
