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
name = rhaos-4.4-rhel-8-build
gpgcheck = 1
gpgkey = file:///etc/pki/rpm-gpg/RPM-GPG-KEY-redhat-release

"""

EXPECTED_BASIC_UNSIGNED_REPO = """[rhaos-4.4-rhel-8-build]
baseurl = http://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.4-rhel-8-build/latest/x86_64/
enabled = 1
name = rhaos-4.4-rhel-8-build
gpgcheck = 0
gpgkey = file:///etc/pki/rpm-gpg/RPM-GPG-KEY-redhat-release

"""

EXPECTED_EXTRA_OPTIONS_REPO = """[rhaos-4.4-rhel-8-build]
baseurl = http://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.4-rhel-8-build/latest/x86_64/
enabled = 1
module_hotfixes = 1
name = rhaos-4.4-rhel-8-build
gpgcheck = 1
gpgkey = file:///etc/pki/rpm-gpg/RPM-GPG-KEY-redhat-release

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

    # no conf
    invalid_repo_config1 = {
        'content_set': {
            'default': 'rhel-7-server-ose-4.2-rpms',
            'ppc64le': 'rhel-7-for-power-le-ose-4.2-rpms',
            's390x': 'rhel-7-for-system-z-ose-4.2-rpms',
            'optional': True,
        }
    }
    # no content sets
    invalid_repo_config2 = {
        'conf': {
            'enabled': 1,
            'baseurl': {
                'ppc64le': 'http://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.4-rhel-8-build/latest/ppc64le/',
                's390x': 'http://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.4-rhel-8-build/latest/s390x/',
                'x86_64': 'http://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.4-rhel-8-build/latest/x86_64/',
            }
        }
    }
    # no baseurls
    invalid_repo_config3 = {
        'conf': {
            'enabled': 1,
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
        conf_str = self.repo.conf_section('unsigned')
        # The two basic repo files are the same
        self.assertEqual(EXPECTED_BASIC_UNSIGNED_REPO, conf_str)

    def test_conf_section_extra_options(self):
        """ensure we can print a correct expected repo string with extra options"""
        conf_str_extra = self.repo_extras.conf_section('signed')
        # The repo with an 'extra_options' section is correct
        self.assertEqual(EXPECTED_EXTRA_OPTIONS_REPO, conf_str_extra)

    def test_content_set(self):
        """ensure content sets can be correctly selected"""
        self.assertEqual("rhel-7-server-ose-4.2-rpms", self.repo.content_set('x86_64'))

        with self.assertRaises(ValueError):
            # Will not be a valid content set
            self.repo.content_set('redhat')

        # Add a fake content set to the 'invalid sets' list
        self.repo.set_invalid_cs_arch('hatred')
        # Now ensure there is an error when we try to use it
        with self.assertRaises(ValueError):
            self.repo.content_set('hatred')

        # Manually indicate that a previously valid content set arch
        # is no longer valid
        self.repo.set_invalid_cs_arch('x86_64')
        self.assertIsNone(self.repo.content_set('x86_64'))

    def test_arches(self):
        """ensure we can get the arches we configured the repo with"""
        self.assertListEqual(self.repo.arches, self.arches)

    def test_init_validation(self):
        """ensure the init method checks for incorrectly configured repos"""
        # no 'conf' section
        with self.assertRaises(ValueError):
            Repo('no-conf', self.invalid_repo_config1, self.arches)
        # no 'content sets' section
        with self.assertRaises(ValueError):
            Repo('no-content-sets', self.invalid_repo_config2, self.arches)
        # no 'base urls' section
        with self.assertRaises(ValueError):
            Repo('no-base-urls', self.invalid_repo_config3, self.arches)
