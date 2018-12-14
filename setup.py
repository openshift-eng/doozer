#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

with open('./requirements.txt') as f:
    INSTALL_REQUIRES = f.read().splitlines()


def _get_version():
    from os.path import abspath, dirname, join
    filename = join(dirname(abspath(__file__)), 'doozerlib', 'VERSION')
    return open(filename).read().strip()


setup(
    name="rh-doozer",
    author="AOS ART Team",
    author_email="aos-team-art@redhat.com",
    version=_get_version(),
    description="CLI tool for managing and automating Red Hat software releases",
    long_description=open('README.md').read(),
    url="https://github.com/openshift/doozer",
    license="Red Hat Internal",
    packages=["doozerlib"],
    include_package_data=True,
    scripts=[
        'doozer'
    ],

    install_requires=INSTALL_REQUIRES,

    dependency_links=[]
)
