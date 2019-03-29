#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup


def _get_version():
    from os.path import abspath, dirname, join
    filename = join(dirname(abspath(__file__)), '../doozerlib', 'VERSION')
    return open(filename).read().strip()


setup(
    name="rundoozer",
    author="AOS ART Team",
    author_email="aos-team-art@redhat.com",
    version=_get_version(),
    description="CLI tool for managing and automating Red Hat software releases",
    url="https://github.com/openshift/doozer",
    license="Red Hat Internal",
    packages=[],
    include_package_data=True,
    scripts=[
        'rundoozer'
    ],

    install_requires=['pydotconfig>=0.1.5'],

    dependency_links=[]
)
