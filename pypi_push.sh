#!/usr/bin/env bash

set -e

REPO="https://upload.pypi.org/legacy/"

# install twine for pypi push
pip install twine

# deploy rh-doozer
python setup.py sdist bdist_wheel
twine upload -u OpenShiftART -p "$1" --repository-url $REPO dist/*

# deploy rundoozer
pushd rundoozer
python setup.py sdist bdist_wheel
twine upload -u OpenShiftART -p "$1" --repository-url $REPO dist/*
popd