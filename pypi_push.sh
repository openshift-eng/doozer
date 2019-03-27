#!/usr/bin/env bash

REPO="https://upload.pypi.org/legacy/"

# deploy rh-doozer
python setup.py sdist bdist_wheel
twine upload -u OpenShiftART -p "$1" --repository-url $REPO dist/*

# deploy rundoozer
pushd rundoozer
python setup.py sdist bdist_wheel
twine upload -u OpenShiftART -p "$1" --repository-url $REPO dist/*
popd