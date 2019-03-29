[![Build Status](https://travis-ci.com/openshift/doozer.svg?branch=master)](https://travis-ci.com/openshift/doozer)

## Doozer

Doozer is a build management utility that currently has the capability to build RPMs and Container Images via OSBS/Brew

## Deployment

_**Note**: This is for running the full, local python `doozer` client, not the containerized version. This requires other dependencies discussed in the [Installation doc](Installation.md)._

For local development pull the code and run:

`python setup.py develop`

For new releases, Travis-CI is already setup and deployment to PyPi is easy:

- Bump the version in `./doozerlib/VERSION`
- Push the change to `master`
- Create a new GitHub release: https://github.com/openshift/doozer/releases/new

That's it. Travis CI will do the rest automatically.


## Installation

If you just want to use `doozer` checkout the [container usage doc](Container.md).

If you need to develop for doozer checkout the full [installation doc](Installation.md).

## Usage

Checkout the [Usage doc](Usage.md)