[![Build Status](https://travis-ci.com/openshift/doozer.svg?branch=master)](https://travis-ci.com/openshift/doozer)

## Doozer

Doozer is a build management utility that currently has the capability to build RPMs and Container Images via OSBS/Brew
This repository is an in-progress migration from the older https://github.com/openshift/enterprise-images

## Deployment

For local development pull the code and run:

`python setup.py develop`

For new releases, Travis-CI is already setup and deployment to PyPi is easy:

- Bump the version in `./doozerlib/VERSION`
- Push the change to `master`
- Create a new GitHub release: https://github.com/openshift/doozer/releases/new

That's it. Travis CI will do the rest automatically.


## Installation

To install the latest released version of doozer, run:

```
pip install -U rh-doozer
```

If instead, you would like to run with the latest and greatest from source, but potential unstable, run:

```
pip install https://github.com/openshift/doozer/archive/master.zip
```

The Doozer installation will of course automatically pull in the required python modules but it is dependent on the following packages and CLI tools already being installed on your system:

### **devel packages***

`[dnf|yum] install krb5-devel python2-devel libffi`

### **git**

Likely already on your system. If not:

`[dnf|yum] install git`

### **brew & rhpkg**

Enable the following repos on your syste:

- https://gitlab.cee.redhat.com/platform-eng-core-services/internal-repos/raw/master/rhel/rhel-7.repo
- http://download.devel.redhat.com/rel-eng/RCMTOOLS/rcm-tools-rhel-7-server.repo

Then install with:

`[yum|dnf] install koji rhpkg`


### **tito**

Fedora:

`dnf install tito`

RHEL/CentOS:
```
#enable EPEL - https://fedoraproject.org/wiki/EPEL#How_can_I_use_these_extra_packages.3F
yum install tito
```

### **docker**

- RHEL: https://docs.docker.com/install/linux/docker-ee/rhel/
- Fedora: https://docs.docker.com/install/linux/docker-ce/fedora/

### **repoquery** (from yum, not dnf) and **rsync**

`[yum|dnf] install yum-utils rsync`


# Local Image Builds

### Install ImageBuilder

Some images are built with `imagebuilder` instead of `docker build` (this is configured in the image's config yaml) and therefore you need to install `imagebuilder`:

 `go get -u github.com/openshift/imagebuilder/cmd/imagebuilder`

*Note*: by default go "gets" binaries into $GOPATH/bin, which you may want to put in your $PATH. If there is no $GOPATH golang defaults to $HOME/go

### Registry Setup

Because many images require resources on internal registries is it important to setup your local docker to search for images in the right place.
Note: This is just one example and your mileage may vary depending on your system and tool versions

- Add the following entry to your `/etc/docker/daemon.json`:

    `"insecure-registries": ["http://brew-pulp-docker01.web.prod.ext.phx2.redhat.com:8888/"]`

- In `/etc/containers/registries.conf ` add `http://brew-pulp-docker01.web.prod.ext.phx2.redhat.com:8888` to both `[registries.search:registries]` and `[registries.insecure:registries]`. We recommend adding it to the beginning of the list if possible so that docker does not pull public versions of an image.

- Restart docker
