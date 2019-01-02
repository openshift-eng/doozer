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

`[dnf|yum] install krb5-devel python2-devel`

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
