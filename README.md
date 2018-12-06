## Doozer

Doozer is a build management utility that currently has the capability to build RPMs and Container Images via OSBS/Brew
This repository is an in-progress migration from the older https://github.com/openshift/enterprise-images

## Deployment

For local development pull the code and run:

`python setup.py develop`

For new releases, once `master` is decided to be good:

- Bump the version number in `doozerlib/VERSION`
- rebase `master` onto the `released` branch so that they are even.
    - `git clone git@github.com:openshift/doozer.git`
    - `cd doozer`
    - `git fetch`
    - `git checkout released`
    - `git rebase master`
    - `git push origin released`
- Cut a new github release with the same version string as given in `setup.py`
- On your system run `pip install -U https://github.com/openshift/doozer/archive/released.zip`


## Installation

To install the latest released version of doozer, run:

```
pip install https://github.com/openshift/doozer/archive/released.zip
```

If instead, you would like to run with the latest and greatest, but potential unstable, run:

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
