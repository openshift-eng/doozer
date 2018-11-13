## Doozer

Doozer is a build management utility that currently has the capability to build RPMs and Container Images via OSBS/Brew
This repository is an in-progress migration from the older https://github.com/openshift/enterprise-images

## Installation

```
pip install https://github.com/openshift/doozer/archive/master.zip
```

The Doozer installation will of course automatically pull in the required python modules but it is dependent on the following CLI tools already being installed on your system:

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