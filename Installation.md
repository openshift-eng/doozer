# READ FIRST
**`doozer` has a highly involved set of dependencies and unless you are developing doozer, there's an easier way! Run it as a [container app](Container.md)!**


# Dependencies

While Doozer is a pure python application it's heavily dependent on a wide range of internal Red Hat tooling so we've wrapped as much of that as possible into an install script (more on this below). But first you need ensure that you have a few basic requirements setup.


## Kerberos
You must have kerberos authentication setup and be able to kinit as your LDAP user. This is required for interacting with distgit.

## Internal Red Hat Certs
You must have valid corporate IT certificates imported on your system:


```
$ cd /etc/pki/ca-trust/source/anchors
$ sudo curl -O https://password.corp.redhat.com/RH-IT-Root-CA.crt
$ sudo curl -k -O https://engineering.redhat.com/Eng-CA.crt
$ sudo update-ca-trust extract
```

## Key Auth to GitHub

You will need to make sure that you can clone from GitHub without a password. If you have not already setup an SSH key in your GitHub account follow [these instructions](https://help.github.com/articles/adding-a-new-ssh-key-to-your-github-account/). This can be tested by running:

`ssh -T git@github.com`

# Run Install Script

With the above completed you can just run the install script via:

`bash <(curl https://raw.githubusercontent.com/openshift-eng/doozer/master/install.sh)`

You **must** run the script as the kerberos user you will be using.

## Dependency Details

To dive further into what the above install script is doing, `doozer` requires all of the following:

### **devel packages***

`[dnf|yum] install gcc krb5-devel python3-devel python3-rpm python3-pip libffi`

### **git**

Likely already on your system. If not:

`[dnf|yum] install git`

### **brew & rhpkg**

Enable the following repos on your system:

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

### **podman**

`[yum|dnf] install podman`

More details [here](https://github.com/containers/libpod/blob/master/install.md)

### **repoquery** (from yum, not dnf) and **rsync**

`[yum|dnf] install yum-utils rsync`


# Local Image Builds

### Install ImageBuilder

Some images are built with `imagebuilder` instead of `docker build` (this is configured in the image's config yaml) and therefore you need to install `imagebuilder`:

 `go get -u github.com/openshift/imagebuilder/cmd/imagebuilder`

*Note*: by default go "gets" binaries into $GOPATH/bin, which you may want to put in your $PATH. If there is no $GOPATH golang defaults to $HOME/go

### Registry Setup

Because many images require resources on internal registries, it is important to setup your local docker to search for images in the right place.
Note: This is just one example and your mileage may vary depending on your system and tool versions

- Add the following entry to your `/etc/docker/daemon.json`:

    `"insecure-registries": ["http://brew-pulp-docker01.web.prod.ext.phx2.redhat.com:8888/"]`

- In `/etc/containers/registries.conf ` add `http://brew-pulp-docker01.web.prod.ext.phx2.redhat.com:8888` to both `[registries.search:registries]` and `[registries.insecure:registries]`. We recommend adding it to the beginning of the list if possible so that docker does not pull public versions of an image.

- Restart docker
