#!/usr/bin/env bash

set -x
set -e

# enable misc repos needed for install
yum-config-manager --add-repo=https://gitlab.cee.redhat.com/platform-eng-core-services/internal-repos/raw/master/rhel/rhel-7.repo
yum-config-manager --add-repo=http://download.devel.redhat.com/rel-eng/RCMTOOLS/rcm-tools-rhel-7-server.repo
yum-config-manager --enable rhel-7-server-optional-rpms
yum-config-manager --enable rhel-7-server-rpms
yum-config-manager --enable rhel-7-server-extras-rpms
yum-config-manager --enable rcm-tools-rhel-7-server-optional-rpms
yum-config-manager --enable rcm-tools-rhel-7-server-rpms


# setup EPEL and rcm tools with keys
curl -O https://dl.fedoraproject.org/pub/epel/RPM-GPG-KEY-EPEL-7
rpm --import RPM-GPG-KEY-EPEL-7
curl -O https://download.devel.redhat.com/rel-eng/RCMTOOLS/RPM-GPG-KEY-rcminternal
rpm --import RPM-GPG-KEY-rcminternal
curl -O https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
yum install -y epel-release-latest-7.noarch.rpm

# this needs to come back when we revisit local building
# ARCH=$(uname -i)
# wget -O imagebuilder.rpm "http://download.eng.bos.redhat.com/brewroot/vol/rhel-7/packages/imagebuilder/0.0.5/1.el7eng/${ARCH}/imagebuilder-0.0.5-1.el7eng.${ARCH}.rpm"
# yum install -y imagebuilder.rpm

# install oc client
# rpm -Uvh http://download.eng.bos.redhat.com/rcm-guest/puddles/RHAOS/AtomicOpenShift/4.0/v4.0.0-0.185.0_2019-02-25.1/x86_64/os/Packages/atomic-openshift-clients-4.0.0-0.185.0.git.0.e883da2.el7.x86_64.rpm

# Installing misc dependencies...
INSTALL_PKGS="podman git tito koji python3-brewkoji rhpkg krb5-devel python-devel python3-pip gcc"
yum install -y $INSTALL_PKGS
rpm -V $INSTALL_PKGS
yum clean all


# install doozer py packages
pip install -r ./requirements.txt

# install doozer
python setup.py install

# pre-add dist-git key to known_hosts
# mkdir -p ~/.ssh
# chmod 700 ~/.ssh
# touch ~/.ssh/known_hosts
# chmod 644 ~/.ssh/known_hosts
# ssh-keyscan -t rsa,dsa,ecdsa -H pkgs.devel.redhat.com 2>&1 | tee -a ~/.ssh/known_hosts
# ssh-keyscan -t rsa,dsa,ecdsa -H github.com 2>&1 | tee -a ~/.ssh/known_hosts
# echo -e "Host github.com\n\tStrictHostKeyChecking no\n" >> ~/.ssh/config

WORKING_DIR="/working"
mkdir $WORKING_DIR
