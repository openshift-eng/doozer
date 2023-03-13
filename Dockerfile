FROM openshift/ose-cli:latest as cli

FROM rhel7:7-released
WORKDIR /doozer
COPY --from=cli /bin/oc /bin/oc
ADD ./certs/RH-IT-Root-CA.crt /etc/pki/ca-trust/source/anchors/
COPY . .
RUN update-ca-trust && \
    yum-config-manager --add-repo=https://gitlab.cee.redhat.com/platform-eng-core-services/internal-repos/raw/master/rhel/rhel-7.repo && \
    yum-config-manager --add-repo=http://download.devel.redhat.com/rel-eng/RCMTOOLS/rcm-tools-rhel-7-server.repo && \
    yum-config-manager --enable rhel-7-server-optional-rpms && \
    yum-config-manager --enable rhel-7-server-rpms && \
    yum-config-manager --enable rhel-7-server-extras-rpms && \
    yum-config-manager --enable rcm-tools-rhel-7-server-optional-rpms && \
    yum-config-manager --enable rcm-tools-rhel-7-server-rpms && \
    curl -O https://dl.fedoraproject.org/pub/epel/RPM-GPG-KEY-EPEL-7 && \
    rpm --import RPM-GPG-KEY-EPEL-7 && \
    curl -O https://download.devel.redhat.com/rel-eng/RCMTOOLS/RPM-GPG-KEY-rcminternal && \
    rpm --import RPM-GPG-KEY-rcminternal && \
    curl -O https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm && \
    yum install -y epel-release-latest-7.noarch.rpm && \
    INSTALL_PKGS="podman git tito koji python3-brewkoji rhpkg krb5-devel python-devel python3-pip gcc" && \
    yum install -y $INSTALL_PKGS && \
    rpm -V $INSTALL_PKGS && \
    yum clean all && \
    pip install -r ./requirements.txt && \
    python setup.py install && \
    mkdir "/working"

ENTRYPOINT [ "./entrypoint.sh" ]


LABEL io.k8s.display-name="Doozer Client" \
      summary="Doozer is a client for managing and building groups of containers" \
      io.k8s.description="Doozer is a client for managing and building groups of containers" \
      description="Doozer is a client for managing and building groups of containers" \
      io.openshift.tags="openshift" \
      authoritative-source-url="https://github.com/openshift-eng/doozer" \
      url="https://github.com/openshift-eng/doozer" \
      name="doozer"

# version information is set at build time
