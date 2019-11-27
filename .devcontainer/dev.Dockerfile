FROM fedora:30

ENV PYCURL_SSL_LIBRARY=openssl

RUN dnf install -y \
    # runtime dependencies
    krb5-workstation git rsync skopeo podman docker \
    python2 python2-certifi python2-rpm \
    python3 python3-certifi python3-rpm \
    # development dependencies
    gcc krb5-devel openssl-devel \
    python2-devel python2-pip \
    python3-devel python3-pip \
    # other tools
    bash-completion vim tmux procps-ng psmisc wget curl net-tools iproute \
  # Red Hat IT Root CA
  && curl -o /etc/pki/ca-trust/source/anchors/RH-IT-Root-CA.crt --fail -L \
    https://password.corp.redhat.com/RH-IT-Root-CA.crt \
  && update-ca-trust extract \
  # clean up
  && dnf clean all

# install brewkoji
RUN wget -O /etc/yum.repos.d/rcm-tools-fedora.repo https://download.devel.redhat.com/rel-eng/RCMTOOLS/rcm-tools-fedora.repo \
  && dnf install -y koji brewkoji \
  && dnf install -y rhpkg \
  && dnf clean all

ARG OC_VERSION=4.2.8
# include oc client
RUN wget -O /tmp/openshift-client-linux-"$OC_VERSION".tar.gz https://mirror.openshift.com/pub/openshift-v4/clients/ocp/"$OC_VERSION"/openshift-client-linux-"$OC_VERSION".tar.gz \
  && tar -C /usr/local/bin -xzf  /tmp/openshift-client-linux-"$OC_VERSION".tar.gz oc kubectl \
  && rm /tmp/openshift-client-linux-"$OC_VERSION".tar.gz

# Create a non-root user - see https://aka.ms/vscode-remote/containers/non-root-user.
ARG USERNAME=dev
# On Linux, replace with your actual UID, GID if not the default 1000
ARG USER_UID=1000
ARG USER_GID=$USER_UID

# Create the "dev" user
RUN groupadd --gid "$USER_GID" "$USERNAME" \
    && useradd --uid "$USER_UID" --gid "$USER_GID" -m "$USERNAME" \
    && mkdir -p /home/"$USERNAME"/.vscode-server /home/"$USERNAME"/.vscode-server-insiders \
    && chown -R "${USER_UID}:${USER_GID}" /home/"$USERNAME" \
    && chmod 0755 /home/"$USERNAME" \
    && echo "$USERNAME" ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/"$USERNAME" \
    && chmod 0440 /etc/sudoers.d/"$USERNAME"

# Configure Kerberos
COPY .devcontainer/krb5-redhat.conf /etc/krb5.conf.d/

USER "$USER_UID"
