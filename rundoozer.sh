#!/usr/bin/env bash

podman run \
-v "/tmp/krb5cc_${UID}":/kerb \
-v ~/src/workspace:/working \
-v ~/.ssh:/root/.ssh/ \
-v ~/.gitconfig:/root/.gitconfig \
--rm \
-it \
doozer "$@"