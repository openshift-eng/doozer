#!/usr/bin/env bash

KERB="/kerb"

if [ ! -f ${KERB} ]; then
    echo "Count not find a Kerberos cache at ${KERB}"
    echo "Please use `-v [LOCAL_KERB]:${KERB}` when starting container"
    exit 1
fi

if [ ! -s ${KERB} ]; then
    echo "Kerberos cache at ${KERB} was mounted but empty"
    echo "Please mount a valid Kerberos ticket"
    exit 1
fi

# set working dir
export DOOZER_WORKING_DIR="/working"

# setup mount point for kerb auth
export KRB5CCNAME="FILE:${KERB}"

# run doozer
doozer "$@"
