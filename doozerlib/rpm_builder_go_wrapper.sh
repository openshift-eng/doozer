#!/bin/sh
export GOEXPERIMENT=strictfipsruntime

LOG_PREFIX="Go compliance shim [${__doozer_group}][${__doozer_key}]:"
echoerr() {
  echo -n "${LOG_PREFIX} " 1>&2
  cat <<< "$@" 1>&2
}

set -o xtrace

echo 1>&2
echo 1>&2
echo -n "${LOG_PREFIX} incoming command line arguments: " 1>&2
for arg in "$@"; do
  echo -n "\"${arg}\" " 1>&2
done
echo 1>&2
echo 1>&2

echoerr "----"
echoerr "incoming environment: "
echoerr "---------------------"
env 1>&2
echoerr "---------------------"
echo 1>&2

if [[ "${CGO_ENABLED}" == "0" ]]; then
    echoerr "non-compliant RPM attempted to use CGO_ENABLED=${CGO_ENABLED}"
fi

if [[ "${GOOS}" == "darwin" ]]; then
    echoerr "permitting CGO_ENABLED=${CGO_ENABLED} because of GOOS=${GOOS}"
else
    export CGO_ENABLED="1"
    echoerr "forcing CGO_ENABLED=${CGO_ENABLED}"
fi

HAS_TAGS=0
if cat <<< "$@" | grep "\-tags" > /dev/null; then
  HAS_TAGS="1"
fi

FORCE_FOD_MODE=1
FORCE_OPENSSL=1
FORCE_DYNAMIC=1
IN_RUN=0
IN_TAGS=0
ARGS=()  # We need to rebuild the argument list.
# Compilation with -extldflags "-static" is problematic with
# CGO_ENABLED=1 because compilation tries to link against
# static libraries which don't exist. Remove -static flag
# when detected. This is tricky because extldflags can be simple
# or something like -ldflags '-X $(REPO_PATH)/pkg/version.Raw=$(VERSION) -extldflags "-lm -lstdc++ -static"'
ARGS=()  # We need to rebuild the argument list.
for arg in "$@"; do
    if [[ "${arg}" == "run" ]]; then
      IN_RUN="1"
    fi

    # prior to detecting 'IN_RUN', grafana failed because it ran
    # 'go run build.go build' which caused the script to exec 'go run build.go build -tags strictfipsruntime'
    if [[ "${arg}" == "build" && "${IN_RUN}" == "0" ]]; then
      # -tags apparently cannot come after a build path
      # e.g. "build ./cmd/cluster-openshift-apiserver-operator -tags strictfipsruntime" is invalid.
      # So, if we see "build" and no "-tags" ahead, then go ahead and force FOD tag.

      ARGS+=("${arg}") # Add "build"
      if [[ "${FORCE_FOD_MODE}" == "1" && "${HAS_TAGS}" == "0" ]]; then
        ARGS+=("-tags")
        ARGS+=("strictfipsruntime")
      fi
      continue  # We've already added 'build', so don't reach the bottom of the loop where it would be added again.
    fi

    if [[ ( "${arg}" == "-tags="* || "${arg}" == "--tags="* ) && "${FORCE_FOD_MODE}" == "1" ]]; then
      echoerr "adding strictfipsruntime tag to \"${arg}\""
      arg=$(echo "${arg}" | tr -d "'" | tr -d "\"")  # Delete any quotes which get passed in literally. grafana managed this.
      if [[ "${arg}" == *" "* ]]; then  # If the tags parameter is space delimited
        arg="${arg} strictfipsruntime"
      else
        # Assume comma delimited
        arg="${arg},strictfipsruntime"
      fi
    fi

    if [[ "${IN_TAGS}" == "1" ]]; then
      if [[ "${FORCE_FOD_MODE}" == "1" ]]; then
        echoerr "adding strictfipsruntime tag to ${arg} (IN_TAGS=${IN_TAGS})"
        arg=$(echo "${arg}" | tr -d "'" | tr -d "\"")  # Delete any quotes which get passed in literally. prom-label-proxy managed this.
        if [[ "${arg}" == *" "* ]]; then  # If the tags parameter is space delimited
          arg="${arg} strictfipsruntime"
        else
          # Assume comma delimited
          arg="${arg},strictfipsruntime"
        fi
      fi
      if [[ "${FORCE_OPENSSL}" == "1" ]]; then
        pre_arg="${arg}"
        arg=$(echo "${arg}" | sed 's/no_openssl/shim_prevented_no_openssl/g')
        if [[ "${pre_arg}" != "${arg}" ]]; then
          echoerr "non-compliant: eliminated no_openssl"
        fi
      fi
      IN_TAGS=0
    fi

    if [[ "${arg}" == "-tags" || "${arg}" == "--tags" ]]; then
      IN_TAGS=1
    fi

    # Compilation with -extldflags "-static" is problematic with
    # CGO_ENABLED=1 because compilation tries to link against
    # static libraries which don't exist. Remove -static flag
    # when detected. This is tricky because extldflags can be simple
    # or something like -ldflags '-X $(REPO_PATH)/pkg/version.Raw=$(VERSION) -extldflags "-lm -lstdc++ -static"'
    # Note that extldflags is a flag embedded within the value of the
    # -ldflags argument. From our script's perspective, it will be part of a single
    # argument, but this argument might look like '-X $(REPO_PATH)/pkg/version.Raw=$(VERSION) -extldflags "-lm -lstdc++ -static"'.
    if [[ "${arg}" == *"-extldflags"* && "${FORCE_DYNAMIC}" == "1" ]]; then
      # We replace -static with -lc because '-lc' implies to link against stdlib. This is a default
      # and should therefore be benign for virtually any compilation (unless -nostdlib or -nodefaultlibs
      # is specified -- and we don't account for this).
      # Why replace instead of remove? Consider the complex possible scenarios:
      # -ldflags '-extldflags "-static"'   # Removing -extldflags would mean we also need to remove ldflags.
      # -ldflags '-X $(REPO_PATH)/pkg/version.Raw=$(VERSION) -extldflags "-static"'  # Would remove extldflags but keep ldflags
      # -ldflags '-X $(REPO_PATH)/pkg/version.Raw=$(VERSION) -extldflags "-static -lm"'  # Would need to remove -static but keep extldflags
      # In any scenario, replacing "-static" with something benign should work without the need for complex logic.
      pre_arg="${arg}"
      arg=$(echo "${arg}" | sed "s/-static/-lc/g")
      if [[ "${pre_arg}" != "${arg}" ]]; then
        echoerr "non-compliant: eliminated static"
      fi
    fi
    ARGS+=("${arg}")
done

echo 1>&2
echo 1>&2
echo -n "${LOG_PREFIX} final command line arguments: " 1>&2
for arg in "${ARGS[@]}"; do
  echo -n "\"${arg}\" " 1>&2
done
echo 1>&2
echo 1>&2

echoerr "Invoking actual go binary"
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd ) # Get the directory in which this script is executing
${SCRIPT_DIR}/go.real "${ARGS[@]}"
