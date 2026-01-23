#!/usr/bin/env bash

set -ex
set -o pipefail

TAG=${TAG:-kubevirt-arm64}
REPO=${REPO:-registry.gitlab.com/sonaproject}
PUSH=${PUSH:-}

# support other container tools. e.g. podman
CONTAINER_CLI=${CONTAINER_CLI:-docker}
CONTAINER_BUILDER=${CONTAINER_BUILDER:-"buildx build"}

# If set, just building, no pushing
if [[ -z "${DRY_RUN:-}" ]]; then
  PUSH="--push"
fi

# supported platforms
PLATFORMS=linux/arm64

# shellcheck disable=SC2086 # inteneded splitting of CONTAINER_BUILDER
${CONTAINER_CLI} ${CONTAINER_BUILDER} \
  --platform ${PLATFORMS} \
  ${PUSH} \
  -f Dockerfile \
  --provenance=false \
  -t "${REPO}"/onos-sona-nightly-docker:"${TAG}" .
