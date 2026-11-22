#!/bin/sh -eu

if [ -z "${DOCKERHUB_USERNAME:-}" ]; then
  echo "Error: DOCKERHUB_USERNAME is not set"
  exit 1
fi

if [ -z "${DOCKERHUB_TOKEN:-}" ]; then
  echo "Error: DOCKERHUB_TOKEN is not set"
  exit 1
fi

IMAGE_NAME="pgsync"
PLATFORMS="${PLATFORMS:-linux/amd64,linux/arm64}"
IMAGE="${DOCKERHUB_USERNAME}/${IMAGE_NAME}"

# Tag the image with the full, minor and major version, e.g. 8.0.0, 8.0 and 8
VERSION="${VERSION:-$(sed -n 's/^__version__ = "\(.*\)"/\1/p' pgsync/__init__.py)}"
if [ -z "${VERSION}" ]; then
  echo "Error: cannot read the version from pgsync/__init__.py"
  exit 1
fi
MAJOR="${VERSION%%.*}"
MINOR="${VERSION%.*}"

# Set LATEST=false to publish an older release without moving the latest tag
LATEST="${LATEST:-true}"

TAGS="--tag ${IMAGE}:${VERSION} --tag ${IMAGE}:${MINOR} --tag ${IMAGE}:${MAJOR}"
if [ "${LATEST}" = "true" ]; then
  TAGS="${TAGS} --tag ${IMAGE}:latest"
fi

if ! docker buildx version >/dev/null 2>&1; then
  echo "Error: Docker Buildx is required to publish a multi-platform image"
  exit 1
fi

echo "Logging into Docker Hub..."
echo "${DOCKERHUB_TOKEN}" | docker login -u "${DOCKERHUB_USERNAME}" --password-stdin

echo "Building and publishing ${IMAGE} ${VERSION} for ${PLATFORMS}..."
# shellcheck disable=SC2086
docker buildx build \
  --platform "${PLATFORMS}" \
  ${TAGS} \
  --file Dockerfile.dockerhub \
  --push \
  .

echo "Done."
