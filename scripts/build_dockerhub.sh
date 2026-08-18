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
TAG="${TAG:-latest}"
PLATFORMS="${PLATFORMS:-linux/amd64,linux/arm64}"
IMAGE="${DOCKERHUB_USERNAME}/${IMAGE_NAME}:${TAG}"

if ! docker buildx version >/dev/null 2>&1; then
  echo "Error: Docker Buildx is required to publish a multi-platform image"
  exit 1
fi

echo "Logging into Docker Hub..."
echo "${DOCKERHUB_TOKEN}" | docker login -u "${DOCKERHUB_USERNAME}" --password-stdin

echo "Building and publishing ${IMAGE} for ${PLATFORMS}..."
docker buildx build \
  --platform "${PLATFORMS}" \
  --tag "${IMAGE}" \
  --file Dockerfile.dockerhub \
  --push \
  .

echo "Done."
