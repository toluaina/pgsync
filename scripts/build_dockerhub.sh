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
TAG="latest"

echo "Building Docker image..."
docker build -t ${DOCKERHUB_USERNAME}/${IMAGE_NAME}:${TAG} -f Dockerfile.dockerhub .

echo "Logging into Docker Hub..."
echo "${DOCKERHUB_TOKEN}" | docker login -u "${DOCKERHUB_USERNAME}" --password-stdin

# echo "Pushing image to Docker Hub..."
docker push "${DOCKERHUB_USERNAME}/${IMAGE_NAME}:${TAG}"

echo "Done."
