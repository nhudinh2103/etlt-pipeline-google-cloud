#!/bin/bash

# Get the current git commit hash
#GIT_COMMIT=$(git rev-parse --short HEAD)
IMAGE_VERSION=0.0.1
DOCKER_IMAGE=github-etl-cicd:$IMAGE_VERSION

# Build the Docker image with latest tag (for simplicity versioning)
echo "Building Docker image with latest tag"
docker build .. -t $DOCKER_IMAGE -f Dockerfile

echo "Build complete!"
echo "You can run the image using: docker run $DOCKER_IMAGE"
