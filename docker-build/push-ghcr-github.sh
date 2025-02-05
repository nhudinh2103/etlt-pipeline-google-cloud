#!/bin/bash

# Exit on error
set -e

# Check if GITHUB_TOKEN is provided
if [ -z "$GITHUB_TOKEN" ]; then
    echo "Error: GITHUB_TOKEN environment variable is not set"
    echo "Please set it with: export GITHUB_TOKEN=your_github_token"
    exit 1
fi

# Check if GITHUB_USERNAME is provided
if [ -z "$GITHUB_USERNAME" ]; then
    echo "Error: GITHUB_USERNAME environment variable is not set"
    echo "Please set it with: export GITHUB_USERNAME=your_github_username"
    exit 1
fi

# Image configuration
IMAGE_VERSION=0.0.1
IMAGE_NAME="github-etl-cicd"
GHCR_REPO="ghcr.io/$GITHUB_USERNAME/$IMAGE_NAME"

# Login to GitHub Container Registry
echo "Logging in to GitHub Container Registry..."
echo "$GITHUB_TOKEN" | docker login ghcr.io -u "$GITHUB_USERNAME" --password-stdin

# Tag the image for GHCR
echo "Tagging image for GHCR..."
docker tag "$IMAGE_NAME:$IMAGE_VERSION" "$GHCR_REPO:$IMAGE_VERSION"

# Push the images
echo "Pushing images to GHCR..."
docker push "$GHCR_REPO:$IMAGE_VERSION"

echo "Successfully pushed images to GitHub Container Registry:"
echo "- $GHCR_REPO:$IMAGE_VERSION"
