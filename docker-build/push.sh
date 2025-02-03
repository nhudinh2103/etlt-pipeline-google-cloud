#!/bin/bash

# Check if project ID is provided
if [ -z "$1" ]; then
    echo "Error: GCP project ID is required"
    echo "Usage: ./push.sh [GCP_PROJECT_ID]"
    exit 1
fi

PROJECT_ID=$1
IMAGE_VERSION=0.0.1
GCR_IMAGE="gcr.io/$PROJECT_ID/airflow-github-etl"

# Ensure user is authenticated with GCP
echo "Ensuring authentication with GCP..."
if ! gcloud auth configure-docker; then
    echo "Error: Failed to configure docker authentication"
    exit 1
fi

# Tag images for GCR
echo "Tagging images for GCR..."
docker tag airflow-github-etl:$IMAGE_VERSION $GCR_IMAGE:$IMAGE_VERSION

# Push images to GCR
echo "Pushing images to GCR..."
docker push $GCR_IMAGE:$IMAGE_VERSION

echo "Push complete!"
echo "Images pushed to:"
echo "- $GCR_IMAGE:$IMAGE_VERSION"
