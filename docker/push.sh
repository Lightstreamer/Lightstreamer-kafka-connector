#!/bin/bash
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

# Registry can be overridden via environment variable (format: registry.example.com/organization)
if [ -z "${REGISTRY:-}" ]; then
    echo "Warning: REGISTRY environment variable not set"
    echo "Using default: ghcr.io/lightstreamer"
    echo ""
    REGISTRY="ghcr.io/lightstreamer"
fi

LOCAL_IMAGE="${IMAGE_NAME}-${VERSION}"
REMOTE_IMAGE_VERSION="${REGISTRY}/${IMAGE_NAME}:${VERSION}"
REMOTE_IMAGE_LATEST="${REGISTRY}/${IMAGE_NAME}:latest"

if ! docker image inspect "${LOCAL_IMAGE}" &>/dev/null; then
    echo "Error: Local image '${LOCAL_IMAGE}' not found"
    echo "Please run ./build.sh first"
    exit 1
fi

echo "Pushing ${IMAGE_NAME} version ${VERSION} to ${REGISTRY}"

echo "Step 1: Tagging images..."
docker tag "${LOCAL_IMAGE}" "${REMOTE_IMAGE_VERSION}"
docker tag "${LOCAL_IMAGE}" "${REMOTE_IMAGE_LATEST}"

echo "Step 2: Pushing to registry..."
docker push "${REMOTE_IMAGE_VERSION}"
docker push "${REMOTE_IMAGE_LATEST}"
echo ""
echo "✓ Successfully pushed images!"
echo ""
echo "Published images:"
echo "  - ${REMOTE_IMAGE_VERSION}"
echo "  - ${REMOTE_IMAGE_LATEST}"
echo ""
echo "Pull with:"
echo "  docker pull ${REMOTE_IMAGE_VERSION}"
echo ""
