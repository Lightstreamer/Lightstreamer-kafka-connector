#!/bin/bash
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

TMP_DIR="${SCRIPT_DIR}/tmp"
GRADLEW="${KAFKA_PROJECT_DIR}/gradlew"

echo "Building Lightstreamer Kafka Connector Docker image version ${VERSION}"

echo "Step 1: Creating distribution package..."
"${GRADLEW}" --project-dir "${KAFKA_PROJECT_DIR}" adapterDistZip

echo "Step 2: Copying distribution to tmp/..."
DIST_ZIP="${KAFKA_PROJECT_DIR}/kafka-connector/build/distributions/lightstreamer-kafka-connector-${VERSION}.zip"

if [ ! -f "$DIST_ZIP" ]; then
    echo "Error: Distribution zip not found at ${DIST_ZIP}"
    exit 1
fi

mkdir -p "${TMP_DIR}"
cp "${DIST_ZIP}" "${TMP_DIR}/"

echo "Step 3: Building Docker image..."
BUILD_DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

docker build \
    -t "${IMAGE_NAME}:${VERSION}" \
    -t "${IMAGE_NAME}:latest" \
    --build-arg VERSION="${VERSION}" \
    --build-arg BUILD_DATE="${BUILD_DATE}" \
    "${SCRIPT_DIR}"

# Output version for CI/CD pipelines
if [ -n "${GITHUB_OUTPUT:-}" ]; then
    echo "VERSION=${VERSION}" >> "$GITHUB_OUTPUT"
fi
echo ""
echo "✓ Docker image built successfully!"
echo ""
echo "Image: ${IMAGE_NAME}:${VERSION}"
echo "       ${IMAGE_NAME}:latest"
echo ""
echo "Launch locally with:"
echo "  docker run --name kafka-connector -d -p 8080:8080 ${IMAGE_NAME}:${VERSION}"
echo ""
