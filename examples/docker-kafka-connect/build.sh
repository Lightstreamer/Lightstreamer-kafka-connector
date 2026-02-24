#!/bin/bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../" && pwd)"

# Load version from shared location
source "${PROJECT_ROOT}/kafka-connector-project/version.sh"

TMP_DIR=${SCRIPT_DIR}/tmp
GRADLEW="${KAFKA_PROJECT_DIR}/gradlew"

echo "Building Lightstreamer Kafka Connector Docker image version ${VERSION}"

echo "Step 1: Creating distribution package..."
"${GRADLEW}" --project-dir "${KAFKA_PROJECT_DIR}" connectDistZip

echo "Step 2: Copying distribution to tmp/..."
DIST_ZIP="${KAFKA_PROJECT_DIR}/kafka-connector/build/distributions/lightstreamer-kafka-connect-lightstreamer-${VERSION}.zip"

rm -fr "${TMP_DIR}"
mkdir -p "${TMP_DIR}"
unzip "${DIST_ZIP}" -d "${TMP_DIR}"

echo "Step 3: Building Docker image..." 
docker build \
    -t kafka-connect-lightstreamer-${VERSION} \
    -t kafka-connect-lightstreamer:latest \
    --build-arg VERSION="${VERSION}" \
    "$SCRIPT_DIR" \
    
