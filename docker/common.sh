#!/bin/bash
# Common configuration shared by build.sh and push.sh
# This file should be sourced, not executed directly

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
KAFKA_PROJECT_DIR="${PROJECT_ROOT}/kafka-connector-project"

IMAGE_NAME="lightstreamer-kafka-connector"

VERSION=$(grep '^version=' "${KAFKA_PROJECT_DIR}/gradle.properties" | cut -d'=' -f2 | tr -d '[:space:]')

if [ -z "$VERSION" ]; then
    echo "Error: Could not determine version from gradle.properties" >&2
    exit 1
fi
