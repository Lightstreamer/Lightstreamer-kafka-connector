#!/bin/bash
# Common configuration shared by build.sh and push.sh
# This file should be sourced, not executed directly

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Load version from shared location
source "${PROJECT_ROOT}/kafka-connector-project/version.sh"

IMAGE_NAME="lightstreamer-kafka-connector"
