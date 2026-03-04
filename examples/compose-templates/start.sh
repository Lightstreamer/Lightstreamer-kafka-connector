#!/bin/bash
set -euo pipefail
# Resolve symlink and find project root (from compose-templates, go up 2 levels)
SCRIPT_PATH="$(readlink -f "${BASH_SOURCE[0]}")"
PROJECT_ROOT="$(cd "$(dirname "$SCRIPT_PATH")/../.." && pwd)"

# Load version from shared location
source "${PROJECT_ROOT}/kafka-connector-project/version.sh"

# Build the Lightstreamer Kafka Connector Docker image
"${PROJECT_ROOT}/docker/build.sh"

if [ $? == 0 ]; then
    # Export the version env variable to be used by Compose
    export VERSION
    docker compose -f $(pwd)/docker-compose.yml up --build -d
    sleep 4
    echo "Services started. Now you can point your browser to http://localhost:8080/QuickStart to see real-time data."
fi
 