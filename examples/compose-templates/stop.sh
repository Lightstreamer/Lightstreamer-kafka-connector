#!/bin/bash

# Resolve symlink and find project root (from compose-templates, go up 2 levels)
SCRIPT_PATH="$(readlink -f "${BASH_SOURCE[0]}")"
PROJECT_ROOT="$(cd "$(dirname "$SCRIPT_PATH")/../.." && pwd)"

# Load version from shared location
source "${PROJECT_ROOT}/kafka-connector-project/version.sh"

# Export the version env variable to be used by Compose
export VERSION
docker compose down
