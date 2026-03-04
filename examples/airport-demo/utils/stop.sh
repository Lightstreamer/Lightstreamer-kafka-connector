#!/bin/bash
set -euo pipefail
WHICH=$1
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../../" && pwd)"

# Load version from shared location
source "${PROJECT_ROOT}/kafka-connector-project/version.sh"

COMPOSE_FILE="docker-compose-${WHICH}.yml"

# Export the version env variable to be used by Compose
export VERSION
docker compose -f ${COMPOSE_FILE} down
