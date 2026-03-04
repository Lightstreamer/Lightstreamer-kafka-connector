#!/bin/bash
set -euo pipefail
WHICH=$1
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../../" && pwd)"

# Load version from shared location
source "${PROJECT_ROOT}/kafka-connector-project/version.sh"

COMPOSE_FILE="docker-compose-${WHICH}.yml"

# Build the Lightstreamer Kafka Connector Docker image
${PROJECT_ROOT}/docker/build.sh

if [ $? == 0 ]; then
     export VERSION
     docker compose -f ${COMPOSE_FILE} up --build -d
     echo "Services started. Now you can point your browser to http://localhost:8080/AirportDemo to see real-time data."
 fi