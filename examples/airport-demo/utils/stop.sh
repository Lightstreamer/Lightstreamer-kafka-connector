#!/bin/bash
WHICH=$1
COMPOSE_FILE="$(pwd)/docker-compose-${WHICH}.yml"

source ../utils/helpers.sh

# Export the version env variable to be used by Compose
export version
docker compose -f ${COMPOSE_FILE} down
