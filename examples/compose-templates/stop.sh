#!/bin/bash
source ./helpers.sh

# Export the version env variable to be used by Compose
export version
docker compose down
