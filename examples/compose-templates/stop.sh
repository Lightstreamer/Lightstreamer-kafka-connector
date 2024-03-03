#!/bin/bash
source ../utils/helpers.sh

# Export the version env variable to be used by Compose
export version
docker compose down
rm -fr ../compose-templates/producer
$_gradle clean