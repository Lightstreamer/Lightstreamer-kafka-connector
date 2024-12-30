#!/bin/bash
WHICH=$1
COMPOSE_FILE="$(pwd)/docker-compose-${WHICH}.yml"

source ../utils/helpers.sh

# Build the Lightstreamer Kafka Connector Docker image
cd docker
./build.sh

if [ $? == 0 ]; then
     export version
     docker compose -f ${COMPOSE_FILE} up --build -d &&
     echo "Services started. Now you can point your browser to http://localhost:8080/AirportDemo to see real-time data."
 fi