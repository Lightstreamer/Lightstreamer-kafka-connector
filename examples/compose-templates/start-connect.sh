#!/bin/bash

# Load version from shared location
source ../../kafka-connector-project/version.sh

# Build the Lightstreamer Kafka Connector Docker image
../docker-kafka-connect/build.sh

echo "Version: ${VERSION}"
if [ $? == 0 ]; then
     export VERSION
     docker compose -f $(pwd)/docker-compose.yml up --build -d &&
     sleep 10 && 
     echo "Services started. Now you can point your browser to http://localhost:8080/QuickStart to see real-time data."
fi
