#!/bin/bash
source ../utils/helpers.sh
rm -fr tmp; mkdir -p tmp

# Build the Lightstreamer Kafka Connector Docker image
../docker-image/build.sh

if [ $? == 0 ]; then
     cp ../../deploy/lightstreamer-kafka-connector-samples-all-${version}.jar tmp/
     # Export the version env variable to be used by Compose
     export version
     docker compose up --build -d
     sleep 20
     echo "Service started. Now you can point your browers to http://localhost:8080/QuickStart to see real-time data."
 fi