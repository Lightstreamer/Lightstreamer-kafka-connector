#!/bin/bash
source ../utils/helpers.sh
rm -fr tmp; mkdir -p tmp

# Build the Lightstreamer Kafka Connector Docker image
../docker/build.sh

if [ $? == 0 ]; then
    #  # Generate the producer jar
     $_gradle distribuiteProducer
     cp ../../deploy/lightstreamer-kafka-connector-samples-producer-all-${version}.jar tmp/
     # Export the version env variable to be used by Compose
     export version
     docker compose up --build -d
     sleep 20
     echo "Service started. Now you can point your browser to http://localhost:8080/QuickStart to see real-time data."
 fi