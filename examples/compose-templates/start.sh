#!/bin/bash
source ../utils/helpers.sh

# Build the Lightstreamer Kafka Connector Docker image
../docker/build.sh

if [ $? == 0 ]; then
     # Generate the producer jar
     $_gradle distribuiteProducer
     rm -fr ../compose-templates/producer; mkdir -p ../compose-templates/producer
     cp ../../deploy/lightstreamer-kafka-connector-samples-producer-all-${version}.jar ../compose-templates/producer
     # Export the version env variable to be used by Compose
     export version
     docker compose -f $(pwd)/docker-compose.yml up --build -d &&
     sleep 10 && 
     echo "Service started. Now you can point your browser to http://localhost:8080/QuickStart to see real-time data."
 fi