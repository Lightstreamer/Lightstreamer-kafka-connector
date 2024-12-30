#!/bin/bash
source ./helpers.sh

# Build the Lightstreamer Kafka Connector Docker image
./docker/build.sh

if [ $? == 0 ]; then
     export version
     docker compose -f $(pwd)/docker-compose.yml up --build -d &&
     sleep 10 && 
     echo "Services started. Now you can point your browser to http://localhost:8080/QuickStart to see real-time data."
 fi