#!/bin/bash
cwd=$(pwd)

# Build the Lightstreamer Kafka Connector Docker image
cd ../docker-image
./build.sh

if [ $? == 0 ]; then
    cd ../..
    # Deploy the Kafka Producer to the tmp folder
    ./gradlew QuickStart
    cd $cwd
    docker compose up --build 
fi