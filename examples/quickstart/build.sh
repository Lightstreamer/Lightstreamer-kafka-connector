#!/bin/bash
DEPLOY_DIR=$(pwd)/deploy
rm -fr $DEPLOY_DIR || true
mkdir $DEPLOY_DIR
cd ../..
./gradlew distribuite

cp deploy/lightstreamer-kafka-connector-0.1.0.zip $DEPLOY_DIR
cp deploy/lightstreamer-kafka-connector-samples-all-0.1.0.jar $DEPLOY_DIR
docker compose -f examples/quickstart/docker-compose.yaml up --remove-orphans
