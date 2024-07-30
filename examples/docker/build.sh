#!/bin/bash
set -eu
source ../utils/helpers.sh
SCRIPT_DIR=$(dirname ${BASH_SOURCE[0]})
TMP_DIR=${SCRIPT_DIR}/tmp

# Generate the deployment package
echo "Making the deployment package"
$_gradle distribute

mkdir -p ${TMP_DIR}
cp ${projectDir}/kafka-connector/build/distributions/lightstreamer-kafka-connector-${version}.zip ${TMP_DIR}

echo "Build the Lightstramer Kafka Connector Docker image"
docker build -t lightstreamer-kafka-connector-${version} $SCRIPT_DIR --build-arg VERSION=${version}

echo "Launch the image with:"
echo "docker run --name kafka-connector -d -p 8080:8080 lightstreamer-kafka-connector-${version}"
