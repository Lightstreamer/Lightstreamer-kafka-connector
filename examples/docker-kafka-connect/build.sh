#!/bin/bash
set -eu
source ../utils/helpers.sh
SCRIPT_DIR=$(dirname ${BASH_SOURCE[0]})
TMP_DIR=${SCRIPT_DIR}/tmp

# Generate the deployment package
echo "Making the deployment package"
$_gradle clean connectDistZip

mkdir -p ${TMP_DIR}
rm -fr ${TMP_DIR}/
unzip ${projectDir}/kafka-connector/build/distributions/lightstreamer-kafka-connect-lightstreamer-${version}.zip -d ${TMP_DIR}

echo "Building the Kafka Connect Lightstreamer Sink Connector Docker image"
docker build -t kafka-connect-lightstreamer-${version} $SCRIPT_DIR --build-arg VERSION=${version} "$@"
