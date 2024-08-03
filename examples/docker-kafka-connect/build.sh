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

echo "Build the Connect Docker image"
docker build -t lightstreamer-kafka-connect-lighstreamer-${version} $SCRIPT_DIR --build-arg VERSION=${version} "$@"

echo "Launch the image with:"
echo "docker run --name lightstreamer-kafka-connect-lighstreamer -d lightstreamer-kafka-connect-lighstreamer-${version}"
