#!/bin/bash
source ../utils/helpers.sh
SCRIPT_DIR=$(dirname ${BASH_SOURCE[0]})
TMP_DIR=${SCRIPT_DIR}/tmp

# Generate the distribution
echo "Making the distribution package"
$_gradle distribuite

rm -fr  ${TMP_DIR}; mkdir ${TMP_DIR}
cp ${projectDir}/deploy/lightstreamer-kafka-connector-${version}.zip ${TMP_DIR}

echo "Build the Lightstramer Kafka Connector Docker image"
docker build -t lightstreamer-kafka-connector-${version} $SCRIPT_DIR --build-arg VERSION=${version}

if [ $? == 0 ]; then
    echo "Launch the image with:"
    echo "docker run --name kafka-connector -d -p 8080:8080 lightstreamer-kafka-connector-${version}"
fi