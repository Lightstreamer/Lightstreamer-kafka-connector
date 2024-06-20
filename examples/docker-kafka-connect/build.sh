#!/bin/bash
source ../utils/helpers.sh
SCRIPT_DIR=$(dirname ${BASH_SOURCE[0]})
TMP_DIR=${SCRIPT_DIR}/tmp

# Generate the deployment package
echo "Making the deployment package"
$_gradle distribuiteConnect

mkdir -p ${TMP_DIR}
cp ${projectDir}/kafka-connector/build/distributions/lightstreamer-kafka-connect-lightstreamer-${version}.zip ${TMP_DIR}

echo "Build the Connect Docker image"
docker build -t lightstreamer-kafka-connect-lighstreamer-${version} $SCRIPT_DIR --build-arg VERSION=${version}

if [ $? == 0 ]; then
    echo "Launch the image with:"
    echo "docker run --name lightstreamer-kafka-connect-lighstreamer -d -p 8080:8080 lightstreamer-kafka-connect-lighstreamer-${version}"
else
    exit 1
fi
