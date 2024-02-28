#!/bin/bash

# Set the root project directory
projectDir=../../

# Alias to the local gradlew command
_gradle="${projectDir}/gradlew --project-dir ${projectDir}"

# Generate the distribution
echo "Making the distribution"
$_gradle distribuite

# Get the current version
version=$($_gradle properties -q --console=plain | grep '^version:' | awk '{ printf $2}')

mkdir -p tmp
cp ${projectDir}/deploy/lightstreamer-kafka-connector-${version}.zip tmp/

echo "Build the Lightstramer Kafka Connector Docker image"
docker build -t ligthstreamer-kafka-connector-${version} . --build-arg VERSION=${version}

if [ $? == 0 ]; then
    echo "Launch the image with:"
    echo "docker run --name kafka-connector -d -p 8080:8080 ligthstreamer-kafka-connector-${version}"
fi
