#!/bin/bash
HELPER_DIR=$(dirname ${BASH_SOURCE[0]}) ## realpath
projectDir="$HELPER_DIR/../.."

# Alias to the the local gradlew command
_gradle="${projectDir}/gradlew --project-dir ${projectDir}"

# Get the current version
version=$(grep '^version' ${projectDir}/gradle.properties |  awk -F= '{ printf $2}')
