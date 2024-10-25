#!/bin/bash
projectDir="${PWD%/examples/*}/kafka-connector-project"

# Alias to the the local gradlew command
_gradle="${projectDir}/gradlew --project-dir ${projectDir}"

# Get the current version
version=$(grep '^version' ${projectDir}/gradle.properties |  awk -F= '{ printf $2}')
