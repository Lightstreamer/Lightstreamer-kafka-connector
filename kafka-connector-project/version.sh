#!/bin/bash
# Shared version extraction for docker builds and examples
# This file should be sourced, not executed directly

KAFKA_PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERSION=$(grep '^version=' "${KAFKA_PROJECT_DIR}/gradle.properties" | cut -d'=' -f2 | tr -d '[:space:]')

if [ -z "$VERSION" ]; then
    echo "Error: Could not determine version from gradle.properties" >&2
    exit 1
fi
