#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Build the fat jar with Gradle
"${SCRIPT_DIR}/gradlew" -p "${SCRIPT_DIR}" build
