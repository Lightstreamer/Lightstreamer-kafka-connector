# Lightstreamer Kafka Connector Docker Image

This folder contains the resources required to build a Docker image of the Lightstreamer Kafka Connector.

The image derives from the official [Lightstreamer Docker image](https://hub.docker.com/_/lightstreamer) and adds the Kafka Connector adapter.

## Quick Start

Images are published to GitHub Container Registry on each release.

```sh
# Pull the latest image
docker pull ghcr.io/lightstreamer/lightstreamer-kafka-connector:latest

# Or pull a specific version
docker pull ghcr.io/lightstreamer/lightstreamer-kafka-connector:1.4.0

# Run the container
docker run --name kafka-connector -d -p 8080:8080 \
  ghcr.io/lightstreamer/lightstreamer-kafka-connector:latest
```

### Configuration

Mount custom configuration files using Docker volumes:

```sh
docker run --name kafka-connector -d -p 8080:8080 \
  -v $(pwd)/adapters.xml:/lightstreamer/adapters/lightstreamer-kafka-connector-<version>/adapters.xml \
  -v $(pwd)/log4j.properties:/lightstreamer/adapters/lightstreamer-kafka-connector-<version>/log4j.properties \
  ghcr.io/lightstreamer/lightstreamer-kafka-connector:latest
```

**Common files to mount:**
- `adapters.xml` - Main connector configuration (Kafka bootstrap servers, topics, mappings)
- `log4j.properties` - Logging levels and output configuration
- `*.jks`, `*.p12` - SSL/TLS key stores and trust stores
- Schema files - Avro schemas, JSON Schema or PROTOBUF descriptor files

Verify deployment at [http://localhost:8080](http://localhost:8080)

**Note**: Replace `<version>` with the actual version (e.g., `1.4.0`) in mount paths.

## Building Locally

If you want to build the image from source:

### Requirements

- JDK version 17 or newer
- Docker

### Build Steps

```sh
./build.sh
```

This script will:
- Build the Kafka Connector distribution package using Gradle
- Build the Docker image
- Tag the image as `lightstreamer-kafka-connector-<version>`

### Run the Image

```sh
# Run the container
docker run --name kafka-connector -d -p 8080:8080 lightstreamer-kafka-connector-<version>
```

**Note**: Configuration works the same as in Quick Start (see above). Use Docker volumes to mount custom `adapters.xml`, `log4j.properties`, or SSL certificates.

Verify at [http://localhost:8080](http://localhost:8080)

---

## For Maintainers

### Publishing to Registry

#### Automated Publishing (GitHub Actions)

The project includes a GitHub Actions workflow (`.github/workflows/docker-publish.yml`) that automatically builds and publishes images when a release is created:

1. Create a new release in GitHub
2. The workflow automatically:
   - Builds the connector distribution
   - Builds the Docker image
   - Pushes to `ghcr.io/lightstreamer/lightstreamer-kafka-connector`

**First-time setup**: After the first automated push, set the package visibility to "public" in GitHub package settings.

#### Manual Publishing

**Prerequisites**: Login to the registry first:
```sh
echo $GITHUB_TOKEN | docker login ghcr.io -u <username> --password-stdin
```

Build and push using the provided scripts:

```sh
# Build the image
./build.sh

# Push to GitHub Container Registry
REGISTRY=ghcr.io/lightstreamer ./push.sh

# Or push to a custom registry
REGISTRY=docker.io/myorg ./push.sh
```

The `push.sh` script will:
- Verify the local image exists
- Tag for the specified registry
- Push both versioned and `latest` tags

### Technical Details

**Multi-stage Build**: The Dockerfile uses a multi-stage build pattern that extracts the distribution ZIP in one stage and copies only the extracted files to the final image, reducing the image size by ~30-50MB.

**OCI Labels**: The image includes standard OCI-compliant labels for version, source, and documentation metadata.

**Build Script**: The `build.sh` script is designed to work both locally and in CI/CD environments (exports version for GitHub Actions).

View image labels:
```sh
docker inspect lightstreamer-kafka-connector-<version> | jq '.[0].Config.Labels'
```
