# Kafka Connect Lightstreamer Sink Connector Docker Image

This folder contains the resources required to build a Docker image of the Kafka Connect Lightstreamer Sink Connector.

The image is based on the [Confluent Docker Base Image for Kafka Connect](https://hub.docker.com/r/confluentinc/cp-kafka-connect-base). Check out the [`Dockerfile`](./Dockerfile) for more details.

## Building Locally

### Requirements

- JDK version 17 or newer
- Docker

### Build Steps

```sh
./build.sh
```

This script will:
- Create the distribution package using Gradle
- Build the Docker image
- Tag the image as `kafka-connect-lightstreamer-<version>`

### Verify the Image

```sh
docker images kafka-connect-lightstreamer-*
```

Expected output:
```
REPOSITORY                              TAG       IMAGE ID       CREATED          SIZE
kafka-connect-lightstreamer-<version>   latest    417d099deaa8   18 seconds ago   1.75GB
```

**Note**: Replace `<version>` with the actual version (e.g., `1.4.1`).

## Running in Docker Compose

Check out the [quickstart-kafka-connect](../../quickstart-kafka-connect/) folder for an example of how to run this image in Docker Compose.
