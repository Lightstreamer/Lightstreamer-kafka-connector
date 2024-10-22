# Kafka Connect Lightstreamer Sink Connector Docker Image

This folder contains the resources required to build a minimal Docker image of the Kafka Connect Lightstreamer Sink Connector.

The image is based on the official [Official Confluent Docker Base Image for Kafka Connect](https://hub.docker.com/r/confluentinc/cp-kafka-connect-base). Check out the [`Dockerfile`](./Dockerfile) for more details.

## Building

### Requirements:

- Java 17
- Docker

### Instructions

1. Run the following:

   ```sh
   $ ./build.sh
   ```

   which will:
   
   - Create and copy the distribution package to the `tmp` folder.
   - Build the Docker image.

3. Check that the image has been created:

   ```sh
   $ docker image ls kafka-connect-lightstreamer-<version>

   REPOSITORY                              TAG       IMAGE ID       CREATED          SIZE
   kafka-connect-lightstreamer-<version>   latest    417d099deaa8   18 seconds ago   1.75GB
   ```

## Running in Docker Compose

Check out the [quickstart-kafka-connect](../quickstart-kafka-connect/) folder for an example of how to run this image in Docker Compose.
