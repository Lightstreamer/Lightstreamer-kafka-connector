# Lightstreamer Kafka Connector Docker Image

This folder contains the resources required to build a minimal Docker image of Lightstreamer Kafka Connector.

The image is built by deriving the official [Lightstreamer Docker image](https://hub.docker.com/_/lightstreamer) with the current version of the Kafka Connector. Check out the [`Dockerfile`](./Dockerfile) for more details.

## Requirements:

- JDK version 17
- Docker

## Instructions

1. Copy into the [`resources`](resources/) folder any customizable Kafka Connector resource, such as:
   - `adapters.xml`
   - `log4j.properties` (or any other referenced log configuration file)
   - Local schema, key store, and trust store files referenced in `adapters.xml`

2. Run the following:

   ```sh
   $ ./build.sh
   ```

   which will:
   
   - Create and copy the distribution package to the `tmp` folder.
   - Build the Docker image.

3. Check that the image has been created:

   ```sh
   $ docker image ls lightstreamer-kafka-connector-<version>

   REPOSITORY                              TAG               IMAGE ID       CREATED          SIZE
   lightstreamer-kafka-connector-<version> latest            f77fc60f7892   13 minutes ago   602MB
   ```

4. Launch the container with:

   ```sh
   $ docker run --name kafka-connector -d -p 8080:8080 lightstreamer-kafka-connector-<version>
   ```
 
5. Check the logs:
 
   ```sh
   $ docker logs -f kafka-connector
   ```
