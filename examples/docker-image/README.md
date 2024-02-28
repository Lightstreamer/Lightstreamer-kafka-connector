# Lightstreamer Kafka Connector Docker Image

This folder contains the resources required to build a minimal Docker image of the Lighstreamer Kafka Connector.

The image is built by deriving the official [Lighstreamer Docker image](https://hub.docker.com/_/lightstreamer) with the current version of the Kafka Connector. Check out the [`Dockerfile`](./Dockerfile) for more details.

## Requirements:

- Java 17.
- Docker.

## Instructions

To build the image:

1. Copy into the [`resources`](resources/) folder any customizable Kafka Connector resource, such as:
   - `adapters.xml`.
   - `log4j.properties` (or any other referenced log configuration file).
   - Local schema, keystore, and truststore files referenced in `adapters.xml`.

2. Run the command:

   `./build.sh`

   which will:
   
   - create and copy the distribution package to the `tmp` folder;
   - build the Docker image.

3. Check that the image has been created:

   ```sh
   docker image ls kafka-connector-<version>

   REPOSITORY                            TAG               IMAGE ID       CREATED          SIZE
   lightstreamer-kafka-connector-0.1.0   latest            f77fc60f7892   13 minutes ago   602MB
   ```

4. Launch the container with:

   `docker run --name kafka-connector -d -p 8080:8080 lightstreamer-kafka-connector-<version>`
 
5. Check the logs:
 
   `docker logs -f kafka-connector`
