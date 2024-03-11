# Start from the Official Lightstramer Server image
FROM lightstreamer

# The Kafka Connector project version
ARG VERSION

ENV KAFKA_CONNECTOR_NAME=lightstreamer-kafka-connector-${VERSION}
ENV KAFKA_CONNECTOR_ZIP=${KAFKA_CONNECTOR_NAME}.zip
ENV DEPLOY_DIR=/lightstreamer/adapters/${KAFKA_CONNECTOR_NAME}

# Copy the distribution package 
COPY tmp/${KAFKA_CONNECTOR_ZIP} /tmp/${KAFKA_CONNECTOR_ZIP}

# Copy the custom resources
COPY resources /tmp/resources

# Switch to the root user to allow sytem updates.
USER root
RUN apt-get -y update; \
    apt-get install -y unzip rsync; \
    # Unzip the distribution package into the Lightstreamer's adapters fodler
    unzip /tmp/${KAFKA_CONNECTOR_ZIP} -d /lightstreamer/adapters; \
    # Set the env prefix for referencing environment variables in adapters.xml
    sed -i -e 's/env_prefix=""/env_prefix="env."/' /lightstreamer/conf/lightstreamer_conf.xml; \
    # Copy the custom resources into the Lightstreamer's adapters fodler
    rsync -av --exclude=README.md /tmp/resources/* ${DEPLOY_DIR}; \
    # Fix ownership
    chown lightstreamer:lightstreamer -R ${DEPLOY_DIR}; \
    # Clean up
    rm -fr /tmp/${KAFKA_CONNECTOR_ZIP} /tmp/resources; \
    apt-get remove -y unzip rsync; \
    rm -rf /var/lib/apt/lists/*

# Restore the original Lightstreamer user
USER lightstreamer
