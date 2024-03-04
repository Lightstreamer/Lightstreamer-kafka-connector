# Start form the Official Lightstramer Server image
FROM lightstreamer

# The Kafka Connector project version
ARG VERSION

ENV KAFKA_CONNECTOR_NAME=lightstreamer-kafka-connector-${VERSION}
ENV KAFKA_CONNECTOR_ZIP=${KAFKA_CONNECTOR_NAME}.zip
ENV DEPLOY_DIR=/lightstreamer/adapters/${KAFKA_CONNECTOR_NAME}

# Copy the distribution package 
COPY tmp/${KAFKA_CONNECTOR_ZIP} /tmp/${KAFKA_CONNECTOR_ZIP}
# Copy custom resources
COPY resources /tmp/resources

# Switch to the root user to allow OS updates.
USER root
RUN apt-get -y update; \
    apt-get install -y unzip; \
    # Unzip the distribution package into to Lightstreamer's adapters fodler.
    unzip /tmp/${KAFKA_CONNECTOR_ZIP} -d /lightstreamer/adapters; \
    # Set the env prefix for referencing environment variables in adapters.xml
    sed -i -e 's/env_prefix=""/env_prefix="env."/' /lightstreamer/conf/lightstreamer_conf.xml; \
    # Copy the custom resources
    cp /tmp/resources/* ${DEPLOY_DIR}; \
    # Fix ownership
    chown lightstreamer:lightstreamer -R ${DEPLOY_DIR}

# Restore the original Lightstreamer user
USER lightstreamer
