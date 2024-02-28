# Start form the Official Lightstramer Server image
FROM lightstreamer

# The Kafka Connector version
ARG VERSION

ENV KAFKA_CONNECTOR=lightstreamer-kafka-connector-${VERSION}
ENV DEPLOY_DIR=/lightstreamer/adapters/${KAFKA_CONNECTOR}

# Copy the distribution package 
COPY tmp/${KAFKA_CONNECTOR}.zip /tmp/${KAFKA_CONNECTOR}.zip
# Copy custom resources
COPY resources /tmp/resources

# Switch to the root user to allow OS updates.
USER root
RUN apt-get -y update; \
    apt-get install -y unzip; \
    # Unzip the distribution package into to Lightstreamer's adapters fodler.
    unzip /tmp/${KAFKA_CONNECTOR}.zip -d /lightstreamer/adapters; \
    # Copy the custom resources
    cp /tmp/resources/* ${DEPLOY_DIR}; \
    # Fix ownership
    chown lightstreamer:lightstreamer -R ${DEPLOY_DIR}

# Restore the original Lightstreamer user
USER lightstreamer
