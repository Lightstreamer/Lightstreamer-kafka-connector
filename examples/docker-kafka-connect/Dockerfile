# Start from the Official Lightstramer Server image
FROM apache/kafka

# The Kafka Connector project version
ARG VERSION

ENV COMPONENT_NAME=lightstreamer-kafka-connect-lightstreamer-${VERSION}
ENV COMPONENT_ARCHVE=${COMPONENT_NAME}.zip

# Copy the distribution package 
COPY tmp/${COMPONENT_ARCHVE} /tmp/${COMPONENT_ARCHVE}

USER root
RUN set -eux; \
    mkdir /plugins; \
    unzip /tmp/${COMPONENT_ARCHVE} -d /plugins

USER appuser
