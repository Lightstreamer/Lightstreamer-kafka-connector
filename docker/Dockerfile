# ============================================
# Stage 1: Extract the distribution package
# ============================================
FROM alpine:latest AS extractor

ARG VERSION
ENV KAFKA_CONNECTOR_ZIP=lightstreamer-kafka-connector-${VERSION}.zip

COPY tmp/${KAFKA_CONNECTOR_ZIP} /tmp/
RUN apk add --no-cache unzip && \
    unzip /tmp/${KAFKA_CONNECTOR_ZIP} -d /extracted && \
    mv /extracted/lightstreamer-kafka-connector-* /extracted/lightstreamer-kafka-connector

# ============================================
# Stage 2: Build the final image
# ============================================
FROM lightstreamer:7.4.6

ARG VERSION
ARG BUILD_DATE

ENV KAFKA_CONNECTOR_NAME=lightstreamer-kafka-connector
ENV DEPLOY_DIR=/lightstreamer/adapters/${KAFKA_CONNECTOR_NAME}

# OCI labels for metadata
LABEL org.opencontainers.image.title="Lightstreamer Kafka Connector" \
      org.opencontainers.image.description="Lightstreamer Server with Kafka Connector for real-time data streaming" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.vendor="Lightstreamer" \
      org.opencontainers.image.url="https://github.com/Lightstreamer/Lightstreamer-kafka-connector" \
      org.opencontainers.image.source="https://github.com/Lightstreamer/Lightstreamer-kafka-connector" \
      org.opencontainers.image.documentation="https://github.com/Lightstreamer/Lightstreamer-kafka-connector" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.created="${BUILD_DATE}"

# Copy only the extracted connector files from stage 1 (ZIP is not included in final image)
COPY --from=extractor /extracted/${KAFKA_CONNECTOR_NAME} ${DEPLOY_DIR}

# Copy container-optimized log4j.properties (outputs to stdout for docker logs compatibility)
COPY log4j.properties ${DEPLOY_DIR}/log4j.properties

# Configure Lightstreamer and install connector
USER root
RUN set -eux; \
    # Set the env prefix for referencing environment variables in adapters.xml
    sed -i -e 's/env_prefix=""/env_prefix="env."/' /lightstreamer/conf/lightstreamer_conf.xml; \
    # Fix ownership
    chown -R lightstreamer:lightstreamer ${DEPLOY_DIR}

# Run as non-root user
USER lightstreamer

# Expose Lightstreamer default port
EXPOSE 8080
