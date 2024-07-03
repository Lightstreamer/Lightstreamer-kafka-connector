FROM confluentinc/cp-kafka-connect-base

# The Kafka Connector project version
ARG VERSION

ENV COMPONENT_NAME=lightstreamer-kafka-connect-lightstreamer-${VERSION}
ENV COMPONENT_ARCHVE=${COMPONENT_NAME}.zip

# Copy the distribution package 
COPY tmp/${COMPONENT_ARCHVE} /tmp/${COMPONENT_ARCHVE}

# Replace Java 11 with Java 17
USER root

RUN yum remove -y zulu11-ca-jdk-headless && yum remove -y zulu11-ca-jre-headless
RUN yum install -y zulu17-ca-jdk-headless && yum install -y zulu17-ca-jre-headless

# Install the connector
USER appuser
RUN confluent-hub install --no-prompt /tmp/${COMPONENT_ARCHVE}
