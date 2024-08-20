FROM confluentinc/cp-kafka-connect-base

# The Kafka Connector project version
ARG VERSION

ENV COMPONENT_NAME=lightstreamer-kafka-connect-lightstreamer-${VERSION}

# Install the connector
COPY tmp/${COMPONENT_NAME} /usr/local/share/kafka/plugins/${COMPONENT_NAME}
