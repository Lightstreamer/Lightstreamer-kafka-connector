
/*
 * Copyright (C) 2026 Lightstreamer Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package com.lightstreamer.kafka.adapters.producers;

import static com.lightstreamer.kafka.adapters.producers.MessageHandler.ClientError.BROKER_DELIVERY_FAILED;
import static com.lightstreamer.kafka.adapters.producers.MessageHandler.ClientError.INVALID_PAYLOAD_FORMAT;
import static com.lightstreamer.kafka.adapters.producers.MessageHandler.ClientError.MALFORMED_MESSAGE;
import static com.lightstreamer.kafka.adapters.producers.MessageHandler.ClientError.MESSAGE_HOOK_FAILED;
import static com.lightstreamer.kafka.adapters.producers.MessageHandler.ClientError.MISSING_DESTINATION;
import static com.lightstreamer.kafka.adapters.producers.MessageHandler.ClientError.MISSING_PAYLOAD_VALUE;
import static com.lightstreamer.kafka.adapters.producers.MessageHandler.ClientError.SEND_FAILED;
import static com.lightstreamer.kafka.adapters.producers.MessageHandler.ClientError.UNMAPPED_DESTINATION;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.lightstreamer.kafka.adapters.config.PublisherConfig;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.PayloadConverterType;
import com.lightstreamer.kafka.adapters.producers.converters.JsonMessageCodec;
import com.lightstreamer.kafka.adapters.pub.messages.ClientMessage;
import com.lightstreamer.kafka.adapters.pub.messages.ClientMessageInfo;
import com.lightstreamer.kafka.adapters.pub.messages.MessageHook;
import com.lightstreamer.kafka.adapters.pub.messages.PublishingException;
import com.lightstreamer.kafka.adapters.pub.messages.converters.PayloadConverter;
import com.lightstreamer.kafka.common.config.DestinationConfigurations;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** A message handler that sends messages to the Kafka producer. */
class MessagePublisher implements MessageHandler {

    interface ConfirmationModeHandler {

        CompletableFuture<String> combine(List<CompletableFuture<String>> futures);

        void onCompleted(
                CompletableFuture<String> future, RecordMetadata metadata, Exception exception);

        static ConfirmationModeHandler from(boolean enabled, Logger logger) {
            return enabled ? new ConfirmedHandler(logger) : new ImmediateHandler(logger);
        }

        default String messageInfo(RecordMetadata metadata) {
            return String.format(
                    "topic=%s, partition=%d, offset=%d",
                    metadata.topic(), metadata.partition(), metadata.offset());
        }
    }

    static final class ImmediateHandler implements ConfirmationModeHandler {

        protected final Logger logger;

        ImmediateHandler(Logger logger) {
            this.logger = logger;
        }

        @Override
        public void onCompleted(
                CompletableFuture<String> future, RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                logger.atError()
                        .setCause(exception)
                        .log(
                                "Failed to deliver message [{}]; client was not notified (mode=IMMEDIATE)",
                                messageInfo(metadata));
            }
        }

        @Override
        public CompletableFuture<String> combine(List<CompletableFuture<String>> futures) {
            for (CompletableFuture<String> f : futures) {
                if (f.isCompletedExceptionally()) {
                    return f; // sync failure, already holds the CreditsException
                }
            }
            return CompletableFuture.completedFuture(null);
        }
    }

    static class ConfirmedHandler implements ConfirmationModeHandler {

        protected final Logger logger;

        ConfirmedHandler(Logger logger) {
            this.logger = logger;
        }

        @Override
        public final void onCompleted(
                CompletableFuture<String> future, RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                logger.atError()
                        .setCause(exception)
                        .log("Failed to deliver message [{}]", messageInfo(metadata));
                future.completeExceptionally(BROKER_DELIVERY_FAILED.toCreditsException());
            } else {
                future.complete(null);
            }
        }

        @Override
        public CompletableFuture<String> combine(List<CompletableFuture<String>> futures) {
            CompletableFuture<String> response = new CompletableFuture<>();
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .whenComplete(
                            (v, t) -> {
                                if (t != null) {
                                    response.completeExceptionally(t.getCause());
                                } else {
                                    response.complete(null);
                                }
                            });
            return response;
        }
    }

    private final Logger logger;
    private final MessageHook messageHook;
    private final Producer<JsonNode, JsonNode> producer;
    private final DestinationConfigurations destinationConfigs;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ConfirmationModeHandler confirmationModeHandler;

    MessagePublisher(Logger logger, PublisherConfig config, MessageHook messageHook) {
        this(logger, ProducerFactory.defaultFactory(), config, messageHook);
    }

    MessagePublisher(
            Logger logger,
            ProducerFactory producerFactory,
            PublisherConfig config,
            MessageHook messageHook) {
        this.logger = logger;
        this.messageHook = messageHook;

        Serializer<JsonNode> keySerializer =
                createMessageConverter(
                        config.getMessageKeyPayloadConverter(), messageHook::createKeyConverter);
        Serializer<JsonNode> valueSerializer =
                createMessageConverter(
                        config.getMessageValuePayloadConverter(),
                        messageHook::createValueConverter);
        Map<String, Object> producerConfig = config.producerConfigs();
        keySerializer.configure(producerConfig, true);
        valueSerializer.configure(producerConfig, false);
        this.producer =
                producerFactory.createProducer(producerConfig, keySerializer, valueSerializer);
        this.confirmationModeHandler =
                ConfirmationModeHandler.from(config.isSendConfirmationEnable(), logger);
        this.destinationConfigs = config.getDestinationConfigurations();
    }

    static PayloadConverter createMessageConverter(
            PayloadConverterType type, Supplier<PayloadConverter> providedConverterSupplier) {
        return switch (type) {
            case PROVIDED -> Objects.requireNonNull(providedConverterSupplier.get());
            default -> JsonMessageCodec.lookup(type);
        };
    }

    @Override
    public CompletableFuture<String> sendMessage(String user, String sessionID, String message) {
        // #1 Parse the incoming client message into a structured ClientMessage object
        ClientMessage clientMessage;
        logger.atDebug().log("Parsing client message: {}", message);
        try {
            clientMessage = mapper.readValue(message, ClientMessage.class);
        } catch (JsonProcessingException e) {
            logger.atWarn().log("Failed to parse client message: {}", e.getOriginalMessage());
            return CompletableFuture.failedFuture(MALFORMED_MESSAGE.toCreditsException());
        }

        // #2 Apply the message hook to allow custom processing before publishing
        try {
            logger.atDebug().log("Applying message hook for client message: {}", clientMessage);
            clientMessage =
                    messageHook.onMessagePublishing(
                            new ClientMessageInfo(user, sessionID, clientMessage));
        } catch (PublishingException e) {
            logger.atWarn().setCause(e).log("Failed to process message hook");
            return CompletableFuture.failedFuture(
                    MESSAGE_HOOK_FAILED.toCreditsException(e.getMessage()));
        }

        // #3 Check if the destination is valid
        logger.atDebug().log("Checking topics for destination: {}", clientMessage.destination());
        String destination = clientMessage.destination();
        if (destination == null || destination.isBlank()) {
            logger.atWarn().log("No valid destination provided");
            return CompletableFuture.failedFuture(MISSING_DESTINATION.toCreditsException());
        }

        // #4 Retrieve and validate the topics for the destination
        Set<String> topics = destinationConfigs.lookupTopics(destination);
        if (topics.isEmpty()) {
            logger.atWarn().log("No topics found for destination: {}", destination);
            return CompletableFuture.failedFuture(UNMAPPED_DESTINATION.toCreditsException());
        }
        logger.atDebug().log("Resolved topics for destination {}: {}", destination, topics);
        if (clientMessage.value() == null) {
            logger.atWarn().log("Missing message value for destination: {}", destination);
            return CompletableFuture.failedFuture(MISSING_PAYLOAD_VALUE.toCreditsException());
        }

        // #4 Send the message to each resolved topic
        List<CompletableFuture<String>> futures = new ArrayList<>();
        for (String t : topics) {
            futures.add(sendToTopic(t, clientMessage, message));
        }
        return confirmationModeHandler.combine(futures);
    }

    CompletableFuture<String> sendToTopic(String t, ClientMessage clientMessage, String message) {
        CompletableFuture<String> topicFuture = new CompletableFuture<>();
        logger.atDebug().log("Sending message to topic [{}]: [{}]...", t, message);
        ProducerRecord<JsonNode, JsonNode> record =
                new ProducerRecord<>(t, clientMessage.key(), clientMessage.value());
        try {
            producer.send(
                    record,
                    (metadata, exception) ->
                            confirmationModeHandler.onCompleted(topicFuture, metadata, exception));
            logger.atDebug().log("Sent message to topic [{}]: [{}]", t, message);
        } catch (SerializationException e) {
            logger.atWarn()
                    .log(
                            "Serialization error while sending message to topic [{}]: {}",
                            record.topic(),
                            e.getMessage());
            return CompletableFuture.failedFuture(
                    INVALID_PAYLOAD_FORMAT.toCreditsException(e.getMessage()));
        } catch (InterruptException e) {
            // No need to call Thread.currentThread().interrupt(): InterruptException
            // already restores the interrupt flag when it is constructed.
            logger.atWarn()
                    .setCause(e)
                    .log("Interrupted while sending message to topic [{}]", record.topic());
            return CompletableFuture.failedFuture(SEND_FAILED.toCreditsException());
        } catch (RuntimeException e) {
            logger.atError()
                    .setCause(e)
                    .log("Failed to send message to topic [{}]", record.topic());
            return CompletableFuture.failedFuture(SEND_FAILED.toCreditsException());
        }
        return topicFuture;
    }

    @Override
    public void close() {
        producer.close();
    }

    @VisibleForTesting
    ConfirmationModeHandler confirmationModeHandler() {
        return confirmationModeHandler;
    }
}
