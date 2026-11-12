
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

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lightstreamer.interfaces.metadata.CreditsException;
import com.lightstreamer.interfaces.metadata.NotificationException;
import com.lightstreamer.kafka.adapters.config.GlobalConfig;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.PayloadConverterType;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SendConfirmationMode;
import com.lightstreamer.kafka.adapters.producers.converters.InvalidMessageException;
import com.lightstreamer.kafka.adapters.producers.converters.JsonMessageCodec;
import com.lightstreamer.kafka.adapters.pub.messages.ClientMessage;
import com.lightstreamer.kafka.adapters.pub.messages.ClientMessageInfo;
import com.lightstreamer.kafka.adapters.pub.messages.MessageHook;
import com.lightstreamer.kafka.adapters.pub.messages.converters.PayloadConverter;
import com.lightstreamer.kafka.common.config.DestinationConfigurations;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** A message handler that sends messages to the Kafka producer. */
class MessagePublisher implements MessageHandler {

    interface ConfirmationModeHandler {

        default void onEnqueued(CompletableFuture<String> future) {}

        void onCompleted(
                CompletableFuture<String> future, RecordMetadata metadata, Exception exception);

        static ConfirmationModeHandler from(SendConfirmationMode confirmationMode, Logger logger) {
            return switch (confirmationMode) {
                case IMMEDIATE -> new ImmediateHandler(logger);
                case CONFIRMED -> new ConfirmedHandler(logger);
                case DETAILED -> new DetailedHandler(logger);
            };
        }
    }

    static final class ImmediateHandler implements ConfirmationModeHandler {

        protected final Logger logger;

        ImmediateHandler(Logger logger) {
            this.logger = logger;
        }

        @Override
        public void onEnqueued(CompletableFuture<String> future) {
            future.complete(null);
        }

        @Override
        public void onCompleted(
                CompletableFuture<String> future, RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                logger.atError()
                        .setCause(exception)
                        .log(
                                "Failed to send message to topic {}; client was not notified (mode=IMMEDIATE)",
                                metadata.topic());
            }
        }
    }

    static sealed class ConfirmedHandler implements ConfirmationModeHandler {

        protected final Logger logger;

        ConfirmedHandler(Logger logger) {
            this.logger = logger;
        }

        @Override
        public final void onCompleted(
                CompletableFuture<String> future, RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                logger.atWarn()
                        .setCause(exception)
                        .log("Failed to send message to topic {}", metadata.topic());
                future.completeExceptionally(exception);
            } else {
                onCompletedWithSuccess(future, metadata);
            }
        }

        void onCompletedWithSuccess(CompletableFuture<String> future, RecordMetadata metadata) {
            future.complete(null);
        }
    }

    static final class DetailedHandler extends ConfirmedHandler {

        DetailedHandler(Logger logger) {
            super(logger);
        }

        @Override
        void onCompletedWithSuccess(CompletableFuture<String> future, RecordMetadata metadata) {
            future.complete(receipt(metadata));
        }

        private static String receipt(RecordMetadata m) {
            return "{\"partition\":%d,\"offset\":%d,\"timestamp\":%d}"
                    .formatted(m.partition(), m.offset(), m.timestamp());
        }
    }

    private final Logger logger;
    private final MessageHook messageHook;
    private final Producer<JsonNode, JsonNode> producer;
    private final DestinationConfigurations destinationConfigs;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ConfirmationModeHandler confirmationModeHandler;

    MessagePublisher(Logger logger, GlobalConfig config, MessageHook messageHook) {
        this(logger, ProducerFactory.defaultFactory(), config, messageHook);
    }

    MessagePublisher(
            Logger logger,
            ProducerFactory producerFactory,
            GlobalConfig config,
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
        this.producer =
                producerFactory.createProducer(
                        config.baseProducerProps(), keySerializer, valueSerializer);
        this.confirmationModeHandler =
                ConfirmationModeHandler.from(config.getProducerSendConfirmationMode(), logger);
        this.destinationConfigs = config.getDestinationConfigurations();
    }

    private static PayloadConverter createMessageConverter(
            PayloadConverterType type, Supplier<PayloadConverter> providedConverterSupplier) {
        PayloadConverterType messageValuePayloadConverter = type;
        return switch (messageValuePayloadConverter) {
            case PROVIDED -> providedConverterSupplier.get();
            default -> JsonMessageCodec.lookup(messageValuePayloadConverter);
        };
    }

    ClientMessage parseClientMessage(String clientMessage) throws InvalidMessageException {
        ClientMessage message;
        try {
            message = mapper.readValue(clientMessage, ClientMessage.class);
        } catch (IllegalArgumentException e) {
            throw InvalidMessageException.malformedJson(e.getMessage());
        } catch (JacksonException e) {
            throw InvalidMessageException.malformedJson(e.getOriginalMessage());
        }

        return message;
    }

    @Override
    public CompletableFuture<String> sendMessage(String user, String sessionID, String message) {
        CompletableFuture<String> future = new CompletableFuture<>();
        try {
            logger.atDebug().log("Parsing client message: {}", message);
            ClientMessage clientMessage = parseClientMessage(message);
            clientMessage =
                    messageHook.onMessagePublishing(
                            new ClientMessageInfo(user, sessionID, clientMessage));

            logger.atDebug().log(
                    "Checking topics for destination: {}", clientMessage.destination());
            String destination = clientMessage.destination();
            Set<String> topic = destinationConfigs.lookupTopics(destination);
            if (topic.isEmpty()) {
                throw InvalidMessageException.invalidDestination(destination);
            }
            logger.atDebug().log("Resolved topics for destination {}: {}", destination, topic);
            if (clientMessage.value() == null) {
                throw InvalidMessageException.missingValue();
            }

            for (String t : topic) {
                logger.atDebug().log("Sending message to topic [{}]: [{}]...", t, message);
                ProducerRecord<JsonNode, JsonNode> record =
                        new ProducerRecord<>(t, clientMessage.key(), clientMessage.value());
                try {
                    producer.send(
                            record,
                            (metadata, exception) ->
                                    confirmationModeHandler.onCompleted(
                                            future, metadata, exception));
                    confirmationModeHandler.onEnqueued(future);
                    logger.atDebug().log("Sent message to topic [{}]: [{}]", t, message);
                } catch (InterruptException e) {
                    Thread.currentThread().interrupt();
                    logger.atError()
                            .setCause(e)
                            .log("Interrupted while sending message to topic [{}]", record.topic());
                    future.completeExceptionally(e);
                    break;
                } catch (KafkaException | IllegalStateException e) {
                    logger.atError()
                            .log(
                                    "Failed to send message to topic [{}]: {}",
                                    record.topic(),
                                    e.getMessage());
                    future.completeExceptionally(e);
                    break;
                }
            }
        } catch (InvalidMessageException e) {
            logger.atWarn().log("Rejected invalid message: {}", e.getMessage());
            future.completeExceptionally(
                    new CreditsException(ErrorCode.MESSAGE_REJECTED.code(), e.getMessage()));
        } catch (CreditsException | NotificationException e) {
            logger.atError().setCause(e).log("Failed to process message hook: {}", e.getMessage());
            future.completeExceptionally(e);
        }
        return future;
    }

    // void send(ClientMessage message, CompletableFuture<String> future) throws
    // InvalidMessageException {
    //         try {
    //             producer.send(
    //                     record,
    //                     (metadata, exception) ->
    //                             confirmationModeHandler.onCompleted(future, metadata,
    // exception));
    //             confirmationModeHandler.onEnqueued(future);
    //         } catch (InterruptException e) {
    //             Thread.currentThread().interrupt();
    //             logger.atError()
    //                     .setCause(e)
    //                     .log("Interrupted while sending message to topic {}", record.topic());
    //             future.completeExceptionally(e);
    //         } catch (KafkaException | IllegalStateException e) {
    //             logger.atError()
    //                     .setCause(e)
    //                     .log("Failed to send message to topic {}", record.topic());
    //             future.completeExceptionally(e);
    //         }
    //     }

    @Override
    public void close() {
        producer.close();
    }
}
