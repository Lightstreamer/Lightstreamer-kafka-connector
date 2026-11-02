
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

import com.lightstreamer.interfaces.metadata.CreditsException;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SendConfirmationMode;
import com.lightstreamer.kafka.adapters.producers.converters.InvalidMessageException;
import com.lightstreamer.kafka.adapters.producers.converters.JsonMessageCodec;
import com.lightstreamer.kafka.adapters.producers.converters.JsonMessageConverter;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;

class DefaultMessageHandler implements MessageHandler {

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

    static class ProducerRecordSender<K, V> {
        private final Logger logger;
        private final KafkaProducer<K, V> producer;
        private final ConfirmationModeHandler confirmationModeHandler;
        private final JsonMessageConverter<K, V> converter;

        ProducerRecordSender(
                Logger logger,
                KafkaProducer<K, V> producer,
                SendConfirmationMode sendConfirmationMode,
                JsonMessageConverter<K, V> converter) {
            this.logger = logger;
            this.producer = producer;
            this.confirmationModeHandler =
                    ConfirmationModeHandler.from(sendConfirmationMode, logger);
            this.converter = converter;
        }

        void send(String message, CompletableFuture<String> future) throws InvalidMessageException {
            ProducerRecord<K, V> record = converter.fromJson(message);
            try {
                producer.send(
                        record,
                        (metadata, exception) ->
                                confirmationModeHandler.onCompleted(future, metadata, exception));
                confirmationModeHandler.onEnqueued(future);
            } catch (InterruptException e) {
                Thread.currentThread().interrupt();
                logger.atError()
                        .setCause(e)
                        .log("Interrupted while sending message to topic {}", record.topic());
                future.completeExceptionally(e);
            } catch (KafkaException | IllegalStateException e) {
                logger.atError()
                        .setCause(e)
                        .log("Failed to send message to topic {}", record.topic());
                future.completeExceptionally(e);
            }
        }

        void close() {
            producer.close();
        }
    }

    private final Logger logger;

    private final ProducerRecordSender<?, ?> messageSender;

    DefaultMessageHandler(Logger logger, ProducerSettings settings) {
        this.logger = logger;
        this.messageSender = createMessageSender(logger, settings);
    }

    private static ProducerRecordSender<?, ?> createMessageSender(
            Logger logger, ProducerSettings settings) {
        JsonMessageCodec<?, ?> codec =
                JsonMessageCodec.of(settings.keyEvaluatorType(), settings.valueEvaluatorType());
        return newSender(logger, settings, codec);
    }

    // K and V are captured from the codec, so no unchecked casts are needed.
    private static <K, V> ProducerRecordSender<K, V> newSender(
            Logger logger, ProducerSettings settings, JsonMessageCodec<K, V> codec) {
        KafkaProducer<K, V> producer =
                new KafkaProducer<>(
                        settings.producerProperties(),
                        codec.keySerializer(),
                        codec.valueSerializer());

        return new ProducerRecordSender<>(
                logger, producer, settings.sendConfirmationMode(), codec.converter());
    }

    @Override
    public CompletableFuture<String> sendMessage(String message) throws CreditsException {
        CompletableFuture<String> future = new CompletableFuture<>();
        try {
            logger.atDebug().log("Sending message: {}", message);
            messageSender.send(message, future);
        } catch (InvalidMessageException e) {
            logger.atWarn().log("Rejected invalid message: {}", e.getMessage());
            throw new CreditsException(ErrorCode.MESSAGE_REJECTED.code(), e.getMessage());
        }
        return future;
    }

    @Override
    public void close() {
        messageSender.close();
    }
}
