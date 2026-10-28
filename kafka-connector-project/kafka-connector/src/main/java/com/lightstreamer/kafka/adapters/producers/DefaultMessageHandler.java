
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
import com.lightstreamer.interfaces.metadata.CreditsException;
import com.lightstreamer.kafka.adapters.producers.converters.InvalidMessageException;
import com.lightstreamer.kafka.adapters.producers.converters.JsonMessageCodec;
import com.lightstreamer.kafka.adapters.producers.converters.JsonMessageConverter;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;

class DefaultMessageHandler implements MessageHandler {

    static class ProducerRecordSender<K, V> {
        private final Logger logger;
        private final KafkaProducer<K, V> producer;

        private final boolean ackEnabled;
        private final JsonMessageConverter<K, V> converter;

        ProducerRecordSender(
                Logger logger,
                KafkaProducer<K, V> producer,
                boolean ackEnabled,
                JsonMessageConverter<K, V> converter) {
            this.logger = logger;
            this.producer = producer;
            this.ackEnabled = ackEnabled;
            this.converter = converter;
        }

        void send(String message, CompletableFuture<String> future)
                throws JacksonException, InvalidMessageException {
            ProducerRecord<K, V> record = converter.fromJson(message);
            try {
                producer.send(
                        record,
                        (metadata, exception) -> {
                            if (exception != null) {
                                logger.atError()
                                        .setCause(exception)
                                        .log("Failed to send message: {}", message);
                                future.completeExceptionally(exception);
                            } else {
                                if (ackEnabled) {
                                    future.complete(message);
                                }
                            }
                        });
            } catch (InterruptException e) {
                Thread.currentThread().interrupt();
                logger.atError().setCause(e).log("Interrupted while sending message: {}", message);
                future.completeExceptionally(e);
            } catch (KafkaException | IllegalStateException e) {
                logger.atError().setCause(e).log("Failed to send message: {}", message);
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
                logger, producer, settings.ackEnabled(), codec.converter());
    }

    @Override
    public CompletableFuture<String> sendMessage(String message) throws CreditsException {
        CompletableFuture<String> future = new CompletableFuture<>();
        try {
            logger.atDebug().log("Sending message: {}", message);
            messageSender.send(message, future);
        } catch (JacksonException e) {
            logger.atWarn().log("Rejected malformed JSON message: {}", e.getOriginalMessage());
            throw new CreditsException(
                    ErrorMessages.REJECTED_MESSAGE.getCode(),
                    "Malformed JSON message: " + e.getOriginalMessage());
        } catch (InvalidMessageException e) {
            logger.atWarn().log("Rejected invalid message: {}", e.getMessage());
            throw new CreditsException(ErrorMessages.REJECTED_MESSAGE.getCode(), e.getMessage());
        }
        return future;
    }

    @Override
    public void close() {
        messageSender.close();
    }
}
