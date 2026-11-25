
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

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.adapters.config.PublisherConfig.MESSAGE_KEY_PAYLOAD_CONVERTER;
import static com.lightstreamer.kafka.adapters.config.PublisherConfig.MESSAGE_VALUE_PAYLOAD_CONVERTER;
import static com.lightstreamer.kafka.adapters.config.PublisherConfig.SEND_CONFIRMATION_ENABLE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.interfaces.metadata.CreditsException;
import com.lightstreamer.kafka.adapters.config.PublisherConfig;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.PayloadConverterType;
import com.lightstreamer.kafka.adapters.producers.MessageHandler.ClientError;
import com.lightstreamer.kafka.adapters.producers.MessagePublisher.ConfirmedHandler;
import com.lightstreamer.kafka.adapters.pub.messages.PublishingException;
import com.lightstreamer.kafka.adapters.pub.messages.converters.PayloadConverter;
import com.lightstreamer.kafka.test_utils.PublisherMocks;
import com.lightstreamer.kafka.test_utils.PublisherMocks.MockProducerFactory;
import com.lightstreamer.kafka.test_utils.PublisherMocks.ProducerListener;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class MessagePublisherTest {

    static class FakePayloadConverter implements PayloadConverter {

        final String name;

        FakePayloadConverter() {
            this("default");
        }

        FakePayloadConverter(String name) {
            this.name = name;
        }

        @Override
        public byte[] serialize(String topic, JsonNode data) {
            throw new UnsupportedOperationException("Unimplemented method 'serialize'");
        }
    }

    @Test
    void shouldCreateMessageConverter() {
        PayloadConverter fakeConverter =
                MessagePublisher.createMessageConverter(
                        PayloadConverterType.PROVIDED, FakePayloadConverter::new);
        assertThat(fakeConverter).isInstanceOf(FakePayloadConverter.class);

        PayloadConverter converter =
                MessagePublisher.createMessageConverter(
                        PayloadConverterType.JSON, FakePayloadConverter::new);
        assertThat(converter).isNotNull();
        assertThat(converter).isNotInstanceOf(FakePayloadConverter.class);
    }

    @Test
    void shouldCreateMessagePublisher() {
        AtomicReference<Map<String, Object>> baseProps = new AtomicReference<>();

        AtomicReference<Serializer<JsonNode>> keySerializerRef = new AtomicReference<>();
        AtomicReference<Serializer<JsonNode>> valueSerializerRef = new AtomicReference<>();
        ProducerFactory factory =
                new ProducerFactory() {

                    @Override
                    public Producer<JsonNode, JsonNode> createProducer(
                            Map<String, Object> configs,
                            Serializer<JsonNode> keySerializer,
                            Serializer<JsonNode> valueSerializer) {
                        baseProps.set(configs);
                        keySerializerRef.set(keySerializer);
                        valueSerializerRef.set(valueSerializer);
                        return new MockProducerFactory()
                                .createProducer(configs, keySerializer, valueSerializer);
                    }
                };
        Map<String, String> config =
                Map.of(
                        PublisherConfig.BOOTSTRAP_SERVERS,
                        "localhost:9092",
                        "publisher.map.destination.to",
                        "topic",
                        MESSAGE_KEY_PAYLOAD_CONVERTER,
                        "PROVIDED",
                        MESSAGE_VALUE_PAYLOAD_CONVERTER,
                        "PROVIDED",
                        SEND_CONFIRMATION_ENABLE,
                        "true");
        MessagePublisher publisher =
                new MessagePublisher(
                        LoggerFactory.getLogger("Publisher"),
                        factory,
                        new PublisherConfig(config),
                        new PublisherMocks.MockMessageHook(
                                () -> new FakePayloadConverter("key"),
                                () -> new FakePayloadConverter("value")));
        assertThat(publisher.confirmationModeHandler()).isInstanceOf(ConfirmedHandler.class);
        assertThat(baseProps.get()).isNotNull();

        FakePayloadConverter keyPayloadConverter = (FakePayloadConverter) keySerializerRef.get();
        assertThat(keyPayloadConverter.name).isEqualTo("key");

        FakePayloadConverter valuePayloadConverter =
                (FakePayloadConverter) valueSerializerRef.get();
        assertThat(valuePayloadConverter.name).isEqualTo("value");
    }

    @Test
    void shouldFailSendMessageDueToMalformedMessage() {
        for (boolean sendConfirmationEnabled : List.of(true, false)) {
            MessagePublisher publisher =
                    new MessagePublisher(
                            LoggerFactory.getLogger("Publisher"),
                            new PublisherMocks.MockProducerFactory(),
                            PublisherConfig.defaultConfig(
                                    PublisherConfig.SEND_CONFIRMATION_ENABLE,
                                    String.valueOf(sendConfirmationEnabled)),
                            new PublisherMocks.MockMessageHook());

            CompletableFuture<String> future = publisher.sendMessage("user", "sessionId", "");
            assertCreditsException(future, ClientError.MALFORMED_MESSAGE);
        }
    }

    @Test
    void shouldFailSendMessageDueToPublishingException() {
        PublisherMocks.MockMessageHook messageHook = new PublisherMocks.MockMessageHook();
        messageHook.setPublishingException(new PublishingException("Fake publishing exception"));
        for (boolean sendConfirmationEnabled : List.of(true, false)) {
            MessagePublisher publisher =
                    new MessagePublisher(
                            LoggerFactory.getLogger("Publisher"),
                            new PublisherMocks.MockProducerFactory(),
                            PublisherConfig.defaultConfig(
                                    PublisherConfig.SEND_CONFIRMATION_ENABLE,
                                    String.valueOf(sendConfirmationEnabled)),
                            messageHook);

            String message =
                    """
                {
                        "destination": "destination",
                        "value": "someValue"
                }
                        """;
            CompletableFuture<String> future = publisher.sendMessage("user", "sessionId", message);
            CreditsException ce = assertCreditsException(future, ClientError.MESSAGE_HOOK_FAILED);
            assertThat(ce.getClientErrorMsg()).isEqualTo("Fake publishing exception");
        }
    }

    static Stream<String> invalidDestination() {
        return Stream.of(
                "{ \"value\": \"someValue\" }",
                "{ \"destination\": null, \"value\": \"someValue\" }",
                "{ \"destination\": \"   \", \"value\": \"someValue\" }",
                "{ \"destination\": \"\", \"value\": \"someValue\" }");
    }

    @ParameterizedTest
    @MethodSource("invalidDestination")
    void shouldFailSendMessageDueToMissingDestination(String message) {
        for (boolean sendConfirmationEnabled : List.of(true, false)) {
            MessagePublisher publisher =
                    new MessagePublisher(
                            LoggerFactory.getLogger("Publisher"),
                            new PublisherMocks.MockProducerFactory(),
                            PublisherConfig.defaultConfig(
                                    PublisherConfig.SEND_CONFIRMATION_ENABLE,
                                    String.valueOf(sendConfirmationEnabled)),
                            new PublisherMocks.MockMessageHook());

            CompletableFuture<String> future = publisher.sendMessage("user", "sessionId", message);
            assertCreditsException(future, ClientError.MISSING_DESTINATION);
        }
    }

    @Test
    void shouldFailSendMessageDueDestinationNotMapped() {
        for (boolean sendConfirmationEnabled : List.of(true, false)) {
            MessagePublisher publisher =
                    new MessagePublisher(
                            LoggerFactory.getLogger("Publisher"),
                            new PublisherMocks.MockProducerFactory(),
                            PublisherConfig.defaultConfig(
                                    PublisherConfig.SEND_CONFIRMATION_ENABLE,
                                    String.valueOf(sendConfirmationEnabled)),
                            new PublisherMocks.MockMessageHook());

            String message = "{ \"destination\": \"invalidMapping\", \"value\": \"someValue\" }";
            CompletableFuture<String> future = publisher.sendMessage("user", "sessionId", message);
            assertCreditsException(future, ClientError.UNMAPPED_DESTINATION);
        }
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "{ \"destination\": \"destination\" }",
                "{ \"destination\": \"destination\", \"value\": null }",
            })
    void shouldFailSendMessageDueToMissingPayloadValue(String message) {
        for (boolean sendConfirmationEnabled : List.of(true, false)) {
            MessagePublisher publisher =
                    new MessagePublisher(
                            LoggerFactory.getLogger("Publisher"),
                            new PublisherMocks.MockProducerFactory(),
                            PublisherConfig.defaultConfig(
                                    PublisherConfig.SEND_CONFIRMATION_ENABLE,
                                    String.valueOf(sendConfirmationEnabled)),
                            new PublisherMocks.MockMessageHook());

            CompletableFuture<String> future = publisher.sendMessage("user", "sessionId", message);
            assertCreditsException(future, ClientError.MISSING_PAYLOAD_VALUE);
        }
    }

    @Test()
    void shouldFailSendMessageDueToInvalidValuePayloadConverterType() {
        for (boolean sendConfirmationEnabled : List.of(true, false)) {
            PublisherMocks.MockMessageHook messageHook = new PublisherMocks.MockMessageHook();
            MessagePublisher publisher =
                    new MessagePublisher(
                            LoggerFactory.getLogger("Publisher"),
                            new PublisherMocks.MockProducerFactory(),
                            PublisherConfig.defaultConfig(
                                    PublisherConfig.SEND_CONFIRMATION_ENABLE,
                                    String.valueOf(sendConfirmationEnabled),
                                    PublisherConfig.MESSAGE_VALUE_PAYLOAD_CONVERTER,
                                    "JSON"),
                            messageHook);

            String message =
                    "{ \"destination\": \"destination\", \"value\": \"someValue\", \"key\": \"someKey\" }";
            CompletableFuture<String> future = publisher.sendMessage("user", "sessionId", message);
            CreditsException creditsException =
                    assertCreditsException(future, ClientError.INVALID_PAYLOAD_FORMAT);
            assertThat(creditsException.getClientErrorMsg()).startsWith("Invalid value");
        }
    }

    @Test
    void shouldFailSendMessageDueToInvalidKeyPayloadConverterType() {
        for (boolean sendConfirmationEnabled : List.of(true, false)) {
            MessagePublisher publisher =
                    new MessagePublisher(
                            LoggerFactory.getLogger("Publisher"),
                            new PublisherMocks.MockProducerFactory(),
                            PublisherConfig.defaultConfig(
                                    PublisherConfig.SEND_CONFIRMATION_ENABLE,
                                    String.valueOf(sendConfirmationEnabled),
                                    PublisherConfig.MESSAGE_KEY_PAYLOAD_CONVERTER,
                                    "JSON"),
                            new PublisherMocks.MockMessageHook());

            String message =
                    "{ \"destination\": \"destination\", \"value\": \"someValue\", \"key\": \"someKey\" }";
            CompletableFuture<String> future = publisher.sendMessage("user", "sessionId", message);
            CreditsException creditsException =
                    assertCreditsException(future, ClientError.INVALID_PAYLOAD_FORMAT);
            assertThat(creditsException.getClientErrorMsg()).startsWith("Invalid key");
        }
    }

    static Stream<RuntimeException> sendExceptions() {
        return Stream.of(
                new InterruptException("Fake interrupted exception"),
                new IllegalStateException("Fake illegal state exception"),
                new RuntimeException("Fake runtime exception"),
                new KafkaException("Fake exception"));
    }

    @ParameterizedTest
    @MethodSource("sendExceptions")
    void shouldFailSendMessageDueToRuntimeException(RuntimeException sendException) {
        AtomicInteger successfullyDeliveredCount = new AtomicInteger();
        ProducerListener listener =
                (producer, record) -> successfullyDeliveredCount.incrementAndGet();
        for (boolean sendConfirmationEnabled : List.of(true, false)) {
            MessagePublisher publisher =
                    new MessagePublisher(
                            LoggerFactory.getLogger("Publisher"),
                            new PublisherMocks.MockProducerFactory(sendException, listener),
                            PublisherConfig.defaultConfig(
                                    PublisherConfig.SEND_CONFIRMATION_ENABLE,
                                    String.valueOf(sendConfirmationEnabled)),
                            new PublisherMocks.MockMessageHook());

            String message = "{ \"destination\": \"destination\", \"value\": \"someValue\" }";
            CompletableFuture<String> future = publisher.sendMessage("user", "sessionId", message);
            assertCreditsException(future, ClientError.SEND_FAILED);
            assertThat(successfullyDeliveredCount.get()).isEqualTo(0);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldFailSendMessageAsynchronously(boolean sendConfirmationEnabled) {
        MessagePublisher publisher =
                new MessagePublisher(
                        LoggerFactory.getLogger("Publisher"),
                        new PublisherMocks.MockProducerFactory(
                                (producer, record) -> {
                                    producer.errorNext(
                                            new RuntimeException("Fake async exception"));
                                }),
                        PublisherConfig.defaultConfig(
                                PublisherConfig.SEND_CONFIRMATION_ENABLE,
                                Boolean.toString(sendConfirmationEnabled)),
                        new PublisherMocks.MockMessageHook());

        String message = "{ \"destination\": \"destination\", \"value\": \"someValue\" }";
        CompletableFuture<String> future = publisher.sendMessage("user", "sessionId", message);

        if (sendConfirmationEnabled) {
            assertCreditsException(future, ClientError.BROKER_DELIVERY_FAILED);
        } else {
            assertThat(future.join()).isNull();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldSendMessageSuccessfully(boolean sendConfirmationEnabled)
            throws JsonMappingException, JsonProcessingException {
        AtomicInteger deliveryCount = new AtomicInteger();

        MessagePublisher publisher =
                new MessagePublisher(
                        LoggerFactory.getLogger("Publisher"),
                        new PublisherMocks.MockProducerFactory(
                                (producer, record) -> {
                                    deliveryCount.incrementAndGet();
                                    producer.completeNext();
                                }),
                        PublisherConfig.defaultConfig(
                                PublisherConfig.SEND_CONFIRMATION_ENABLE,
                                Boolean.toString(sendConfirmationEnabled),
                                "publisher.map.destination.to",
                                "topic1,topic2"),
                        new PublisherMocks.MockMessageHook());

        String message = "{ \"destination\": \"destination\", \"value\": \"someValue\" }";
        CompletableFuture<String> future = publisher.sendMessage("user", "sessionId", message);
        assertThat(future.join()).isNull();
        assertThat(deliveryCount.get()).isEqualTo(2);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldSendMessagePartially(boolean sendConfirmationEnabled)
            throws JsonMappingException, JsonProcessingException {
        AtomicInteger successfullyDeliveredCount = new AtomicInteger();
        AtomicInteger topicCount = new AtomicInteger();
        ProducerListener listener =
                (producer, record) -> {
                    if (topicCount.incrementAndGet() == 2) {
                        // Simulate an async error for the second topic after the first one
                        // succeeds
                        producer.errorNext(
                                new RuntimeException("Simulated error for the second topic"));
                    } else {
                        successfullyDeliveredCount.incrementAndGet();
                        producer.completeNext();
                    }
                };

        MessagePublisher publisher =
                new MessagePublisher(
                        LoggerFactory.getLogger("Publisher"),
                        new PublisherMocks.MockProducerFactory(listener),
                        PublisherConfig.defaultConfig(
                                PublisherConfig.SEND_CONFIRMATION_ENABLE,
                                Boolean.toString(sendConfirmationEnabled),
                                "publisher.map.destination.to",
                                "topic1,topic2"),
                        new PublisherMocks.MockMessageHook());

        String message = "{ \"destination\": \"destination\", \"value\": \"someValue\" }";
        CompletableFuture<String> future = publisher.sendMessage("user", "sessionId", message);
        if (sendConfirmationEnabled) {
            assertCreditsException(future, ClientError.BROKER_DELIVERY_FAILED);
        } else {
            assertThat(future.join()).isNull();
        }
        assertThat(successfullyDeliveredCount.get()).isEqualTo(1);
    }

    static CreditsException assertCreditsException(
            CompletableFuture<String> future, ClientError error) {
        Throwable ex = future.handle((result, e) -> e).join();
        assertThat(ex).isInstanceOf(CreditsException.class);
        CreditsException creditsException = (CreditsException) ex;
        assertThat(creditsException.getClientErrorCode()).isEqualTo(error.code());
        assertThat(creditsException.getMessage()).isEqualTo(error.message());
        return creditsException;
    }
}
