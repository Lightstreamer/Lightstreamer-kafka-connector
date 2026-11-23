
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
import static com.lightstreamer.kafka.adapters.producers.MessageHandler.ClientError.MESSAGE_HOOK_FAILED;

import static org.junit.jupiter.api.Assertions.assertThrows;

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

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
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
        Consumer<Map<String, Object>> basePropConsumer = props -> baseProps.set(props);

        AtomicReference<Serializer<JsonNode>> keySerializerRef = new AtomicReference<>();
        Consumer<Serializer<JsonNode>> keySerializerConsumer = s -> keySerializerRef.set(s);
        AtomicReference<Serializer<JsonNode>> valueSerializerRef = new AtomicReference<>();
        Consumer<Serializer<JsonNode>> valueSerializerConsumer = s -> valueSerializerRef.set(s);
        ProducerFactory factory =
                new ProducerFactory() {

                    @Override
                    public Producer<JsonNode, JsonNode> createProducer(
                            Map<String, Object> configs,
                            Serializer<JsonNode> keySerializer,
                            Serializer<JsonNode> valueSerializer) {
                        basePropConsumer.accept(configs);
                        keySerializerConsumer.accept(keySerializer);
                        valueSerializerConsumer.accept(valueSerializer);
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
        MessagePublisher publisher =
                new MessagePublisher(
                        LoggerFactory.getLogger("Publisher"),
                        new PublisherMocks.MockProducerFactory(),
                        PublisherConfig.defaultConfig(),
                        new PublisherMocks.MockMessageHook());

        CompletableFuture<String> future = publisher.sendMessage("user", "sessionId", "");
        assertThat(future.isCompletedExceptionally()).isTrue();
        assertThrows(CompletionException.class, future::join);
        CompletionException exception = assertThrows(CompletionException.class, future::join);
        assertThat(exception)
                .hasCauseThat()
                .hasMessageThat()
                .contains(ClientError.MALFORMED_MESSAGE.message());
    }

    @Test
    void shouldFailSendMessageDueToPublishingException() {
        PublisherMocks.MockMessageHook messageHook = new PublisherMocks.MockMessageHook();
        messageHook.setPublishingException(new PublishingException("Fake publishing exception"));
        MessagePublisher publisher =
                new MessagePublisher(
                        LoggerFactory.getLogger("Publisher"),
                        new PublisherMocks.MockProducerFactory(),
                        PublisherConfig.defaultConfig(),
                        messageHook);

        String message =
                """
          {
            "destination": "destination",
            "value": "someValue"
          }
                """;
        CompletableFuture<String> future = publisher.sendMessage("user", "sessionId", message);
        CompletionException exception = assertThrows(CompletionException.class, future::join);
        assertThat(exception).hasCauseThat().isInstanceOf(CreditsException.class);
        assertThat(exception)
                .hasCauseThat()
                .hasMessageThat()
                .isEqualTo(MESSAGE_HOOK_FAILED.message());
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
        PublisherMocks.MockMessageHook messageHook = new PublisherMocks.MockMessageHook();
        MessagePublisher publisher =
                new MessagePublisher(
                        LoggerFactory.getLogger("Publisher"),
                        new PublisherMocks.MockProducerFactory(),
                        PublisherConfig.defaultConfig(),
                        messageHook);

        CompletableFuture<String> future = publisher.sendMessage("user", "sessionId", message);
        CompletionException exception = assertThrows(CompletionException.class, future::join);
        assertThat(exception).hasCauseThat().isInstanceOf(CreditsException.class);
        assertThat(exception)
                .hasCauseThat()
                .hasMessageThat()
                .isEqualTo(ClientError.MISSING_DESTINATION.message());
    }

    @Test
    void shouldFailSendMessageDueDestinationNotMapped() {
        PublisherMocks.MockMessageHook messageHook = new PublisherMocks.MockMessageHook();
        MessagePublisher publisher =
                new MessagePublisher(
                        LoggerFactory.getLogger("Publisher"),
                        new PublisherMocks.MockProducerFactory(),
                        PublisherConfig.defaultConfig(),
                        messageHook);

        String message = "{ \"destination\": \"invalidMapping\", \"value\": \"someValue\" }";
        CompletableFuture<String> future = publisher.sendMessage("user", "sessionId", message);
        CompletionException exception = assertThrows(CompletionException.class, future::join);
        assertThat(exception).hasCauseThat().isInstanceOf(CreditsException.class);
        assertThat(exception)
                .hasCauseThat()
                .hasMessageThat()
                .isEqualTo(ClientError.UNMAPPED_DESTINATION.message());
    }

    @Test
    void shouldFailSendMessageDueToMissingPayloadValue() {
        PublisherMocks.MockMessageHook messageHook = new PublisherMocks.MockMessageHook();
        MessagePublisher publisher =
                new MessagePublisher(
                        LoggerFactory.getLogger("Publisher"),
                        new PublisherMocks.MockProducerFactory(),
                        PublisherConfig.defaultConfig(),
                        messageHook);

        String message = "{ \"destination\": \"destination\" }";
        CompletableFuture<String> future = publisher.sendMessage("user", "sessionId", message);
        CompletionException exception = assertThrows(CompletionException.class, future::join);
        assertThat(exception).hasCauseThat().isInstanceOf(CreditsException.class);
        assertThat(exception)
                .hasCauseThat()
                .hasMessageThat()
                .isEqualTo(ClientError.MISSING_PAYLOAD_VALUE.message());
    }

    @Test()
    @Timeout(5)
    void shouldFailSendMessageDueToInvalidValuePayloadConverterType() {
        PublisherMocks.MockMessageHook messageHook = new PublisherMocks.MockMessageHook();
        MessagePublisher publisher =
                new MessagePublisher(
                        LoggerFactory.getLogger("Publisher"),
                        new PublisherMocks.MockProducerFactory(),
                        PublisherConfig.defaultConfig(
                                Map.of(PublisherConfig.MESSAGE_VALUE_PAYLOAD_CONVERTER, "JSON")),
                        messageHook);

        String message =
                "{ \"destination\": \"destination\", \"value\": \"someValue\", \"key\": \"someKey\" }";
        CompletableFuture<String> future = publisher.sendMessage("user", "sessionId", message);
        CompletionException exception = assertThrows(CompletionException.class, future::join);
        CreditsException creditsException = (CreditsException) exception.getCause();
        assertThat(creditsException)
                .hasMessageThat()
                .isEqualTo(ClientError.INVALID_PAYLOAD_FORMAT.message());
        assertThat(creditsException.getClientErrorMsg()).startsWith("Invalid value");
    }

    @Test
    void shouldFailSendMessageDueToInvalidKeyPayloadConverterType() {
        PublisherMocks.MockMessageHook messageHook = new PublisherMocks.MockMessageHook();
        MessagePublisher publisher =
                new MessagePublisher(
                        LoggerFactory.getLogger("Publisher"),
                        new PublisherMocks.MockProducerFactory(),
                        PublisherConfig.defaultConfig(
                                Map.of(PublisherConfig.MESSAGE_KEY_PAYLOAD_CONVERTER, "JSON")),
                        messageHook);

        String message =
                "{ \"destination\": \"destination\", \"value\": \"someValue\", \"key\": \"someKey\" }";
        CompletableFuture<String> future = publisher.sendMessage("user", "sessionId", message);
        CompletionException exception = assertThrows(CompletionException.class, future::join);
        CreditsException creditsException = (CreditsException) exception.getCause();
        assertThat(creditsException)
                .hasMessageThat()
                .isEqualTo(ClientError.INVALID_PAYLOAD_FORMAT.message());
        assertThat(creditsException.getClientErrorMsg()).startsWith("Invalid key");
    }

    static Stream<RuntimeException> runtimeExceptions() {
        return Stream.of(
                new InterruptException("Fake interrupted exception"),
                new IllegalStateException("Fake illegal state exception"),
                new RuntimeException("Fake runtime exception"),
                new KafkaException("Fake exception"));
    }

    @ParameterizedTest
    @MethodSource("runtimeExceptions")
    void shouldFailSendMessageDueToRuntimeException(RuntimeException runtimeException) {
        PublisherMocks.MockMessageHook messageHook = new PublisherMocks.MockMessageHook();
        ProducerFactory producerFactory = new PublisherMocks.MockProducerFactory(runtimeException);
        MessagePublisher publisher =
                new MessagePublisher(
                        LoggerFactory.getLogger("Publisher"),
                        producerFactory,
                        PublisherConfig.defaultConfig(),
                        messageHook);

        String message = "{ \"destination\": \"destination\", \"value\": \"someValue\" }";
        CompletableFuture<String> future = publisher.sendMessage("user", "sessionId", message);
        CompletionException exception = assertThrows(CompletionException.class, future::join);
        CreditsException creditsException = (CreditsException) exception.getCause();
        assertThat(creditsException).hasMessageThat().isEqualTo(ClientError.SEND_FAILED.message());
    }

//     @ParameterizedTest
//     @ValueSource(booleans = {true, false})
    void shouldFailSendMessageAsynchronously(boolean enabled) {
        PublisherMocks.MockMessageHook messageHook = new PublisherMocks.MockMessageHook();
        ProducerFactory producerFactory =
                new PublisherMocks.MockProducerFactory(
                        null, new RuntimeException("Fake async exception"));
        MessagePublisher publisher =
                new MessagePublisher(
                        LoggerFactory.getLogger("Publisher"),
                        producerFactory,
                        PublisherConfig.defaultConfig(
                                Map.of(
                                        PublisherConfig.SEND_CONFIRMATION_ENABLE,
                                        Boolean.toString(enabled))),
                        messageHook);

        String message = "{ \"destination\": \"destination\", \"value\": \"someValue\" }";
        CompletableFuture<String> future = publisher.sendMessage("user", "sessionId", message);

        if (enabled) {
            CompletionException exception = assertThrows(CompletionException.class, future::join);
            CreditsException creditsException = (CreditsException) exception.getCause();
            assertThat(creditsException)
                    .hasMessageThat()
                    .isEqualTo(ClientError.BROKER_DELIVERY_FAILED.message());

        } else {
            assertThat(future.join()).isNull();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldSendMessageSuccessfully(boolean enabled)
            throws JsonMappingException, JsonProcessingException {
        PublisherMocks.MockMessageHook messageHook = new PublisherMocks.MockMessageHook();
        PublisherConfig defaultConfig =
                PublisherConfig.defaultConfig(
                        Map.of(
                                PublisherConfig.SEND_CONFIRMATION_ENABLE,
                                Boolean.toString(enabled)));
        MessagePublisher publisher =
                new MessagePublisher(
                        LoggerFactory.getLogger("Publisher"),
                        new PublisherMocks.MockProducerFactory(),
                        defaultConfig,
                        messageHook);

        String message = "{ \"destination\": \"destination\", \"value\": \"someValue\" }";
        CompletableFuture<String> future = publisher.sendMessage("user", "sessionId", message);
        assertThat(future.join()).isNull();
    }

    //     @ParameterizedTest
    //     @EnumSource(SendConfirmationMode.class)
    //     void shouldSendMessageSuccessfullyForAllTopics(SendConfirmationMode confirmationMode)
    //             throws JsonMappingException, JsonProcessingException {
    //         AtomicReference<MockProducer<?, ?>> producerRef = new AtomicReference<>();

    //         PublisherMocks.MockMessageHook messageHook = new PublisherMocks.MockMessageHook();
    //         PublisherConfig defaultConfig =
    //                 PublisherConfig.defaultConfig(
    //                         Map.of(
    //                                 PublisherConfig.SEND_CONFIRMATION_ENABLE,
    //                                 confirmationMode.toString(),
    //                                 "publisher.map.destination.to",
    //                                 "topic1,topic2"));
    //         MessagePublisher publisher =
    //                 new MessagePublisher(
    //                         LoggerFactory.getLogger("Publisher"),
    //                         new PublisherMocks.MockProducerFactory(producerRef::set),
    //                         defaultConfig,
    //                         messageHook);

    //         String message = "{ \"destination\": \"destination\", \"value\": \"someValue\" }";
    //         CompletableFuture<String> future = publisher.sendMessage("user", "sessionId",
    // message);
    //         MockProducer<?, ?> producer = producerRef.get();
    //         assertThat(producer.history()).hasSize(2);
    //         assertThat(future.join()).isNull();
    //     }
}
