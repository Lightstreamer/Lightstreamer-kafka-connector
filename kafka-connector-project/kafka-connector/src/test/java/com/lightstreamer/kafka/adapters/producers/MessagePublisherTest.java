
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
import static com.lightstreamer.kafka.adapters.config.PublisherConfig.SEND_CONFIRMATION_MODE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SendConfirmationMode.DETAILED;
import static com.lightstreamer.kafka.adapters.producers.MessageHandler.ClientError.MESSAGE_HOOK_FAILED;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.interfaces.metadata.CreditsException;
import com.lightstreamer.kafka.adapters.config.PublisherConfig;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.PayloadConverterType;
import com.lightstreamer.kafka.adapters.producers.MessageHandler.ClientError;
import com.lightstreamer.kafka.adapters.producers.MessagePublisher.DetailedHandler;
import com.lightstreamer.kafka.adapters.pub.messages.PublishingException;
import com.lightstreamer.kafka.adapters.pub.messages.converters.PayloadConverter;
import com.lightstreamer.kafka.test_utils.PublisherMocks;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
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
                        return new MockProducer<>();
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
                        SEND_CONFIRMATION_MODE,
                        DETAILED.toString());
        MessagePublisher publisher =
                new MessagePublisher(
                        LoggerFactory.getLogger("Publisher"),
                        factory,
                        new PublisherConfig(config),
                        new PublisherMocks.MockMessageHook(
                                () -> new FakePayloadConverter("key"),
                                () -> new FakePayloadConverter("value")));
        assertThat(publisher.confirmationModeHandler()).isInstanceOf(DetailedHandler.class);
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
                        createProducerFactory(),
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
                        createProducerFactory(),
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
    void shouldFailSendMessageDueToInvalidDestination(String message) {
        PublisherMocks.MockMessageHook messageHook = new PublisherMocks.MockMessageHook();
        MessagePublisher publisher =
                new MessagePublisher(
                        LoggerFactory.getLogger("Publisher"),
                        createProducerFactory(),
                        PublisherConfig.defaultConfig(),
                        messageHook);

        CompletableFuture<String> future = publisher.sendMessage("user", "sessionId", message);
        CompletionException exception = assertThrows(CompletionException.class, future::join);
        assertThat(exception).hasCauseThat().isInstanceOf(CreditsException.class);
    }

    @Test
    void shouldFailSendMessageDueToInvalidDestinationMapping() {
        PublisherMocks.MockMessageHook messageHook = new PublisherMocks.MockMessageHook();
        MessagePublisher publisher =
                new MessagePublisher(
                        LoggerFactory.getLogger("Publisher"),
                        createProducerFactory(),
                        PublisherConfig.defaultConfig(),
                        messageHook);

        String message = "{ \"destination\": \"invalidMapping\", \"value\": \"someValue\" }";
        CompletableFuture<String> future = publisher.sendMessage("user", "sessionId", message);
        CompletionException exception = assertThrows(CompletionException.class, future::join);
        assertThat(exception).hasCauseThat().isInstanceOf(CreditsException.class);
    }

    @Test
    void shouldFailDueToInvalidValuePayloadConverterType() {
        PublisherMocks.MockMessageHook messageHook = new PublisherMocks.MockMessageHook();
        MessagePublisher publisher =
                new MessagePublisher(
                        LoggerFactory.getLogger("Publisher"),
                        createProducerFactory(),
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
    void shouldFailDueToInvalidKeyPayloadConverterType() {
        PublisherMocks.MockMessageHook messageHook = new PublisherMocks.MockMessageHook();
        MessagePublisher publisher =
                new MessagePublisher(
                        LoggerFactory.getLogger("Publisher"),
                        createProducerFactory(),
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

    @Test
    void shouldSendMessageSuccessfully() {
        PublisherMocks.MockMessageHook messageHook = new PublisherMocks.MockMessageHook();
        MessagePublisher publisher =
                new MessagePublisher(
                        LoggerFactory.getLogger("Publisher"),
                        createProducerFactory(),
                        PublisherConfig.defaultConfig(),
                        messageHook);

        String message = "{ \"destination\": \"destination\", \"value\": \"someValue\" }";
        CompletableFuture<String> future = publisher.sendMessage("user", "sessionId", message);
        assertThat(future.join()).isNull();
    }

    private ProducerFactory createProducerFactory() {
        return new ProducerFactory() {

            @Override
            public Producer<JsonNode, JsonNode> createProducer(
                    Map<String, Object> configs,
                    Serializer<JsonNode> keySerializer,
                    Serializer<JsonNode> valueSerializer) {
                return new MockProducer<>(true, null, keySerializer, valueSerializer);
            }
        };
    }
}
