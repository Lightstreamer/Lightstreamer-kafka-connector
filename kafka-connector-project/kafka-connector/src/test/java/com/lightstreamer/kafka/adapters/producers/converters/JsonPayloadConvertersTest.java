
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

package com.lightstreamer.kafka.adapters.producers.converters;

import static com.google.common.truth.Truth.assertThat;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;

import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.stream.Stream;

public class JsonPayloadConvertersTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Builds the node the same way MessagePublisher does when it parses a client message, so the
    // converters are tested with the node types Jackson actually produces (for example, IntNode
    // or LongNode for integers and DoubleNode for decimals).
    private static JsonNode json(String text) {
        try {
            return MAPPER.readTree(text);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    static Stream<Arguments> provideValidTextNodes() {
        return Stream.of(
                Arguments.of(json("\"\""), new byte[0]),
                Arguments.of(json("\"test\""), "test".getBytes()));
    }

    @ParameterizedTest
    @MethodSource("provideValidTextNodes")
    void shouldConvertToString(JsonNode textNode, byte[] expected) {
        try (JsonPayloadConverters.StringPayloadConverter converter =
                new JsonPayloadConverters.StringPayloadConverter()) {

            byte[] result = converter.serialize("topic", textNode);
            assertThat(result).isEqualTo(expected);
        }
    }

    static Stream<Arguments> provideInvalidTextNodes() {
        return Stream.of(
                Arguments.of(json("123"), JsonNodeType.NUMBER),
                Arguments.of(json("null"), JsonNodeType.NULL));
    }

    @ParameterizedTest
    @MethodSource("provideInvalidTextNodes")
    void shouldFailToConvertToString(JsonNode node, JsonNodeType foundType) {
        try (JsonPayloadConverters.StringPayloadConverter converter =
                new JsonPayloadConverters.StringPayloadConverter()) {

            SerializationException ce =
                    assertThrows(
                            SerializationException.class, () -> converter.serialize("topic", node));
            assertThat(ce)
                    .hasMessageThat()
                    .contains("Invalid value: expected STRING but found " + foundType);
        }
    }

    static Stream<Arguments> provideValidIntNodes() {
        return Stream.of(
                Arguments.of(json("1"), ByteBuffer.allocate(4).putInt(1).array()),
                Arguments.of(json("0"), ByteBuffer.allocate(4).putInt(0).array()),
                Arguments.of(json("-1"), ByteBuffer.allocate(4).putInt(-1).array()),
                Arguments.of(
                        json("2147483647"),
                        ByteBuffer.allocate(4).putInt(Integer.MAX_VALUE).array()));
    }

    @ParameterizedTest
    @MethodSource("provideValidIntNodes")
    void shouldConvertToInteger(JsonNode intNode, byte[] expected) {
        try (JsonPayloadConverters.IntegerPayloadConverter converter =
                new JsonPayloadConverters.IntegerPayloadConverter()) {

            byte[] result = converter.serialize("topic", intNode);
            assertThat(result).isEqualTo(expected);
        }
    }

    static Stream<Arguments> provideInvalidIntNodes() {
        return Stream.of(
                // Fails only !isIntegralNumber(): the value is within the int range.
                Arguments.of(json("1.5"), JsonNodeType.NUMBER),
                // Fails only !canConvertToInt(): asInt() would wrap around to 1.
                Arguments.of(json("4294967297"), JsonNodeType.NUMBER),
                Arguments.of(json("null"), JsonNodeType.NULL));
    }

    @ParameterizedTest
    @MethodSource("provideInvalidIntNodes")
    void shouldFailToConvertToInteger(JsonNode node, JsonNodeType foundType) {
        try (JsonPayloadConverters.IntegerPayloadConverter converter =
                new JsonPayloadConverters.IntegerPayloadConverter()) {

            SerializationException ce =
                    assertThrows(
                            SerializationException.class, () -> converter.serialize("topic", node));
            assertThat(ce)
                    .hasMessageThat()
                    .contains("Invalid value: expected INTEGER but found " + foundType);
        }
    }

    static Stream<Arguments> provideValidBooleanNodes() {
        return Stream.of(
                Arguments.of(json("true"), ByteBuffer.allocate(1).put((byte) 1).array()),
                Arguments.of(json("false"), ByteBuffer.allocate(1).put((byte) 0).array()));
    }

    @ParameterizedTest
    @MethodSource("provideValidBooleanNodes")
    void shouldConvertToBoolean(JsonNode booleanNode, byte[] expected) {
        try (JsonPayloadConverters.BooleanPayloadConverter converter =
                new JsonPayloadConverters.BooleanPayloadConverter()) {

            byte[] result = converter.serialize("topic", booleanNode);
            assertThat(result).isEqualTo(expected);
        }
    }

    static Stream<Arguments> provideInvalidBooleanNodes() {
        return Stream.of(
                // A boolean-looking string must not be accepted as a boolean.
                Arguments.of(json("\"true\""), JsonNodeType.STRING),
                Arguments.of(json("null"), JsonNodeType.NULL));
    }

    @ParameterizedTest
    @MethodSource("provideInvalidBooleanNodes")
    void shouldFailToConvertToBoolean(JsonNode node, JsonNodeType foundType) {
        try (JsonPayloadConverters.BooleanPayloadConverter converter =
                new JsonPayloadConverters.BooleanPayloadConverter()) {

            SerializationException ce =
                    assertThrows(
                            SerializationException.class, () -> converter.serialize("topic", node));
            assertThat(ce)
                    .hasMessageThat()
                    .contains("Invalid value: expected BOOLEAN but found " + foundType);
        }
    }

    static Stream<Arguments> provideValidDoubleNodes() {
        return Stream.of(
                Arguments.of(json("3.14"), ByteBuffer.allocate(8).putDouble(3.14).array()),
                Arguments.of(json("100"), ByteBuffer.allocate(8).putDouble(100).array()));
    }

    @ParameterizedTest
    @MethodSource("provideValidDoubleNodes")
    void shouldConvertToDouble(JsonNode doubleNode, byte[] expected) {
        try (JsonPayloadConverters.DoublePayloadConverter converter =
                new JsonPayloadConverters.DoublePayloadConverter()) {

            byte[] result = converter.serialize("topic", doubleNode);
            assertThat(result).isEqualTo(expected);
        }
    }

    static Stream<Arguments> provideInvalidDoubleNodes() {
        return Stream.of(
                // A numeric-looking string must not be accepted as a number.
                Arguments.of(json("\"1.5\""), JsonNodeType.STRING),
                Arguments.of(json("null"), JsonNodeType.NULL));
    }

    @ParameterizedTest
    @MethodSource("provideInvalidDoubleNodes")
    void shouldFailToConvertToDouble(JsonNode node, JsonNodeType foundType) {
        try (JsonPayloadConverters.DoublePayloadConverter converter =
                new JsonPayloadConverters.DoublePayloadConverter()) {

            SerializationException ce =
                    assertThrows(
                            SerializationException.class, () -> converter.serialize("topic", node));
            assertThat(ce)
                    .hasMessageThat()
                    .contains("Invalid value: expected a number but found " + foundType);
        }
    }

    static Stream<Arguments> provideValidFloatNodes() {
        return Stream.of(
                Arguments.of(json("3.14"), ByteBuffer.allocate(4).putFloat(3.14f).array()),
                Arguments.of(json("100"), ByteBuffer.allocate(4).putFloat(100).array()),
                Arguments.of(
                        json("3.4028235e38"),
                        ByteBuffer.allocate(4).putFloat(Float.MAX_VALUE).array()));
    }

    @ParameterizedTest
    @MethodSource("provideValidFloatNodes")
    void shouldConvertToFloat(JsonNode floatNode, byte[] expected) {
        try (JsonPayloadConverters.FloatPayloadConverter converter =
                new JsonPayloadConverters.FloatPayloadConverter()) {

            byte[] result = converter.serialize("topic", floatNode);
            assertThat(result).isEqualTo(expected);
        }
    }

    static Stream<Arguments> provideInvalidFloatNodes() {
        return Stream.of(
                // A numeric-looking string must not be accepted as a number.
                Arguments.of(json("\"1.5\""), JsonNodeType.STRING),
                Arguments.of(json("null"), JsonNodeType.NULL));
    }

    @ParameterizedTest
    @MethodSource("provideInvalidFloatNodes")
    void shouldFailToConvertToFloat(JsonNode node, JsonNodeType foundType) {
        try (JsonPayloadConverters.FloatPayloadConverter converter =
                new JsonPayloadConverters.FloatPayloadConverter()) {

            SerializationException ce =
                    assertThrows(
                            SerializationException.class, () -> converter.serialize("topic", node));
            assertThat(ce)
                    .hasMessageThat()
                    .contains("Invalid value: expected a number but found " + foundType);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"1e40", "-1e40"})
    void shouldFailToConvertToFloatWhenOutOfRange(String text) {
        try (JsonPayloadConverters.FloatPayloadConverter converter =
                new JsonPayloadConverters.FloatPayloadConverter()) {

            SerializationException ce =
                    assertThrows(
                            SerializationException.class,
                            () -> converter.serialize("topic", json(text)));
            assertThat(ce)
                    .hasMessageThat()
                    .contains("Invalid value: expected a number within the FLOAT range");
        }
    }

    static Stream<Arguments> provideValidLongNodes() {
        return Stream.of(
                Arguments.of(json("123"), ByteBuffer.allocate(8).putLong(123L).array()),
                Arguments.of(
                        json("9223372036854775807"),
                        ByteBuffer.allocate(8).putLong(Long.MAX_VALUE).array()));
    }

    @ParameterizedTest
    @MethodSource("provideValidLongNodes")
    void shouldConvertToLong(JsonNode longNode, byte[] expected) {
        try (JsonPayloadConverters.LongPayloadConverter converter =
                new JsonPayloadConverters.LongPayloadConverter()) {

            byte[] result = converter.serialize("topic", longNode);
            assertThat(result).isEqualTo(expected);
        }
    }

    static Stream<Arguments> provideInvalidLongNodes() {
        return Stream.of(
                // Fails only !isIntegralNumber(): the value is within the long range.
                Arguments.of(json("1.5"), JsonNodeType.NUMBER),
                // Fails only !canConvertToLong(): the value is integral but exceeds Long.MAX_VALUE.
                Arguments.of(json("9223372036854775808"), JsonNodeType.NUMBER),
                Arguments.of(json("null"), JsonNodeType.NULL));
    }

    @ParameterizedTest
    @MethodSource("provideInvalidLongNodes")
    void shouldFailToConvertToLong(JsonNode node, JsonNodeType foundType) {
        try (JsonPayloadConverters.LongPayloadConverter converter =
                new JsonPayloadConverters.LongPayloadConverter()) {

            SerializationException ce =
                    assertThrows(
                            SerializationException.class, () -> converter.serialize("topic", node));
            assertThat(ce)
                    .hasMessageThat()
                    .contains("Invalid value: expected LONG but found " + foundType);
        }
    }

    static Stream<Arguments> provideValidShortNodes() {
        return Stream.of(
                Arguments.of(json("123"), ByteBuffer.allocate(2).putShort((short) 123).array()),
                Arguments.of(
                        json("32767"), ByteBuffer.allocate(2).putShort(Short.MAX_VALUE).array()),
                Arguments.of(
                        json("-32768"), ByteBuffer.allocate(2).putShort(Short.MIN_VALUE).array()));
    }

    @ParameterizedTest
    @MethodSource("provideValidShortNodes")
    void shouldConvertToShort(JsonNode shortNode, byte[] expected) {
        try (JsonPayloadConverters.ShortPayloadConverter converter =
                new JsonPayloadConverters.ShortPayloadConverter()) {

            byte[] result = converter.serialize("topic", shortNode);
            assertThat(result).isEqualTo(expected);
        }
    }

    static Stream<Arguments> provideInvalidShortNodes() {
        return Stream.of(
                // Fails only !isIntegralNumber(): intValue() is 1, a valid short.
                Arguments.of(json("1.5"), JsonNodeType.NUMBER),
                // Fails only !canConvertToInt(): intValue() wraps around to 1, a valid short.
                Arguments.of(json("4294967297"), JsonNodeType.NUMBER),
                // Fails only intValue() < Short.MIN_VALUE.
                Arguments.of(json("-32769"), JsonNodeType.NUMBER),
                // Fails only intValue() > Short.MAX_VALUE.
                Arguments.of(json("32768"), JsonNodeType.NUMBER),
                Arguments.of(json("null"), JsonNodeType.NULL));
    }

    @ParameterizedTest
    @MethodSource("provideInvalidShortNodes")
    void shouldFailToConvertToShort(JsonNode node, JsonNodeType foundType) {
        try (JsonPayloadConverters.ShortPayloadConverter converter =
                new JsonPayloadConverters.ShortPayloadConverter()) {

            SerializationException ce =
                    assertThrows(
                            SerializationException.class, () -> converter.serialize("topic", node));
            assertThat(ce)
                    .hasMessageThat()
                    .contains("Invalid value: expected SHORT but found " + foundType);
        }
    }
}
