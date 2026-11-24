
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

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import com.fasterxml.jackson.databind.node.TextNode;

public class JsonPayloadConvertersTest {

    static Stream<Arguments> provideTextNodes() {
        return Stream.of(
                Arguments.of(new TextNode(null), null),
                Arguments.of(new TextNode(""), new byte[0]),
                Arguments.of(new TextNode("test"), "test".getBytes()));
    }

    @ParameterizedTest
    @MethodSource("provideTextNodes")
    void shouldConvertJsonToString(TextNode textNode, byte[] expected) {
        try (JsonPayloadConverters.StringPayloadConverter converter =
                new JsonPayloadConverters.StringPayloadConverter()) {

            byte[] result = converter.serialize("topic", textNode);
            assertThat(result).isEqualTo(expected);
        }
    }

    static Stream<Arguments> invalidTextNodes() {
        return Stream.of(
                Arguments.of(new IntNode(1), JsonNodeType.NUMBER),
                Arguments.of(new ObjectNode(null), JsonNodeType.OBJECT),
                Arguments.of(new LongNode(1L), JsonNodeType.NUMBER));
    }

    @ParameterizedTest
    @MethodSource("invalidTextNodes")
    void shouldFailConvertJsonToString(JsonNode textNode, JsonNodeType foundType) {
        try (JsonPayloadConverters.StringPayloadConverter converter =
                new JsonPayloadConverters.StringPayloadConverter()) {

            SerializationException ce =
                    assertThrows(
                            SerializationException.class,
                            () -> converter.serialize("topic", textNode));
            assertThat(ce)
                    .hasMessageThat()
                    .contains("Invalid value: expected STRING but found " + foundType);
        }
    }

    static Stream<Arguments> provideIntNodes() {
        return Stream.of(
                Arguments.of(new IntNode(1), ByteBuffer.allocate(4).putInt(1).array()),
                Arguments.of(new IntNode(0), ByteBuffer.allocate(4).putInt(0).array()),
                Arguments.of(new IntNode(-1), ByteBuffer.allocate(4).putInt(-1).array()));
    }

    @ParameterizedTest
    @MethodSource("provideIntNodes")
    void shouldConvertJsonToInteger(IntNode intNode, byte[] expected) {
        try (JsonPayloadConverters.IntegerPayloadConverter converter =
                new JsonPayloadConverters.IntegerPayloadConverter()) {

            byte[] result = converter.serialize("topic", intNode);
            assertThat(result).isEqualTo(expected);
        }
    }

    static Stream<Arguments> invalidIntNodes() {
        return Stream.of(
                Arguments.of(new TextNode("text"), JsonNodeType.STRING),
                Arguments.of(new ObjectNode(null), JsonNodeType.OBJECT));
    }

    @ParameterizedTest
    @MethodSource("invalidIntNodes")
    void shouldFailConvertJsonToInteger(JsonNode node, JsonNodeType foundType) {
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

    static Stream<Arguments> provideBooleanNodes() {
        return Stream.of(
                Arguments.of(BooleanNode.TRUE, ByteBuffer.allocate(1).put((byte) 1).array()),
                Arguments.of(BooleanNode.FALSE, ByteBuffer.allocate(1).put((byte) 0).array()));
    }

    @ParameterizedTest
    @MethodSource("provideBooleanNodes")
    void shouldConvertJsonToBoolean(BooleanNode booleanNode, byte[] expected) {
        try (JsonPayloadConverters.BooleanPayloadConverter converter =
                new JsonPayloadConverters.BooleanPayloadConverter()) {

            byte[] result = converter.serialize("topic", booleanNode);
            assertThat(result).isEqualTo(expected);
        }
    }

    static Stream<Arguments> invalidBooleanNodes() {
        return Stream.of(
                Arguments.of(new TextNode("text"), JsonNodeType.STRING),
                Arguments.of(new ObjectNode(null), JsonNodeType.OBJECT));
    }

    @ParameterizedTest
    @MethodSource("invalidBooleanNodes")
    void shouldFailConvertJsonToBoolean(JsonNode node, JsonNodeType foundType) {
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

    static Stream<Arguments> provideDoubleNodes() {
        return Stream.of(
                Arguments.of(new DoubleNode(3.14), ByteBuffer.allocate(8).putDouble(3.14).array()),
                Arguments.of(new DoubleNode(100), ByteBuffer.allocate(8).putDouble(100).array()));
    }

    @ParameterizedTest
    @MethodSource("provideDoubleNodes")
    void shouldConvertJsonToDouble(DoubleNode doubleNode, byte[] expected) {
        try (JsonPayloadConverters.DoublePayloadConverter converter =
                new JsonPayloadConverters.DoublePayloadConverter()) {

            byte[] result = converter.serialize("topic", doubleNode);
            assertThat(result).isEqualTo(expected);
        }
    }

    static Stream<Arguments> invalidDoubleNodes() {
        return Stream.of(
                Arguments.of(new TextNode("text"), JsonNodeType.STRING),
                Arguments.of(new ObjectNode(null), JsonNodeType.OBJECT));
    }

    @ParameterizedTest
    @MethodSource("invalidDoubleNodes")
    void shouldFailConvertJsonToDouble(JsonNode node, JsonNodeType foundType) {
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

    static Stream<Arguments> provideFloatNodes() {
        return Stream.of(
                Arguments.of(new FloatNode(3.14f), ByteBuffer.allocate(4).putFloat(3.14f).array()),
                Arguments.of(new FloatNode(100), ByteBuffer.allocate(4).putFloat(100).array()));
    }

    @ParameterizedTest
    @MethodSource("provideFloatNodes")
    void shouldConvertJsonToFloat(FloatNode floatNode, byte[] expected) {
        try (JsonPayloadConverters.FloatPayloadConverter converter =
                new JsonPayloadConverters.FloatPayloadConverter()) {

            byte[] result = converter.serialize("topic", floatNode);
            assertThat(result).isEqualTo(expected);
        }
    }

    static Stream<Arguments> invalidFloatNodes() {
        return Stream.of(
                Arguments.of(new TextNode("text"), JsonNodeType.STRING),
                Arguments.of(new ObjectNode(null), JsonNodeType.OBJECT));
    }

    @ParameterizedTest
    @MethodSource("invalidFloatNodes")
    void shouldFailConvertJsonToFloat(JsonNode node, JsonNodeType foundType) {
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

    static Stream<Arguments> provideLongNodes() {
        return Stream.of(
                Arguments.of(
                        new LongNode(123L),
                        ByteBuffer.allocate(8).putLong(123L).array()),
                Arguments.of(
                        new LongNode(Long.MAX_VALUE),
                        ByteBuffer.allocate(8).putLong(Long.MAX_VALUE).array()));
    }

    @ParameterizedTest
    @MethodSource("provideLongNodes")
    void shouldConvertJsonToLong(LongNode longNode, byte[] expected) {
        try (JsonPayloadConverters.LongPayloadConverter converter =
                new JsonPayloadConverters.LongPayloadConverter()) {

            byte[] result = converter.serialize("topic", longNode);
            assertThat(result).isEqualTo(expected);
        }
    }

    static Stream<Arguments> invalidLongNodes() throws JacksonException {
        return Stream.of(
                Arguments.of(new TextNode("text"), JsonNodeType.STRING),
                Arguments.of(new ObjectNode(null), JsonNodeType.OBJECT),
                Arguments.of(new ObjectMapper().readTree("9223372036854775808"), 
                JsonNodeType.NUMBER));
    }

    @ParameterizedTest
    @MethodSource("invalidLongNodes")
    void shouldFailConvertJsonToLong(JsonNode node, JsonNodeType foundType) {
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

    static Stream<Arguments> provideShortNodes() {
        return Stream.of(
                Arguments.of(
                        new ShortNode((short) 123),
                        ByteBuffer.allocate(2).putShort((short) 123).array()),
                Arguments.of(
                        new ShortNode(Short.MAX_VALUE),
                        ByteBuffer.allocate(2).putShort(Short.MAX_VALUE).array()));
    }



    @ParameterizedTest
    @MethodSource("provideShortNodes")
    void shouldConvertJsonToShort(ShortNode shortNode, byte[] expected) {
        try (JsonPayloadConverters.ShortPayloadConverter converter =
                new JsonPayloadConverters.ShortPayloadConverter()) {

            byte[] result = converter.serialize("topic", shortNode);
            assertThat(result).isEqualTo(expected);
        }
    }

        static Stream<Arguments> invalidShortNodes() throws JsonProcessingException {
        return Stream.of(
                Arguments.of(new ObjectMapper().readTree("\"a text\""), JsonNodeType.STRING),
                Arguments.of(new IntNode(Short.MIN_VALUE - 1), JsonNodeType.NUMBER),
                Arguments.of(new IntNode(Short.MAX_VALUE + 1), JsonNodeType.NUMBER),
                Arguments.of(new ObjectMapper().readTree("32768"), JsonNodeType.NUMBER));
    }

    @ParameterizedTest
    @MethodSource("invalidShortNodes")
    void shouldFailConvertJsonToShort(JsonNode node, JsonNodeType foundType) {
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
