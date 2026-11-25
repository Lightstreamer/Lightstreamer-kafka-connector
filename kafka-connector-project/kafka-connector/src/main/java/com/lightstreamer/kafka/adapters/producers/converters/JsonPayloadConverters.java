
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.google.protobuf.DynamicMessage;
import com.lightstreamer.kafka.adapters.pub.messages.converters.PayloadConverter;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.BooleanSerializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.ShortSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.UUIDSerializer;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

class JsonPayloadConverters {

    private abstract static class DefaultPayloadConverter<T> implements PayloadConverter {

        private final Serializer<T> serializer;
        private boolean isKey;

        protected DefaultPayloadConverter(Serializer<T> serializer) {
            this.serializer = serializer;
        }

        @Override
        public final void configure(Map<String, ?> configs, boolean isKey) {
            serializer.configure(configs, isKey);
            this.isKey = isKey;
        }

        @Override
        public final void close() {
            serializer.close();
        }

        @Override
        public final byte[] serialize(String topic, JsonNode data) {
            if (data == null) {
                return null;
            }
            return doSerialize(topic, fromJson(data));
        }

        private byte[] doSerialize(String topic, T value) {
            return serializer.serialize(topic, value);
        }

        /**
         * Converts the given JSON node to the Java value handled by the wrapped serializer.
         *
         * @param node the JSON node to convert
         * @return the converted value
         * @throws SerializationException if the node cannot be converted to the expected type
         */
        protected abstract T fromJson(JsonNode node);

        /**
         * Indicates whether this converter has been configured to handle record keys.
         *
         * @return {@code true} if the converter handles keys, {@code false} if it handles values
         */
        protected boolean isKey() {
            return isKey;
        }

        /**
         * Creates an exception for a JSON node whose type does not match the expected type.
         *
         * @param nodeType the offending node type
         * @param expected a short description of the expected type
         * @param isKey {@code true} if the invalid data is a record key, {@code false} if it is a
         *     value
         * @return a new {@link SerializationException}
         */
        static SerializationException unexpectedType(
                JsonNodeType nodeType, String expected, boolean isKey) {
            return unexpected(nodeType.name(), expected, isKey);
        }

        /**
         * Creates an exception for a text whose content is not valid for the expected type. The
         * text is quoted in the message to make empty or blank content visible.
         *
         * @param found the offending text
         * @param expected a short description of the expected content
         * @param isKey {@code true} if the invalid data is a record key, {@code false} if it is a
         *     value
         * @return a new {@link SerializationException}
         */
        static SerializationException unexpectedString(
                String found, String expected, boolean isKey) {
            return unexpected("\"" + found + "\"", expected, isKey);
        }

        private static SerializationException unexpected(
                String found, String expected, boolean isKey) {
            return new SerializationException(
                    "Invalid %s: expected %s but found %s"
                            .formatted(isKey ? "key" : "value", expected, found));
        }
    }

    static class StringPayloadConverter extends DefaultPayloadConverter<String> {

        StringPayloadConverter() {
            super(new StringSerializer());
        }

        @Override
        protected String fromJson(JsonNode json) {
            if (!json.isTextual()) {
                throw unexpectedType(json.getNodeType(), "STRING", isKey());
            }
            return json.asText();
        }
    }

    static class IntegerPayloadConverter extends DefaultPayloadConverter<Integer> {

        IntegerPayloadConverter() {
            super(new IntegerSerializer());
        }

        @Override
        protected Integer fromJson(JsonNode json) {
            if (!json.isIntegralNumber() || !json.canConvertToInt()) {
                throw unexpectedType(json.getNodeType(), "INTEGER", isKey());
            }
            return json.asInt();
        }
    }

    static class BooleanPayloadConverter extends DefaultPayloadConverter<Boolean> {

        BooleanPayloadConverter() {
            super(new BooleanSerializer());
        }

        @Override
        protected Boolean fromJson(JsonNode json) {
            if (!json.isBoolean()) {
                throw unexpectedType(json.getNodeType(), "BOOLEAN", isKey());
            }
            return json.asBoolean();
        }
    }

    static class DoublePayloadConverter extends DefaultPayloadConverter<Double> {

        DoublePayloadConverter() {
            super(new DoubleSerializer());
        }

        @Override
        protected Double fromJson(JsonNode json) {
            if (!json.isNumber()) {
                throw unexpectedType(json.getNodeType(), "a number", isKey());
            }
            return json.asDouble();
        }
    }

    static class FloatPayloadConverter extends DefaultPayloadConverter<Float> {

        FloatPayloadConverter() {
            super(new FloatSerializer());
        }

        @Override
        protected Float fromJson(JsonNode json) {
            if (!json.isNumber()) {
                throw unexpectedType(json.getNodeType(), "a number", isKey());
            }
            float value = json.floatValue();
            if (Float.isInfinite(value)) {
                throw unexpectedType(
                        json.getNodeType(), "a number within the FLOAT range", isKey());
            }
            return value;
        }
    }

    static class LongPayloadConverter extends DefaultPayloadConverter<Long> {

        LongPayloadConverter() {
            super(new LongSerializer());
        }

        @Override
        protected Long fromJson(JsonNode json) {
            if (!json.isIntegralNumber() || !json.canConvertToLong()) {
                throw unexpectedType(json.getNodeType(), "LONG", isKey());
            }
            return json.asLong();
        }
    }

    static class ShortPayloadConverter extends DefaultPayloadConverter<Short> {

        ShortPayloadConverter() {
            super(new ShortSerializer());
        }

        @Override
        protected Short fromJson(JsonNode json) {
            if (!json.isIntegralNumber()
                    || !json.canConvertToInt()
                    || json.intValue() < Short.MIN_VALUE
                    || json.intValue() > Short.MAX_VALUE) {
                throw unexpectedType(json.getNodeType(), "SHORT", isKey());
            }
            return json.shortValue();
        }
    }

    static class UUIDPayloadConverter extends DefaultPayloadConverter<UUID> {

        UUIDPayloadConverter() {
            super(new UUIDSerializer());
        }

        @Override
        protected UUID fromJson(JsonNode json) {
            if (!json.isTextual()) {
                throw unexpectedType(json.getNodeType(), "a UUID string", isKey());
            }
            String text = json.asText();
            try {
                return UUID.fromString(text);
            } catch (IllegalArgumentException e) {
                throw unexpectedString(text, "a valid UUID string", isKey());
            }
        }
    }

    static class JsonPayloadConverter extends DefaultPayloadConverter<JsonNode> {

        JsonPayloadConverter() {
            super(new KafkaJsonSerializer<JsonNode>());
        }

        @Override
        protected JsonNode fromJson(JsonNode json) {
            if (!json.isObject()) {
                throw unexpectedType(json.getNodeType(), "a JSON object", isKey());
            }
            return json;
        }
    }

    static class GenericRecordPayloadConverter extends DefaultPayloadConverter<GenericRecord> {

        private static class GenericRecordSerializer implements Serializer<GenericRecord> {

            private final KafkaAvroSerializer kafkaAvroSerializer;

            public GenericRecordSerializer() {
                this.kafkaAvroSerializer = new KafkaAvroSerializer();
            }

            @Override
            public void configure(java.util.Map<String, ?> configs, boolean isKey) {
                this.kafkaAvroSerializer.configure(configs, isKey);
            }

            @Override
            public byte[] serialize(java.lang.String topic, GenericRecord data) {
                if (data == null) {
                    return null;
                }
                try {
                    return this.kafkaAvroSerializer.serialize(topic, data);
                } catch (Exception e) {
                    throw new SerializationException("Failed to serialize GenericRecord", e);
                }
            }
        }

        private GenericDatumReader<GenericRecord> datumReader;
        private Schema schema;

        GenericRecordPayloadConverter(String schemaString) {
            super(new GenericRecordSerializer());
            this.schema = new Schema.Parser().parse(schemaString);
            this.datumReader = new GenericDatumReader<>(this.schema);
            // GenericRecord avroRecord = datumReader.read(null, decoder);
        }

        @Override
        protected GenericRecord fromJson(JsonNode json) {
            try {
                Decoder decoder = DecoderFactory.get().jsonDecoder(this.schema, json.toString());
                return datumReader.read(null, decoder);
            } catch (IOException e) {
                throw new SerializationException("Failed to deserialize JSON to GenericRecord", e);
            }
        }
    }

    static class DynamicMessagePayloadConverter extends DefaultPayloadConverter<DynamicMessage> {

        private static class DynamicMessageSerializer implements Serializer<DynamicMessage> {

            private final KafkaProtobufSerializer<DynamicMessage> kafkaProtobufSerializer;

            public DynamicMessageSerializer() {
                this.kafkaProtobufSerializer = new KafkaProtobufSerializer<DynamicMessage>();
            }

            @Override
            public void configure(java.util.Map<String, ?> configs, boolean isKey) {
                kafkaProtobufSerializer.configure(configs, isKey);
            }

            @Override
            public byte[] serialize(java.lang.String topic, DynamicMessage data) {
                if (data == null) {
                    return null;
                }
                try {
                    return this.kafkaProtobufSerializer.serialize(topic, data);
                } catch (Exception e) {
                    throw new SerializationException("Failed to serialize DynamicMessage", e);
                }
            }
        }

        DynamicMessagePayloadConverter(String schemaString) {
            super(new DynamicMessageSerializer());
            // GenericRecord avroRecord = datumReader.read(null, decoder);
        }

        @Override
        protected DynamicMessage fromJson(JsonNode json) {
            // DynamicMessage.Builder dynamicBuilder = DynamicMessage.newBuilder((Message)null);

            // // The exact same JsonFormat parser works for DynamicMessage builders
            // JsonFormat.parser().ignoringUnknownFields().merge(json.toString(), dynamicBuilder);

            // return dynamicBuilder.build();
            return null;
        }
    }
}
