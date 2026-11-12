
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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DynamicMessage;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
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
import java.util.UUID;

class JsonPayloadConverters {

    static class StringPayloadConverter extends DefaultPayloadConverter<String> {

        StringPayloadConverter() {
            super(new StringSerializer());
        }

        @Override
        protected String fromJson(JsonNode json) {
            if (!json.isTextual()) {
                throw unexpectedType(json.getNodeType(), "STRING");
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
                throw unexpectedType(json.getNodeType(), "INTEGER");
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
                throw unexpectedType(json.getNodeType(), "BOOLEAN");
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
                throw unexpectedType(json.getNodeType(), "a number");
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
                throw unexpectedType(json.getNodeType(), "a number");
            }
            return json.floatValue();
        }
    }

    static class LongPayloadConverter extends DefaultPayloadConverter<Long> {

        LongPayloadConverter() {
            super(new LongSerializer());
        }

        @Override
        protected Long fromJson(JsonNode json) {
            if (!json.isIntegralNumber() || !json.canConvertToLong()) {
                throw unexpectedType(json.getNodeType(), "LONG");
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
                throw unexpectedType(json.getNodeType(), "SHORT");
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
                throw unexpectedType(json.getNodeType(), "a UUID string");
            }
            String text = json.asText();
            try {
                return UUID.fromString(text);
            } catch (IllegalArgumentException e) {
                throw new SerializationException(
                        "expected a valid UUID but found \"%s\"".formatted(text));
            }
        }
    }

    static class JsonPayloadConverter extends DefaultPayloadConverter<JsonNode> {

        private static class JsonNodeSerializer implements Serializer<JsonNode> {

            private final ObjectMapper objectMapper = new ObjectMapper();

            @Override
            public void configure(java.util.Map<String, ?> configs, boolean isKey) {
                // No configuration needed
            }

            @Override
            public byte[] serialize(java.lang.String topic, JsonNode data) {
                if (data == null) {
                    return null;
                }

                try {
                    return objectMapper.writeValueAsBytes(data);
                } catch (Exception e) {
                    // Should not happen, but wrap any exception as SerializationException
                    throw new SerializationException("Failed to serialize JsonNode", e);
                }
            }
        }

        JsonPayloadConverter() {
            super(new JsonNodeSerializer());
        }

        @Override
        protected JsonNode fromJson(JsonNode json) {
            if (!json.isObject()) {
                throw unexpectedType(json.getNodeType(), "a JSON object");
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
