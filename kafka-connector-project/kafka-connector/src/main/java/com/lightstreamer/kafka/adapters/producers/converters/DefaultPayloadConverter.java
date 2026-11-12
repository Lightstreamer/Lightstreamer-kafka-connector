
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
import com.lightstreamer.kafka.adapters.pub.messages.converters.PayloadConverter;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

abstract class DefaultPayloadConverter<T> implements PayloadConverter {

    private final Serializer<T> serializer;

    protected DefaultPayloadConverter(Serializer<T> serializer) {
        this.serializer = serializer;
    }

    byte[] doSerialize(java.lang.String topic, T value) {
        return serializer.serialize(topic, value);
    }

    @Override
    public void configure(java.util.Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        serializer.close();
    }

    @Override
    public byte[] serialize(java.lang.String topic, JsonNode data) {
        if (data == null) {
            return null;
        }
        return doSerialize(topic, fromJson(data));
    }

    protected abstract T fromJson(JsonNode node);

    /**
     * Creates an exception for a JSON node whose type does not match the configured evaluator type.
     * The field name is not known at this level and is added by the caller through {@link
     * #invalidField(String, String)}.
     *
     * @param nodeType the offending node type
     * @param expected a short description of the expected type
     * @return a new {@code SerializationException}
     */
    public static SerializationException unexpectedType(JsonNodeType nodeType, String expected) {
        return new SerializationException("expected %s but found %s".formatted(expected, nodeType));
    }
}
