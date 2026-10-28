
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

import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;

import org.apache.kafka.common.serialization.Serializer;

import java.util.EnumMap;

/**
 * Resolves a pair of {@link EvaluatorType}s into the matching JSON conversion and Kafka
 * serialization components for publishing records.
 *
 * <p>A codec is the single source of truth for a key/value type pair: the {@link
 * JsonMessageConverter} returned by {@link #converter()} and the {@link Serializer}s returned by
 * {@link #keySerializer()} and {@link #valueSerializer()} are guaranteed to agree on the Java types
 * {@code K} and {@code V}.
 *
 * @see JsonMessageConverter
 * @param <K> the type of the key in the Kafka record
 * @param <V> the type of the value in the Kafka record
 */
public final class JsonMessageCodec<K, V> {

    private static final EnumMap<EvaluatorType, JsonPayloadConverter<?>> CONVERTERS =
            new EnumMap<>(EvaluatorType.class);

    static {
        CONVERTERS.put(EvaluatorType.STRING, JsonPayloadConverters.String());
        CONVERTERS.put(EvaluatorType.INTEGER, JsonPayloadConverters.Integer());
        CONVERTERS.put(EvaluatorType.BOOLEAN, JsonPayloadConverters.Boolean());
        // SERIALIZERS.put(EvaluatorType.BYTE_ARRAY, Converters.ByteArray());
        // SERIALIZERS.put(EvaluatorType.BYTE_BUFFER, Converters.ByteBuffer());
        // SERIALIZERS.put(EvaluatorType.BYTES, Converters.Bytes());
        CONVERTERS.put(EvaluatorType.DOUBLE, JsonPayloadConverters.Double());
        CONVERTERS.put(EvaluatorType.FLOAT, JsonPayloadConverters.Float());
        CONVERTERS.put(EvaluatorType.LONG, JsonPayloadConverters.Long());
        CONVERTERS.put(EvaluatorType.SHORT, JsonPayloadConverters.Short());
        CONVERTERS.put(EvaluatorType.UUID, JsonPayloadConverters.UUID());
    }

    private final JsonPayloadConverter<K> keyConverter;
    private final JsonPayloadConverter<V> valueConverter;

    private JsonMessageCodec(
            JsonPayloadConverter<K> keyConverter, JsonPayloadConverter<V> valueConverter) {
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
    }

    /**
     * Creates a new {@code JsonMessageCodec} for the specified key and value evaluator types.
     *
     * @param keyEvaluatorType the evaluator type of the record key
     * @param valueEvaluatorType the evaluator type of the record value
     * @return a new {@code JsonMessageCodec}
     * @throws IllegalArgumentException if either evaluator type is {@code null} or not supported
     *     for publishing
     */
    public static JsonMessageCodec<?, ?> of(
            EvaluatorType keyEvaluatorType, EvaluatorType valueEvaluatorType) {
        return new JsonMessageCodec<>(lookup(keyEvaluatorType), lookup(valueEvaluatorType));
    }

    private static JsonPayloadConverter<?> lookup(EvaluatorType evaluatorType) {
        JsonPayloadConverter<?> converter = CONVERTERS.get(evaluatorType);
        if (converter == null) {
            throw new IllegalArgumentException(
                    "Unsupported evaluator type for publishing: " + evaluatorType);
        }
        return converter;
    }

    /**
     * Creates a new {@link JsonMessageConverter} bound to the key and value types of this codec.
     *
     * @return a new {@code JsonMessageConverter}
     */
    public JsonMessageConverter<K, V> converter() {
        return new JsonMessageConverter<>(keyConverter, valueConverter);
    }

    /**
     * Returns the serializer for the record key.
     *
     * @return the key {@link Serializer}
     */
    public Serializer<K> keySerializer() {
        return keyConverter.serializer();
    }

    /**
     * Returns the serializer for the record value.
     *
     * @return the value {@code Serializer}
     */
    public Serializer<V> valueSerializer() {
        return valueConverter.serializer();
    }
}
