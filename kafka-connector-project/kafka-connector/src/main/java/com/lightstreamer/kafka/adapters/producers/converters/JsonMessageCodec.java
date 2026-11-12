
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
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.PayloadConverterType;
import com.lightstreamer.kafka.adapters.pub.messages.converters.PayloadConverter;

import org.apache.kafka.common.serialization.Serializer;

import java.util.EnumMap;

/**
 * Resolves a pair of {@link EvaluatorType}s into the matching JSON conversion and Kafka
 * serialization components for publishing records.
 *
 * <p>A codec is the single source of truth for a key/value type pair: the {@link
 * JsonMessageConverter} returned by {@link #converter()} and the {@link Serializer}s returned by
 * {@link #payloadConverter()} and {@link #valueSerializer()} are guaranteed to agree on the Java
 * types {@code K} and {@code V}.
 *
 * @see JsonMessageConverter
 * @param <K> the type of the key in the Kafka record
 * @param <V> the type of the value in the Kafka record
 */
public final class JsonMessageCodec {

    private static final EnumMap<PayloadConverterType, PayloadConverter> CONVERTERS =
            new EnumMap<>(PayloadConverterType.class);

    static {
        CONVERTERS.put(
                PayloadConverterType.STRING, new JsonPayloadConverters.StringPayloadConverter());
        CONVERTERS.put(
                PayloadConverterType.INTEGER, new JsonPayloadConverters.IntegerPayloadConverter());
        CONVERTERS.put(
                PayloadConverterType.BOOLEAN, new JsonPayloadConverters.BooleanPayloadConverter());
        // SERIALIZERS.put(EvaluatorType.BYTE_ARRAY, Converters.ByteArray());
        // SERIALIZERS.put(EvaluatorType.BYTE_BUFFER, Converters.ByteBuffer());
        // SERIALIZERS.put(EvaluatorType.BYTES, Converters.Bytes());
        CONVERTERS.put(
                PayloadConverterType.DOUBLE, new JsonPayloadConverters.DoublePayloadConverter());
        CONVERTERS.put(
                PayloadConverterType.FLOAT, new JsonPayloadConverters.FloatPayloadConverter());
        CONVERTERS.put(PayloadConverterType.LONG, new JsonPayloadConverters.LongPayloadConverter());
        CONVERTERS.put(
                PayloadConverterType.SHORT, new JsonPayloadConverters.ShortPayloadConverter());
        CONVERTERS.put(PayloadConverterType.UUID, new JsonPayloadConverters.UUIDPayloadConverter());
        CONVERTERS.put(PayloadConverterType.JSON, new JsonPayloadConverters.JsonPayloadConverter());
    }

    /**
     * Creates a new {@code JsonMessageCodec} for the specified converter type.
     *
     * @param converterType the converter type of the record key
     * @return a new {@code JsonMessageCodec}
     * @throws IllegalArgumentException if either evaluator type is {@code null} or not supported
     *     for publishing
     */
    public static PayloadConverter lookup(PayloadConverterType payloadConverter) {
        PayloadConverter converter = CONVERTERS.get(payloadConverter);
        if (converter == null) {
            throw new IllegalArgumentException(
                    "Unsupported evaluator type for publishing: " + payloadConverter);
        }
        return converter;
    }
}
