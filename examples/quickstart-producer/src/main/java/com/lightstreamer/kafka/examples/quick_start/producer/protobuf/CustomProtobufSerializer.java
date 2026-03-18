
/*
 * Copyright (C) 2025 Lightstreamer Srl
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

package com.lightstreamer.kafka.examples.quick_start.producer.protobuf;

import org.apache.kafka.common.serialization.Serializer;

/**
 * A Kafka {@link Serializer} for Protocol Buffer {@link Stock} messages.
 *
 * <p>This serializer does not require a Schema Registry: it serializes {@link Stock} messages
 * directly to their raw binary wire format using Protocol Buffers' built-in {@link
 * com.google.protobuf.MessageLite#toByteArray()} method. This makes it a lightweight alternative to
 * {@code io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer} for environments where a
 * Schema Registry is not available or not desired.
 *
 * <p>A {@code null} value is serialized as {@code null}, consistent with the Kafka serializer
 * contract.
 *
 * @see Stock
 */
public class CustomProtobufSerializer implements Serializer<Stock> {

    @Override
    public byte[] serialize(String topic, Stock data) {
        if (data == null) {
            return null;
        }
        return data.toByteArray();
    }
}
