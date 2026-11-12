
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

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Properties;

public interface ProducerFactory {

    Producer<JsonNode, JsonNode> createProducer(
            Properties props,
            Serializer<JsonNode> keySerializer,
            Serializer<JsonNode> valueSerializer);

    static ProducerFactory defaultFactory() {
        return new ProducerFactory() {
            @Override
            public Producer<JsonNode, JsonNode> createProducer(
                    Properties props,
                    Serializer<JsonNode> keySerializer,
                    Serializer<JsonNode> valueSerializer) {
                return new org.apache.kafka.clients.producer.KafkaProducer<>(
                        props, keySerializer, valueSerializer);
            }
        };
    }
}
