
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

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Converts a JSON message into a Kafka {@link ProducerRecord}.
 *
 * <p>The JSON message is expected to contain a {@code topic} field, a {@code key} field and a
 * {@code value} field, which are converted according to the evaluator types the converter was
 * created with. A missing or JSON {@code null} key or value is converted to {@code null}.
 *
 * @param <K> the type of the key in the Kafka record
 * @param <V> the type of the value in the Kafka record
 */
public class JsonMessageConverter<K, V> {

    private final JsonPayloadConverter<K> keyConverter;
    private final JsonPayloadConverter<V> valueConverter;
    private final ObjectMapper mapper;

    JsonMessageConverter(
            JsonPayloadConverter<K> keyConverter, JsonPayloadConverter<V> valueConverter) {
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        mapper = new ObjectMapper();
    }

    /**
     * Creates a {@link ProducerRecord} from the given JSON message.
     *
     * @param jsonMessage the JSON message to convert
     * @return a new {@code ProducerRecord} built from the {@code topic}, {@code key} and {@code
     *     value} fields of the message
     * @throws JacksonException if the message is not valid JSON
     * @throws InvalidMessageException if the topic is missing or blank, if the key or value is
     *     present but of the wrong type, or if both key and value are missing
     */
    public ProducerRecord<K, V> fromJson(String jsonMessage)
            throws JacksonException, InvalidMessageException {
        JsonNode json = mapper.readTree(jsonMessage);
        JsonNode topicNode = json.path("topic");
        if (!topicNode.isTextual() || topicNode.textValue().isBlank()) {
            throw new InvalidMessageException(
                    "Invalid \"topic\" field: expected a non-blank string");
        }
        K key = extract(json, "key", keyConverter);
        V value = extract(json, "value", valueConverter);
        if (key == null && value == null) {
            throw new InvalidMessageException("Both key and value are missing in JSON message");
        }
        return new ProducerRecord<>(topicNode.textValue(), key, value);
    }

    private static <T> T extract(
            JsonNode jsonMessage, String fieldName, JsonPayloadConverter<T> converter)
            throws InvalidMessageException {
        JsonNode node = jsonMessage.path(fieldName);
        if (node.isMissingNode() || node.isNull()) {
            return null;
        }
        try {
            return converter.fromJson(node);
        } catch (InvalidMessageException e) {
            throw new InvalidMessageException(
                    "Invalid \"%s\" field: %s".formatted(fieldName, e.getMessage()));
        }
    }
}
