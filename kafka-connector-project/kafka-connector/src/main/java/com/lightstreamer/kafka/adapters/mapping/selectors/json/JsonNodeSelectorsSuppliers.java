
/*
 * Copyright (C) 2024 Lightstreamer Srl
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

package com.lightstreamer.kafka.adapters.mapping.selectors.json;

import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.KEY;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka.adapters.mapping.selectors.KeyValueSelectorSuppliersMaker;
import com.lightstreamer.kafka.common.mapping.selectors.Data;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelector;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.Node;
import com.lightstreamer.kafka.common.mapping.selectors.SelectorEvaluatorType;
import com.lightstreamer.kafka.common.mapping.selectors.StructuredBaseSelector;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelectorSupplier;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

public class JsonNodeSelectorsSuppliers implements KeyValueSelectorSuppliersMaker<JsonNode> {

    static class JsonNodeNode implements Node<JsonNodeNode> {

        private final String name;
        private final JsonNode node;

        static BiFunction<String, JsonNode, JsonNodeNode> rootFactory(String boundParameter) {
            return (name, node) -> new JsonNodeNode(name, node);
        }

        JsonNodeNode(String name, JsonNode node) {
            this.name = name;
            this.node = node;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean has(String propertyname) {
            return node.has(propertyname);
        }

        @Override
        public JsonNodeNode get(String nodeName, String propertyName) {
            return new JsonNodeNode(nodeName, node.get(propertyName));
        }

        @Override
        public boolean isArray() {
            return node.isArray();
        }

        @Override
        public int size() {
            return node.size();
        }

        @Override
        public JsonNodeNode get(String nodeName, int index) {
            return new JsonNodeNode(nodeName, node.get(index));
        }

        @Override
        public boolean isNull() {
            return node.isNull();
        }

        @Override
        public boolean isScalar() {
            return node.isValueNode();
        }

        @Override
        public String text() {
            return node.isValueNode() ? node.asText(null) : node.toString();
        }

        @Override
        public void flatIntoMap(Map<String, String> target) {
            if (isScalar()) {
                return;
            }

            if (isArray()) {
                int size = size();
                // Pre-allocate StringBuilder for array index keys to avoid string concatenation
                StringBuilder keyBuilder =
                        new StringBuilder(name.length() + 4); // reasonable initial capacity
                for (int i = 0; i < size; i++) {
                    JsonNode element = node.get(i);
                    String value =
                            element.isValueNode() ? element.asText(null) : element.toString();

                    keyBuilder.append(name);
                    keyBuilder.append('[').append(i).append(']');
                    target.put(keyBuilder.toString(), value);
                    keyBuilder.delete(0, keyBuilder.length()); // reset for next use
                }
                return;
            }

            // Here we have an object node
            Iterator<String> fieldNames = node.fieldNames();
            while (fieldNames.hasNext()) {
                String key = fieldNames.next();
                JsonNode valueNode = node.get(key);
                String value =
                        valueNode.isValueNode() ? valueNode.asText(null) : valueNode.toString();
                target.put(key, value);
            }
        }
    }

    private static class JsonNodeKeySelectorSupplier implements KeySelectorSupplier<JsonNode> {

        private final Deserializer<JsonNode> deserializer;

        JsonNodeKeySelectorSupplier(ConnectorConfig config) {
            this.deserializer = JsonNodeDeserializers.KeyDeserializer(config);
        }

        JsonNodeKeySelectorSupplier() {
            this.deserializer = JsonNodeDeserializers.KeyDeserializer();
        }

        @Override
        public KeySelector<JsonNode> newSelector(ExtractionExpression expression)
                throws ExtractionException {
            return new JsonNodeKeySelector(expression);
        }

        @Override
        public Deserializer<JsonNode> deserializer() {
            return deserializer;
        }

        @Override
        public SelectorEvaluatorType evaluatorType() {
            return EvaluatorType.JSON;
        }
    }

    private static class JsonNodeKeySelector extends StructuredBaseSelector<JsonNode, JsonNodeNode>
            implements KeySelector<JsonNode> {

        JsonNodeKeySelector(ExtractionExpression expression) throws ExtractionException {
            super(expression, KEY, JsonNodeNode::new);
        }

        @Override
        public Data extractKey(String name, KafkaRecord<JsonNode, ?> record, boolean checkScalar)
                throws ValueException {
            return eval(name, record::key, checkScalar);
        }

        @Override
        public Data extractKey(KafkaRecord<JsonNode, ?> record, boolean checkScalar)
                throws ValueException {
            return eval(record::key, checkScalar);
        }

        @Override
        public void extractKeyInto(KafkaRecord<JsonNode, ?> record, Map<String, String> target)
                throws ValueException {
            evalInto(record::key, target);
        }
    }

    private static class JsonNodeValueSelectorSupplier implements ValueSelectorSupplier<JsonNode> {

        private final Deserializer<JsonNode> deserializer;

        JsonNodeValueSelectorSupplier(ConnectorConfig config) {
            this.deserializer = JsonNodeDeserializers.ValueDeserializer(config);
        }

        JsonNodeValueSelectorSupplier() {
            this.deserializer = JsonNodeDeserializers.ValueDeserializer();
        }

        @Override
        public ValueSelector<JsonNode> newSelector(ExtractionExpression expression)
                throws ExtractionException {
            return new JsonNodeValueSelector(expression);
        }

        @Override
        public Deserializer<JsonNode> deserializer() {
            return deserializer;
        }

        @Override
        public SelectorEvaluatorType evaluatorType() {
            return EvaluatorType.JSON;
        }
    }

    private static final class JsonNodeValueSelector
            extends StructuredBaseSelector<JsonNode, JsonNodeNode>
            implements ValueSelector<JsonNode> {

        JsonNodeValueSelector(ExtractionExpression expression) throws ExtractionException {
            super(expression, Constant.VALUE, JsonNodeNode::new);
        }

        @Override
        public Data extractValue(String name, KafkaRecord<?, JsonNode> record, boolean checkScalar)
                throws ValueException {
            return eval(name, record::value, checkScalar);
        }

        @Override
        public Data extractValue(KafkaRecord<?, JsonNode> record, boolean checkScalar)
                throws ValueException {
            return eval(record::value, checkScalar);
        }

        @Override
        public void extractValueInto(KafkaRecord<?, JsonNode> record, Map<String, String> target)
                throws ValueException {
            evalInto(record::value, target);
        }
    }

    private final Optional<ConnectorConfig> config;

    public JsonNodeSelectorsSuppliers(ConnectorConfig config) {
        this.config = Optional.of(config);
    }

    public JsonNodeSelectorsSuppliers() {
        this.config = Optional.empty();
    }

    @Override
    public KeySelectorSupplier<JsonNode> makeKeySelectorSupplier() {
        return config.map(JsonNodeKeySelectorSupplier::new)
                .orElseGet(() -> new JsonNodeKeySelectorSupplier());
    }

    @Override
    public ValueSelectorSupplier<JsonNode> makeValueSelectorSupplier() {
        return config.map(JsonNodeValueSelectorSupplier::new)
                .orElseGet(() -> new JsonNodeValueSelectorSupplier());
    }
}
