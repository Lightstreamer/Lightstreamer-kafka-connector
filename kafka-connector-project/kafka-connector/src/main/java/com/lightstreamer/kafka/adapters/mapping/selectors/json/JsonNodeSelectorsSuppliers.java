
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
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.VALUE;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka.common.mapping.selectors.Data;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelector;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka.common.mapping.selectors.KeyValueSelectorSuppliersMaker;
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.Node;
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.Node.NullNode;
import com.lightstreamer.kafka.common.mapping.selectors.SelectorEvaluatorType;
import com.lightstreamer.kafka.common.mapping.selectors.StructuredBaseSelector;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelectorSupplier;
import com.lightstreamer.kafka.common.records.KafkaRecord;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

public class JsonNodeSelectorsSuppliers implements KeyValueSelectorSuppliersMaker<JsonNode> {

    interface JsonNodeNode extends Node<JsonNodeNode> {

        static JsonNodeNode newNode(String name, JsonNode node) {
            if (node == null) {
                return new JsonNullNode(name);
            }
            return switch (node.getNodeType()) {
                case OBJECT -> new JsonObjectNode(name, (ObjectNode) node);
                case ARRAY -> new JsonArrayNode(name, (ArrayNode) node);
                case NULL -> new JsonNullNode(name);

                default -> new JsonScalarNode(name, node.asText());
            };
        }
    }

    static class JsonNullNode extends NullNode<JsonNodeNode> implements JsonNodeNode {

        JsonNullNode(String name) {
            super(name);
        }
    }

    static class JsonObjectNode implements JsonNodeNode {

        private final String name;
        private final JsonNode objectNode;

        JsonObjectNode(String name, ObjectNode objectNode) {
            this.name = name;
            this.objectNode = objectNode;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean has(String propertyname) {
            return objectNode.has(propertyname);
        }

        @Override
        public JsonNodeNode getProperty(String nodeName, String propertyName) {
            JsonNode node = objectNode.get(propertyName);
            if (node != null) {
                return JsonNodeNode.newNode(nodeName, node);
            }
            throw ValueException.fieldNotFound(propertyName);
        }

        @Override
        public JsonNodeNode getIndexed(String nodeName, int index, String indexedPropertyName) {
            throw ValueException.nonArrayObject(index);
        }

        @Override
        public boolean isArray() {
            return objectNode.isArray();
        }

        @Override
        public int size() {
            return objectNode.size();
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public boolean isScalar() {
            return false;
        }

        @Override
        public String text() {
            return objectNode.toString();
        }

        @Override
        public void flatIntoMap(Map<String, String> target) {
            Iterator<String> fieldNames = objectNode.fieldNames();
            while (fieldNames.hasNext()) {
                String key = fieldNames.next();
                JsonNode valueNode = objectNode.get(key);
                String value =
                        valueNode.isValueNode() ? valueNode.asText(null) : valueNode.toString();
                target.put(key, value);
            }
        }
    }

    static class JsonScalarNode implements JsonNodeNode {

        private final String name;
        private final String value;

        JsonScalarNode(String name, String value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean has(String propertyname) {
            return false;
        }

        @Override
        public JsonNodeNode getProperty(String nodeName, String propertyName) {
            throw ValueException.scalarObject(propertyName);
        }

        @Override
        public boolean isArray() {
            return false;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public JsonNodeNode getIndexed(String nodeName, int index, String indexedPropertyName) {
            throw ValueException.noIndexedField(indexedPropertyName);
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public boolean isScalar() {
            return true;
        }

        @Override
        public String text() {
            return value;
        }

        @Override
        public void flatIntoMap(Map<String, String> target) {}
    }

    static class JsonArrayNode implements JsonNodeNode {

        private final String name;
        private final ArrayNode arrayNode;
        private final int size;

        JsonArrayNode(String name, ArrayNode node) {
            this.name = name;
            this.arrayNode = node;
            this.size = node.size();
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean has(String propertyname) {
            return false;
        }

        @Override
        public JsonNodeNode getProperty(String nodeName, String propertyName) {
            throw ValueException.arrayObject(propertyName);
        }

        @Override
        public boolean isArray() {
            return true;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public JsonNodeNode getIndexed(String nodeName, int index, String indexedPropertyName) {
            if (index < size) {
                return JsonNodeNode.newNode(nodeName, arrayNode.get(index));
            }
            throw ValueException.indexOfOutBounds(index);
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public boolean isScalar() {
            return false;
        }

        @Override
        public String text() {
            return arrayNode.toString();
        }

        @Override
        public void flatIntoMap(Map<String, String> target) {
            // Pre-allocate StringBuilder for array index keys to avoid string concatenation
            StringBuilder keyBuilder =
                    new StringBuilder(name.length() + 4); // reasonable initial capacity
            for (int i = 0; i < size; i++) {
                JsonNode element = arrayNode.get(i);
                String value = element.isValueNode() ? element.asText(null) : element.toString();

                keyBuilder.append(name);
                keyBuilder.append('[').append(i).append(']');
                target.put(keyBuilder.toString(), value);
                keyBuilder.delete(0, keyBuilder.length()); // reset for next use
            }
            return;
        }
    }

    private static class JsonNodeKeySelectorSupplier implements KeySelectorSupplier<JsonNode> {

        private final Deserializer<JsonNode> deserializer;

        JsonNodeKeySelectorSupplier(ConnectorConfig config) {
            this.deserializer = JsonNodeDeserializers.KeyDeserializer(config);
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
            super(expression, KEY, JsonNodeNode::newNode);
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
            super(expression, VALUE, JsonNodeNode::newNode);
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

    private final ConnectorConfig config;

    public JsonNodeSelectorsSuppliers(ConnectorConfig config) {
        this.config = config;
    }

    @Override
    public KeySelectorSupplier<JsonNode> makeKeySelectorSupplier() {
        return new JsonNodeKeySelectorSupplier(config);
    }

    @Override
    public ValueSelectorSupplier<JsonNode> makeValueSelectorSupplier() {
        return new JsonNodeValueSelectorSupplier(config);
    }
}
