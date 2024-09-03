
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

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.common.expressions.Constant;
import com.lightstreamer.kafka.common.expressions.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.Data;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelector;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.Node;
import com.lightstreamer.kafka.common.mapping.selectors.StructuredBaseSelector;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelectorSupplier;

import org.apache.kafka.common.serialization.Deserializer;

public class JsonNodeSelectorsSuppliers {

    private static class JsonNodeNode implements Node<JsonNodeNode> {

        private final JsonNode node;

        JsonNodeNode(JsonNode node) {
            this.node = node;
        }

        @Override
        public boolean has(String propertyname) {
            return node.has(propertyname);
        }

        @Override
        public JsonNodeNode get(String propertyName) {
            return new JsonNodeNode(node.get(propertyName));
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
        public JsonNodeNode get(int index) {
            return new JsonNodeNode(node.get(index));
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
        public String asText(String defaultStr) {
            return node.asText(defaultStr);
        }
    }

    private static class JsonNodeKeySelectorSupplier implements KeySelectorSupplier<JsonNode> {

        private final JsonNodeDeserializer deseralizer;

        JsonNodeKeySelectorSupplier(ConnectorConfig config) {
            this.deseralizer = new JsonNodeDeserializer(config, true);
        }

        @Override
        public KeySelector<JsonNode> newSelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            return new JsonNodeKeySelector(name, expression);
        }

        @Override
        public Deserializer<JsonNode> deseralizer() {
            return deseralizer;
        }
    }

    private static final class JsonNodeKeySelector extends StructuredBaseSelector<JsonNodeNode>
            implements KeySelector<JsonNode> {

        JsonNodeKeySelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            super(name, expression, Constant.KEY);
        }

        @Override
        public Data extractKey(KafkaRecord<JsonNode, ?> record) {
            JsonNodeNode node = new JsonNodeNode(record.key());
            return super.eval(node);
        }
    }

    private static class JsonNodeValueSelectorSupplier implements ValueSelectorSupplier<JsonNode> {

        private final JsonNodeDeserializer deseralizer;

        JsonNodeValueSelectorSupplier(ConnectorConfig config) {
            this.deseralizer = new JsonNodeDeserializer(config, false);
        }

        @Override
        public ValueSelector<JsonNode> newSelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            return new JsonNodeValueSelector(name, expression);
        }

        @Override
        public Deserializer<JsonNode> deseralizer() {
            return deseralizer;
        }
    }

    private static final class JsonNodeValueSelector extends StructuredBaseSelector<JsonNodeNode>
            implements ValueSelector<JsonNode> {

        JsonNodeValueSelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            super(name, expression, Constant.VALUE);
        }

        @Override
        public Data extractValue(KafkaRecord<?, JsonNode> record) {
            JsonNodeNode node = new JsonNodeNode(record.value());
            return super.eval(node);
        }
    }

    public static KeySelectorSupplier<JsonNode> keySelectorSupplier(ConnectorConfig config) {
        return new JsonNodeKeySelectorSupplier(config);
    }

    public static ValueSelectorSupplier<JsonNode> valueSelectorSupplier(ConnectorConfig config) {
        return new JsonNodeValueSelectorSupplier(config);
    }

    private JsonNodeSelectorsSuppliers() {}
}
