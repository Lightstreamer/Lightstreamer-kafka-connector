
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
import com.lightstreamer.kafka.adapters.mapping.selectors.KeyValueSelectorSuppliersMaker;
import com.lightstreamer.kafka.common.mapping.selectors.Data;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelector;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.Node;
import com.lightstreamer.kafka.common.mapping.selectors.StructuredBaseSelector;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelectorSupplier;
import com.lightstreamer.kafka.common.records.KafkaRecord;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Optional;

public class JsonNodeSelectorsSuppliers implements KeyValueSelectorSuppliersMaker<JsonNode> {

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
        public String asText() {
            return node.isValueNode() ? node.asText(null) : node.toString();
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
        public KeySelector<JsonNode> newSelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            return new JsonNodeKeySelector(name, expression);
        }

        @Override
        public Deserializer<JsonNode> deserializer() {
            return deserializer;
        }
    }

    private static final class JsonNodeKeySelector extends StructuredBaseSelector<JsonNodeNode>
            implements KeySelector<JsonNode> {

        JsonNodeKeySelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            super(name, expression, Constant.KEY);
        }

        @Override
        public Data extractKey(KafkaRecord<JsonNode, ?> record, boolean checkScalar)
                throws ValueException {
            return super.eval(() -> record.key(), JsonNodeNode::new, checkScalar);
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
        public ValueSelector<JsonNode> newSelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            return new JsonNodeValueSelector(name, expression);
        }

        @Override
        public Deserializer<JsonNode> deserializer() {
            return deserializer;
        }
    }

    private static final class JsonNodeValueSelector extends StructuredBaseSelector<JsonNodeNode>
            implements ValueSelector<JsonNode> {

        JsonNodeValueSelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            super(name, expression, Constant.VALUE);
        }

        @Override
        public Data extractValue(KafkaRecord<?, JsonNode> record, boolean checkScalar)
                throws ValueException {
            return super.eval(() -> record.value(), JsonNodeNode::new, checkScalar);
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
