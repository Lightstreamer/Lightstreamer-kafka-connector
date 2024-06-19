
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
import com.lightstreamer.kafka.mapping.selectors.BaseSelector;
import com.lightstreamer.kafka.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.mapping.selectors.KeySelector;
import com.lightstreamer.kafka.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka.mapping.selectors.SelectorExpressionParser;
import com.lightstreamer.kafka.mapping.selectors.SelectorExpressionParser.GeneralizedKey;
import com.lightstreamer.kafka.mapping.selectors.SelectorExpressionParser.LinkedNode;
import com.lightstreamer.kafka.mapping.selectors.SelectorExpressionParser.NodeEvaluator;
import com.lightstreamer.kafka.mapping.selectors.Value;
import com.lightstreamer.kafka.mapping.selectors.ValueException;
import com.lightstreamer.kafka.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka.mapping.selectors.ValueSelectorSupplier;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.List;
import java.util.Objects;

public class JsonNodeSelectorsSuppliers {

    public static KeySelectorSupplier<JsonNode> keySelectorSupplier(ConnectorConfig config) {
        return new JsonNodeKeySelectorSupplier(config);
    }

    public static ValueSelectorSupplier<JsonNode> valueSelectorSupplier(ConnectorConfig config) {
        return new JsonNodeValueSelectorSupplier(config);
    }

    abstract static class JsonNodeBaseSelector extends BaseSelector {

        private static class PropertyGetter implements NodeEvaluator<JsonNode, JsonNode> {

            private final String name;

            PropertyGetter(String name) {
                this.name = name;
            }

            @Override
            public String name() {
                return name;
            }

            @Override
            public JsonNode get(JsonNode node) {
                return get(name, node);
            }

            public static JsonNode get(String name, JsonNode node) {
                if (!node.has(name)) {
                    ValueException.throwFieldNotFound(name);
                }
                return node.get(name);
            }
        }

        private static class ArrayGetter implements NodeEvaluator<JsonNode, JsonNode> {

            private final String name;

            private final PropertyGetter getter;

            private final List<GeneralizedKey> indexes;

            ArrayGetter(String fieldName, List<GeneralizedKey> indexes) {
                this.name = Objects.requireNonNull(fieldName);
                this.indexes = Objects.requireNonNull(indexes);
                this.getter = new PropertyGetter(name);
            }

            @Override
            public String name() {
                return name;
            }

            static JsonNode get(int index, JsonNode node) {
                if (node.isArray()) {
                    if (index < node.size()) {
                        return node.get(index);
                    } else {
                        ValueException.throwIndexOfOutBoundex(index);
                        // Actually unreachable code
                        return null;
                    }
                } else {
                    ValueException.throwNoIndexedField();
                    // Actually unreachable code
                    return null;
                }
            }

            @Override
            public JsonNode get(JsonNode node) {
                JsonNode value = getter.get(node);
                for (GeneralizedKey i : indexes) {
                    if (i.isIndex()) {
                        value = get(i.index(), value);
                    } else {
                        value = PropertyGetter.get(i.key().toString(), value);
                    }
                }
                return value;
            }
        }

        private final LinkedNode<NodeEvaluator<JsonNode, JsonNode>> rootNode;

        private static final SelectorExpressionParser<JsonNode, JsonNode> PARSER =
                new SelectorExpressionParser.Builder<JsonNode, JsonNode>()
                        .withFieldEvaluator(PropertyGetter::new)
                        .withGenericIndexedEvaluator(ArrayGetter::new)
                        .build();

        private JsonNodeBaseSelector(String name, String expression, String expectedRoot) {
            super(name, expression);
            this.rootNode = PARSER.parse(name, expression, expectedRoot);
        }

        Value eval(JsonNode node) {
            LinkedNode<NodeEvaluator<JsonNode, JsonNode>> currentLinkedNode = rootNode;
            while (currentLinkedNode != null) {
                NodeEvaluator<JsonNode, JsonNode> nodeEvaluator = currentLinkedNode.value();
                node = nodeEvaluator.get(node);
                currentLinkedNode = currentLinkedNode.next();
            }
            if (node.isContainerNode()) {
                ValueException.throwNonComplexObjectRequired(expression());
            }

            String text = !node.isNull() ? node.asText() : null;
            return Value.of(name(), text);
        }
    }

    static class JsonNodeKeySelectorSupplier implements KeySelectorSupplier<JsonNode> {

        private final JsonNodeDeserializer deseralizer;

        JsonNodeKeySelectorSupplier(ConnectorConfig config) {
            this.deseralizer = new JsonNodeDeserializer(config, true);
        }

        @Override
        public KeySelector<JsonNode> newSelector(String name, String expression) {
            return new JsonNodeKeySelector(name, expression, expectedRoot());
        }

        @Override
        public Deserializer<JsonNode> deseralizer() {
            return deseralizer;
        }
    }

    static final class JsonNodeKeySelector extends JsonNodeBaseSelector
            implements KeySelector<JsonNode> {

        JsonNodeKeySelector(String name, String expression, String expectedRoot) {
            super(name, expression, expectedRoot);
        }

        @Override
        public Value extract(KafkaRecord<JsonNode, ?> record) {
            return super.eval(record.key());
        }
    }

    static class JsonNodeValueSelectorSupplier implements ValueSelectorSupplier<JsonNode> {

        private final JsonNodeDeserializer deseralizer;

        JsonNodeValueSelectorSupplier(ConnectorConfig config) {
            this.deseralizer = new JsonNodeDeserializer(config, false);
        }

        @Override
        public ValueSelector<JsonNode> newSelector(String name, String expression) {
            return new JsonNodeValueSelector(name, expression, expectedRoot());
        }

        @Override
        public Deserializer<JsonNode> deseralizer() {
            return deseralizer;
        }
    }

    static final class JsonNodeValueSelector extends JsonNodeBaseSelector
            implements ValueSelector<JsonNode> {

        protected JsonNodeValueSelector(String name, String expression, String expectedRoot) {
            super(name, expression, expectedRoot);
        }

        @Override
        public Value extract(KafkaRecord<?, JsonNode> record) {
            return super.eval(record.value());
        }
    }
}
