package com.lightstreamer.kafka_connector.adapter.mapping.selectors.json;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.AbstractSelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.BaseSelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.KeySelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.SelectorExpressionParser;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.SelectorExpressionParser.LinkedNode;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.SelectorExpressionParser.NodeEvaluator;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Value;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueSelectorSupplier;

import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;

public class JsonNodeSelectorsSuppliers {

    private static final JsonNodeKeySelectorSupplier KEY_SELECTOR_SUPPLIER = new JsonNodeKeySelectorSupplier();

    private static final JsonNodeValueSelectorSupplier VALUE_SELECTOR_SUPPLIER = new JsonNodeValueSelectorSupplier();

    public static KeySelectorSupplier<JsonNode> keySelectorSupplier() {
        return KEY_SELECTOR_SUPPLIER;
    }

    public static ValueSelectorSupplier<JsonNode> valueSelectorSupplier() {
        return VALUE_SELECTOR_SUPPLIER;
    }

    private static abstract class JsonNodeBaseSelector extends BaseSelector {

        private static class PropertyGetter implements NodeEvaluator<JsonNode, JsonNode> {

            private final String name;

            PropertyGetter(String name) {
                this.name = name;
            }

            @Override
            public JsonNode get(JsonNode node) {
                if (!node.has(name)) {
                    throw new RuntimeException("No field <" + name + "> exists!");
                }
                return node.get(name);
            }
        }

        private static class ArrayGetter implements NodeEvaluator<JsonNode, JsonNode> {

            private final String name;

            private final PropertyGetter getter;

            private final List<Integer> indexes;

            ArrayGetter(String fieldName, List<Integer> indexes) {
                this.name = fieldName;
                this.indexes = indexes;
                this.getter = new PropertyGetter(name);
            }

            @Override
            public JsonNode get(JsonNode node) {
                JsonNode value = getter.get(node);
                for (int i : indexes) {
                    if (value.isArray()) {
                        value = value.get(i);
                    } else {
                        throw new RuntimeException("Current evaluated field is not an Array");
                    }
                }
                return value;
            }
        }

        private final LinkedNode<NodeEvaluator<JsonNode, JsonNode>> linkedNode;

        private static final SelectorExpressionParser<JsonNode, JsonNode> PARSER = new SelectorExpressionParser.Builder<JsonNode, JsonNode>()
                .withFieldEvaluator(PropertyGetter::new)
                .withArrayEvaluator(ArrayGetter::new)
                .build();

        protected JsonNodeBaseSelector(String name, String expectedRoot, String expression) {
            super(name, expression);
            this.linkedNode = PARSER.parse(expectedRoot, expression);
        }

        protected Value eval(JsonNode currentJsonNode) {
            LinkedNode<NodeEvaluator<JsonNode, JsonNode>> currentLinkedNode = linkedNode;
            while (currentLinkedNode != null) {
                NodeEvaluator<JsonNode, JsonNode> nodeEvaluator = currentLinkedNode.value();
                currentJsonNode = nodeEvaluator.get(currentJsonNode);
                currentLinkedNode = currentLinkedNode.next();
            }
            return Value.of(name(), currentJsonNode.asText());
        }
    }

    static class JsonNodeKeySelectorSupplier extends AbstractSelectorSupplier<JsonNode>
            implements KeySelectorSupplier<JsonNode> {

        public JsonNodeKeySelectorSupplier() {
        }

        protected Class<?> getLocalSchemaDeserializer() {
            return JsonLocalSchemaDeserializer.class;
        }

        protected Class<?> getSchemaDeserializer() {
            return KafkaJsonSchemaDeserializer.class;
        }

        @Override
        public void configKey(Map<String, String> conf, Properties props) {
            KeySelectorSupplier.super.configKey(conf, props);
            props.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, JsonNode.class.getName());
        }

        @Override
        public KeySelector<JsonNode> selector(String name, String expression) {
            return new JsonNodeKeySelector(name, expectedRoot(), expression);
        }

    }

    static final class JsonNodeKeySelector extends JsonNodeBaseSelector implements KeySelector<JsonNode> {

        protected JsonNodeKeySelector(String name, String expectedRoot, String expression) {
            super(name, expectedRoot, expression);
        }

        @Override
        public Value extract(ConsumerRecord<JsonNode, ?> record) {
            return super.eval(record.key());
        }
    }

    static class JsonNodeValueSelectorSupplier extends AbstractSelectorSupplier<JsonNode>
            implements ValueSelectorSupplier<JsonNode> {

        public JsonNodeValueSelectorSupplier() {
        }

        protected Class<?> getLocalSchemaDeserializer() {
            return JsonLocalSchemaDeserializer.class;
            // return KafkaJsonDeserializer.class;
        }

        protected Class<?> getSchemaDeserializer() {
            return KafkaJsonSchemaDeserializer.class;
        }

        @Override
        public void configValue(Map<String, String> conf, Properties props) {
            ValueSelectorSupplier.super.configValue(conf, props);
            props.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, JsonNode.class.getName());
        }

        @Override
        public String deserializer(Properties props) {
            return super.deserializer(false, props);
        }

        @Override
        public ValueSelector<JsonNode> selector(String name, String expression) {
            return new JsonNodeValueSelector(name, expectedRoot(), expression);
        }
    }

    static final class JsonNodeValueSelector extends JsonNodeBaseSelector implements ValueSelector<JsonNode> {

        protected JsonNodeValueSelector(String name, String expectedRoot, String expression) {
            super(name, expectedRoot, expression);
        }

        @Override
        public Value extract(ConsumerRecord<?, JsonNode> record) {
            return super.eval(record.value());
        }
    }

}
