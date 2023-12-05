package com.lightstreamer.kafka_connector.adapter.evaluator.selectors.json;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.AbstractSelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.KeySelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.Value;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;

public class JsonNodeKeySelectorSupplier extends AbstractSelectorSupplier<JsonNode>
        implements KeySelectorSupplier<JsonNode> {

    static final class JsonNodeKeySelector extends JsonNodeBaseSelector implements KeySelector<JsonNode> {

        protected JsonNodeKeySelector(String name, String expression) {
            super(name, expression);
        }

        @Override
        public Value extract(ConsumerRecord<JsonNode, ?> record) {
            return super.eval(record.key());
        }
    }

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
        return new JsonNodeKeySelector(name, expression);
    }

}
