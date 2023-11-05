package com.lightstreamer.kafka_connector.adapter.consumers.json;

import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka_connector.adapter.evaluator.AbstractSelectorSupplierWithSchema;
import com.lightstreamer.kafka_connector.adapter.evaluator.KeySelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.KeySelectorSupplier;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;

public class KeyJsonNodeSelectorSupplier extends AbstractSelectorSupplierWithSchema<JsonNode> implements KeySelectorSupplier<JsonNode> {

    public KeyJsonNodeSelectorSupplier() {
    }

    protected Class<?> getLocalSchemaDeserializer() {
        return KafkaJsonDeserializer.class;
    }

    protected Class<?> getSchemaDeserializer() {
        return KafkaJsonSchemaDeserializer.class;
    }

    @Override
    public void configKey(Map<String, String> conf, Properties props) {
        configKeyDeserializer(conf, props);
        conf.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, JsonNode.class.getName());
    }

    @Override
    public KeySelector<JsonNode> selector(String name, String expression) {
        return new KeyJsonNodeSelector(name, expression);
    }

}
