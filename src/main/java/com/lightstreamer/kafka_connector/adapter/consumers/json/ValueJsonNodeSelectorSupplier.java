package com.lightstreamer.kafka_connector.adapter.consumers.json;

import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka_connector.adapter.evaluator.AbstractSelectorSupplierWithSchema;
import com.lightstreamer.kafka_connector.adapter.evaluator.ValueSelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.ValueSelectorSupplier;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;

public class ValueJsonNodeSelectorSupplier extends AbstractSelectorSupplierWithSchema<JsonNode> implements ValueSelectorSupplier<JsonNode> {

    public ValueJsonNodeSelectorSupplier() {
    }

    protected Class<?> getLocalSchemaDeserializer() {
        return KafkaJsonDeserializer.class;
    }

    protected Class<?> getSchemaDeserializer() {
        return KafkaJsonSchemaDeserializer.class;
    }


    @Override
    public void configValue(Map<String, String> conf, Properties props) {
        configValueDeserializer(conf, props);
        conf.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, JsonNode.class.getName());
    }


    @Override
    public ValueSelector<JsonNode> selector(String name, String expression) {
        return new ValueJsonNodeSelector(name, expression);
    }
}
