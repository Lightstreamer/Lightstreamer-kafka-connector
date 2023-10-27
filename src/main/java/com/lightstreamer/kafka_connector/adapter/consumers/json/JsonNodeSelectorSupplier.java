package com.lightstreamer.kafka_connector.adapter.consumers.json;

import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka_connector.adapter.evaluator.AbstractSelectorSupplierWithSchema;
import com.lightstreamer.kafka_connector.adapter.evaluator.Selector;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;

public class JsonNodeSelectorSupplier extends AbstractSelectorSupplierWithSchema<JsonNode> {

    public JsonNodeSelectorSupplier(Map<String, String> conf, boolean isKey) {
        if (isKey) {
            configKeyDeserializer(conf);
        } else {
            configValueDeserializer(conf);
        }
        set(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, JsonNode.class.getName());
    }

    protected Class<?> getLocalSchemaDeserializer() {
        return KafkaJsonDeserializer.class;
    }

    protected Class<?> getSchemaDeserializer() {
        return KafkaJsonSchemaDeserializer.class;
    }

    @Override
    public Selector<JsonNode> get(String name, String expression) {
        return new JsonNodeSelector(name, expression);
    }
}
