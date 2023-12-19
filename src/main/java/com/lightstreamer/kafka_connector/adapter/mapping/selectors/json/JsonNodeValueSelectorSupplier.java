package com.lightstreamer.kafka_connector.adapter.mapping.selectors.json;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.AbstractSelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Value;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueSelectorSupplier;

import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;

public class JsonNodeValueSelectorSupplier extends AbstractSelectorSupplier<JsonNode>
        implements ValueSelectorSupplier<JsonNode> {

    static final class JsonNodeValueSelector extends JsonNodeBaseSelector implements ValueSelector<JsonNode> {

        protected JsonNodeValueSelector(String name, String expectedRoot, String expression) {
            super(name, expectedRoot, expression);
        }

        @Override
        public Value extract(ConsumerRecord<?, JsonNode> record) {
            return super.eval(record.value());
        }
    }

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
    public ValueSelector<JsonNode> selector(String name, String expression) {
        return new JsonNodeValueSelector(name, expectedRoot(), expression);
    }

    @Override
    public String deserializer(Properties props) {
        return super.deserializer(false, props);
    }
}
