package com.lightstreamer.kafka_connector.adapter.consumers;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.lightstreamer.kafka_connector.adapter.mapping.Schema;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Value;

public class RemappedRecord {

    private final String topic;

    private final Set<Value> valuesSet;

    public RemappedRecord(String topic, Set<Value> values) {
        this.topic = topic;
        this.valuesSet = values;
    }

    public String topic() {
        return topic;
    }

    public Set<Value> values() {
        return valuesSet;
    }

    public Map<String, String> filter(Schema schema) {
        return valuesSet.stream()
                .filter(v -> schema.keys().contains(v.name()))
                .collect(Collectors.toMap(Value::name, Value::text));
    }
}
