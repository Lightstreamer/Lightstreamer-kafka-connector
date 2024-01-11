package com.lightstreamer.kafka_connector.adapter.mapping;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lightstreamer.kafka_connector.adapter.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Schema;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Value;

public interface RecordMapper<K, V> {

    interface MappedRecord {

        String topic();

        Map<String, String> filter(Schema schema);
    }

    MappedRecord map(ConsumerRecord<K, V> record);

    static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    static class Builder<K, V> {

        final List<Selectors<K, V>> allSelectors = new ArrayList<>();

        private Builder() {
        }

        public Builder<K, V> withSelectors(Stream<Selectors<K, V>> selector) {
            allSelectors.addAll(selector.toList());
            return this;
        }

        public final Builder<K, V> withSelectors(Selectors<K, V> selector) {
            allSelectors.add(selector);
            return this;
        }

        public RecordMapper<K, V> build() {
            return new DefaultRecordMapper<>(this);
        }

    }
}

class DefaultRecordMapper<K, V> implements RecordMapper<K, V> {

    protected static Logger log = LoggerFactory.getLogger(DefaultRecordMapper.class);

    private final List<Selectors<K, V>> selectors;

    DefaultRecordMapper(Builder<K, V> builder) {
        this.selectors = Collections.unmodifiableList(builder.allSelectors);
    }

    @Override
    public MappedRecord map(ConsumerRecord<K, V> record) {
        Set<Value> values = selectors.stream()
                .flatMap(s -> s.extractValues(record).stream())
                .collect(Collectors.toSet());
        return new DefaultMappedRecord(record.topic(), values);
    }

}

class DefaultMappedRecord implements MappedRecord {

    private final String topic;

    private final Set<Value> valuesSet;

    DefaultMappedRecord(String topic, Set<Value> values) {
        this.topic = topic;
        this.valuesSet = values;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public Map<String, String> filter(Schema schema) {
        return valuesSet.stream()
                .filter(value -> schema.keys().contains(value.name()) &&
                        schema.name().equals(value.schemaName()))
                .collect(Collectors.toMap(Value::name, Value::text));
    }
}
