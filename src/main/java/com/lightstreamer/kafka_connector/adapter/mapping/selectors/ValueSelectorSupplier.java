package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

public interface ValueSelectorSupplier<V> extends SelectorSupplier<ValueSelector<V>> {

    ValueSelector<V> newSelector(String name, String expression);

    @Override
    default void config(Map<String, String> configuration, Properties props) {
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer(props));
        props.put("adapter.dir", configuration.get("adapter.dir"));
    }

    default String deserializer(Properties pros) {
        return StringDeserializer.class.getName();
    }

    default String expectedRoot() {
        return "VALUE";
    }

}
