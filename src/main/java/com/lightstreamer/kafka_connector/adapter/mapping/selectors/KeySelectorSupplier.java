package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

public interface KeySelectorSupplier<V> extends SelectorSupplier<KeySelector<V>> {

    KeySelector<V> newSelector(String name, String expression);

    @Override
    default void config(Map<String, String> configuration, Properties props) {
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer(props));
    }

    default String deserializer(Properties pros) {
        return StringDeserializer.class.getName();
    }

    default String expectedRoot() {
        return "KEY";
    }

}
