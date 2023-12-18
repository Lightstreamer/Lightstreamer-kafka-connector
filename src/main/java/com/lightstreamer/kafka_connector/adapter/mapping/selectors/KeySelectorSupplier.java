package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

public interface KeySelectorSupplier<V> {

    KeySelector<V> selector(String name, String expression);

    default void configKey(Map<String, String> configuration, Properties props) {
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer(true, props));
    }

    default String deserializer(boolean isKey, Properties pros) {
        return StringDeserializer.class.getName();
    }
}
