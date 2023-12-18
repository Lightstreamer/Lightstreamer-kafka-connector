package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;

public interface ValueSelectorSupplier<V> {

    ValueSelector<V> selector(String name, String expression);

    default void configValue(Map<String, String> configuration, Properties props) {
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer(props));
        props.put("adapter.dir", configuration.get("adapter.dir"));
    }

    String deserializer(Properties pros);
}
