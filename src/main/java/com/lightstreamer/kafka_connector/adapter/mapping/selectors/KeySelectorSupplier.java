package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.lightstreamer.kafka_connector.adapter.config.ConnectorConfig;

public interface KeySelectorSupplier<V> extends SelectorSupplier<KeySelector<V>> {

    KeySelector<V> newSelector(String name, String expression);

    @Override
    default void config(ConnectorConfig config) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer(config));
    }

    default String expectedRoot() {
        return "KEY";
    }

}
