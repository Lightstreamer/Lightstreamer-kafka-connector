package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.Properties;

import org.apache.kafka.common.serialization.StringDeserializer;

public interface SelectorSupplier<V> {

    Selector<V> get(String name, String expression);

    default Properties props() {
        return new Properties();
    }

    default String deserializer(boolean isKey) {
        return StringDeserializer.class.getName();
    }

}
