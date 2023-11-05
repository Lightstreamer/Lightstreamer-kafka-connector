package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

public interface ValueSelectorSupplier<V> {

    ValueSelector<V> selector(String name, String expression);

    void configValue(Map<String, String> configuration, Properties props);

    default String deserializer(boolean isKey, Properties pros) {
        return StringDeserializer.class.getName();
    }

    default  void configValueDeserializer(Map<String, String> conf, Properties props) {
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer(false, props));
    }
}
