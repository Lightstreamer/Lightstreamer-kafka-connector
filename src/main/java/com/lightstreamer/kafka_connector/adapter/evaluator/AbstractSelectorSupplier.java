package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;

public abstract class AbstractSelectorSupplier<S> implements SelectorSupplier<S> {

    protected final Properties props = new Properties();

    protected void configKeyDeserializer(Map<String, String> conf) {
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer(true));
    }

    protected void configValueDeserializer(Map<String, String> conf) {
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer(false));
    }

    protected void set(String key, String value) {
        props.setProperty(key, value);
    }


    @Override
    public final Properties props() {
        return new Properties(props);
    }

}
