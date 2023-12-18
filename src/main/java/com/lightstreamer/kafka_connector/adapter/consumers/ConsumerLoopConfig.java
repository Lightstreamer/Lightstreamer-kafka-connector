package com.lightstreamer.kafka_connector.adapter.consumers;

import java.util.Properties;

import com.lightstreamer.kafka_connector.adapter.mapping.ItemTemplates;
import com.lightstreamer.kafka_connector.adapter.mapping.Selectors;

public interface ConsumerLoopConfig<K, V> {

    Properties consumerProperties();

    ItemTemplates<K, V> itemTemplates();

    Selectors<K, V> fieldsSelectors();

    static <K,V> ConsumerLoopConfig<K,V> of(Properties properties, ItemTemplates<K,V> templates, Selectors<K,V> fiSelectors) {
        return null;
    }

}
