package com.lightstreamer.kafka_connector.adapter.consumers;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.lightstreamer.interfaces.data.ItemEventListener;

public class RawConsumerLoop extends AbstractConsumerLoop<String> {

    public RawConsumerLoop(Map<String, String> configuration, List<TopicMapping> mappings, ItemEventListener eventListener) {
        super(configuration, mappings, RawValueSelector::new, eventListener);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }
}
