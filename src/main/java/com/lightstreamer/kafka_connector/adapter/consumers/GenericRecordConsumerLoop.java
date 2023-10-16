package com.lightstreamer.kafka_connector.adapter.consumers;

import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.lightstreamer.interfaces.data.ItemEventListener;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

public class GenericRecordConsumerLoop extends AbstractConsumerLoop<GenericRecord> {

    public GenericRecordConsumerLoop(Map<String, String> configuration, TopicMapping mappin,
            ItemEventListener eventListener) {
        super(configuration, mappin, GenericRecordSelector::new, eventListener);
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put("schema.registry.url", "http://schema-registry:8081");
    }

}
