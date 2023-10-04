package com.lightstreamer.kafka_connector.adapter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.lightstreamer.interfaces.data.ItemEventListener;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

record Pair<K, V>(K first, V second) {
}

public class GenericRecordConsumerLoop extends AbstractConsumerLoop<GenericRecord> {

    private volatile CompletableFuture<Void> f = CompletableFuture.completedFuture(null);

    private List<RecordNavigator> recordNavigators;

    public GenericRecordConsumerLoop(Map<String, String> configuration, ItemEventListener eventListener) {
        super(configuration, eventListener);
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put("schema.registry.url", "http://schema-registry:8081");

        recordNavigators = configuration.entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith("item_"))
                .map(e -> new Pair<>(
                        e.getKey().substring(e.getKey().indexOf("_") + 1),
                        e.getValue()))
                .map(p -> new RecordNavigator(p.first(), p.second()))
                .toList();
    }

    @Override
    protected void consume(ConsumerRecord<String, GenericRecord> record) {
        Map<String, String> updates = new HashMap<>();
        for (RecordNavigator navigator : recordNavigators) {
            String value = navigator.extract(record.value());
            log.info("Extracted <{}> -> <{}>", navigator.getMappedField(), value);
            updates.put(navigator.getMappedField(), value);
        }
        log.info("Sending updates {}", updates);
        eventListener.update(record.topic(), updates, false);
    }

    @Override
    KafkaConsumer<String, GenericRecord> newKafkaConsumer(Properties properties) {
        return new KafkaConsumer<String, GenericRecord>(properties);
    }

}
