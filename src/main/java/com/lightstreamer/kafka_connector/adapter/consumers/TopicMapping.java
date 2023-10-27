package com.lightstreamer.kafka_connector.adapter.consumers;

import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lightstreamer.kafka_connector.adapter.evaluator.ItemTemplate;
import com.lightstreamer.kafka_connector.adapter.evaluator.RecordInspector;

public final class TopicMapping {

    private static Logger log = LoggerFactory.getLogger(TopicMapping.class);

    private final String topic;

    private final List<String> itemTemplates;

    public TopicMapping(String topic, List<String> itemTemplates) {
        Objects.requireNonNull(topic, "Null topic");
        Objects.requireNonNull(itemTemplates, "Null templates");
        this.topic = topic;
        this.itemTemplates = itemTemplates;
    }

    public String topic() {
        return topic;
    }

    public List<String> itemTemplates() {
        return itemTemplates;
    }

    public <K, V> List<ItemTemplate<K, V>> createItemTemplates(RecordInspector.Builder<K, V> inspector) {
        log.debug("Creating item templates for topic <{}>", topic());
        return itemTemplates.stream().map(s -> ItemTemplate.makeNew(topic, s, inspector)).toList();
    }

    public static <K, V> List<ItemTemplate<K, V>> flatItemTemplates(
            List<TopicMapping> topicMappings,
            RecordInspector.Builder<K, V> inspector) {

        return topicMappings.stream().flatMap(t -> t.createItemTemplates(inspector).stream()).toList();
    }

}
