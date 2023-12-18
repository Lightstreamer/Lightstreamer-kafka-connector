package com.lightstreamer.kafka_connector.adapter.mapping;

import java.util.List;
import java.util.Objects;

public record TopicMapping(String topic, List<String> itemTemplates) {

    public TopicMapping(String topic, List<String> itemTemplates) {
        this.topic = Objects.requireNonNull(topic, "Null topic");
        this.itemTemplates = Objects.requireNonNull(itemTemplates, "Null templates");
    }

}
