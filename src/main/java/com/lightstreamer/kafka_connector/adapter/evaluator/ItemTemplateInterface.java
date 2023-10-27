package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.List;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lightstreamer.kafka_connector.adapter.evaluator.BasicItem.MatchResult;

public interface ItemTemplateInterface<K, V> {

    Item expand(ConsumerRecord<K, V> record);

    String prefix();

    Set<String> schemas();

    MatchResult match(Item other);

    // List<? extends ValueSelector<V>> valueSelectors();

}