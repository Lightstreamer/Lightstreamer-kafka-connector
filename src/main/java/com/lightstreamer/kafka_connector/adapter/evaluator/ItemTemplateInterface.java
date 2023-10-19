package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.List;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lightstreamer.kafka_connector.adapter.evaluator.BasicItem.MatchResult;

public interface ItemTemplateInterface<R> {

    Item expand(ConsumerRecord<String, R> record);

    String prefix();

    Set<String> schemas();

    MatchResult match(Item other);

    List<? extends ValueSelector<String,R>> valueSelectors();

}