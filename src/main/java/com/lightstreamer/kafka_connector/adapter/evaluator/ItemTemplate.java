package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lightstreamer.kafka_connector.adapter.consumers.TopicMapping;
import com.lightstreamer.kafka_connector.adapter.evaluator.ItemExpressionEvaluator.EvaluationException;
import com.lightstreamer.kafka_connector.adapter.evaluator.ItemExpressionEvaluator.Result;

public class ItemTemplate<K, V> {

    private final ItemSchema schema;

    private final String topic;

    private final RecordInspector<K, V> inspector;

    ItemTemplate(String topic, String prefix, RecordInspector<K, V> inspector) {
        this.topic = Objects.requireNonNull(topic);
        this.inspector = Objects.requireNonNull(inspector);
        this.schema = ItemSchema.of(prefix, inspector.names());
    }

    public Optional<Item> expand(ConsumerRecord<K, V> record) {
        if (record.topic().equals(this.topic)) {
            Map<String, String> replaced = inspector.inspect(record);
            return Optional.of(new Item("", schema.prefix(), replaced));
        }
        return Optional.empty();

    }

    String topic() {
        return this.topic;
    }

    ItemSchema schema() {
        return this.schema;
    }

    public boolean matches(Item item) {
        return this.schema.matches(item.schema()).matched();
    }

    public static <K, V> List<ItemTemplate<K, V>> fromTopicMappings(
            List<TopicMapping> topics,
            RecordInspector.Builder<K, V> builder) {

        return topics.stream().flatMap(topic -> fromTopicMapping(topic, builder).stream()).toList();
    }

    private static <K, V> List<ItemTemplate<K, V>> fromTopicMapping(
            TopicMapping topic,
            RecordInspector.Builder<K, V> builder) {

        return topic.itemTemplates().stream()
                .map(s -> create(topic.topic(), s, builder)).toList();
    }

    public static <K, V> ItemTemplate<K, V> create(String topic, String template,
            RecordInspector.Builder<K, V> builder) throws EvaluationException {
        Result result = ItemExpressionEvaluator.template().eval(template);
        result.params().entrySet().stream().forEach(p -> builder.instruct(p.getKey(), p.getValue()));
        return new ItemTemplate<>(topic, result.prefix(), builder.build());
    }
}
