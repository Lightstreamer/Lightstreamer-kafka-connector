package com.lightstreamer.kafka_connector.adapter.mapping;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import com.lightstreamer.kafka_connector.adapter.mapping.ItemExpressionEvaluator.EvaluationException;
import com.lightstreamer.kafka_connector.adapter.mapping.ItemExpressionEvaluator.Result;
import com.lightstreamer.kafka_connector.adapter.mapping.RecordInspector.RemappedRecord;
import com.lightstreamer.kafka_connector.adapter.mapping.Selectors.SelectorsSupplier;

class ItemTemplate<K, V> {

    private final Schema schema;

    private final String topic;

    private final String prefix;

    private final Selectors<K, V> selectors;

    ItemTemplate(String topic, String prefix, Selectors<K, V> selectors) {
        this.topic = Objects.requireNonNull(topic);
        this.selectors = Objects.requireNonNull(selectors);
        this.schema = selectors.schema();
        this.prefix = Objects.requireNonNull(prefix);
    }

    Optional<Item> expand(RemappedRecord record) {
        if (record.topic().equals(this.topic)) {
            Map<String, String> values = record.filter(schema);
            return Optional.of(new Item("", prefix, values));
        }

        return Optional.empty();
    }

    Selectors<K, V> selectors() {
        return selectors;
    }

    String topic() {
        return this.topic;
    }

    Schema schema() {
        return this.schema;
    }

    public boolean matches(Item item) {
        return this.schema.matches(item.schema()).matched();
    }

}

public interface ItemTemplates<K, V> {

    Stream<Item> expand(RemappedRecord record);

    boolean matches(Item item);

    Stream<Selectors<K, V>> selectors();

    Stream<String> topics();

    static <K, V> ItemTemplates<K, V> of(List<TopicMapping> topics, SelectorsSupplier<K, V> selectorsSupplier)
            throws EvaluationException {
        List<ItemTemplate<K, V>> templates = new ArrayList<>();
        for (TopicMapping topic : topics) {
            for (String template : topic.itemTemplates()) {
                Result result = ItemExpressionEvaluator.template().eval(template);
                Selectors<K, V> selectors = Selectors.builder(selectorsSupplier)
                        .withMap(result.params())
                        .build();
                templates.add(new ItemTemplate<>(topic.topic(), result.prefix(), selectors));
            }
        }
        return new DefaultItemTemplates<>(templates);
    }
}

class DefaultItemTemplates<K, V> implements ItemTemplates<K, V> {

    private final List<ItemTemplate<K, V>> templates;

    DefaultItemTemplates(List<ItemTemplate<K, V>> templates) {
        this.templates = templates;
    }

    public Stream<Item> expand(RemappedRecord record) {
        return templates.stream().flatMap(i -> i.expand(record).stream());
    }

    public boolean matches(Item item) {
        return templates.stream().anyMatch(i -> i.matches(item));
    }

    @Override
    public Stream<Selectors<K, V>> selectors() {
        return templates.stream().map(ItemTemplate::selectors);
    }

    @Override
    public Stream<String> topics() {
        return templates.stream().map(ItemTemplate::topic);
    }
}
