package com.lightstreamer.kafka_connector.adapter.mapping;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import com.lightstreamer.kafka_connector.adapter.mapping.ItemExpressionEvaluator.Result;
import com.lightstreamer.kafka_connector.adapter.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Schema;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Selectors;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Schema.MatchResult;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Schema.SchemaName;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Selectors.SelectorsSupplier;

public class Items {

    public static interface Item {

        Schema schema();

        Object itemHandle();

        Map<String, String> values();

        boolean matches(Item other);

    }

    public static interface ItemTemplates<K, V> {

        Stream<Item> expand(MappedRecord record);

        boolean matches(Item item);

        Stream<Selectors<K, V>> selectors();

        Optional<Selectors<K, V>> selectorsByName(String name);

        Stream<String> topics();

    }

    public static Item itemFrom(String input, Object itemHandle) throws ExpressionException {
        Result result = ItemExpressionEvaluator.subscribed().eval(input);
        return new DefaultItem(itemHandle, result.prefix(), result.params());
    }

    public static Item itemFrom(Object itemHandle, String prefix, Map<String, String> values) {
        return new DefaultItem(itemHandle, prefix, values);
    }

    // Invocato in fase di inizializzazione della configurazione
    public static <K, V> ItemTemplates<K, V> templatesFrom(List<TopicMapping> topics,
            SelectorsSupplier<K, V> selectorsSupplier)
            throws ExpressionException {
        List<ItemTemplate<K, V>> templates = new ArrayList<>();
        for (TopicMapping topic : topics) {
            for (String template : topic.itemTemplates()) {
                Result result = ItemExpressionEvaluator.template().eval(template);
                Selectors<K, V> selectors = Selectors.from(selectorsSupplier, SchemaName.of(result.prefix()),
                        result.params());
                templates.add(new ItemTemplate<>(topic.topic(), selectors));
            }
        }
        return new DefaultItemTemplates<>(templates);
    }

    static class DefaultItem implements Item {

        private final Object itemHandle;

        private final Map<String, String> valuesMap;

        private final Schema schema;

        DefaultItem(Object itemHandle, String prefix, Map<String, String> values) {
            this.valuesMap = values;
            this.itemHandle = itemHandle;
            this.schema = Schema.of(SchemaName.of(prefix), values.keySet());
        }

        @Override
        public Schema schema() {
            return schema;
        }

        @Override
        public Object itemHandle() {
            return itemHandle;
        }

        @Override
        public Map<String, String> values() {
            return Map.copyOf(valuesMap);
        }

        @Override
        public boolean matches(Item other) {
            MatchResult result = schema.matches(other.schema());
            if (!result.matched()) {
                return false;
            }

            return result.matchedKeys()
                    .stream()
                    .allMatch(key -> valuesMap.get(key).equals(other.values().get(key)));
        }

    }

    static class ItemTemplate<K, V> {

        private final Schema schema;

        private final String topic;

        private final Selectors<K, V> selectors;

        ItemTemplate(String topic, Selectors<K, V> selectors) {
            this.topic = Objects.requireNonNull(topic);
            this.selectors = Objects.requireNonNull(selectors);
            this.schema = selectors.schema();
        }

        Optional<Item> expand(MappedRecord record) {
            if (record.topic().equals(this.topic)) {
                Map<String, String> values = record.filter(selectors);
                return Optional.of(new DefaultItem("", schema.name().id(), values));
            }

            return Optional.empty();
        }

        Selectors<K, V> selectors() {
            return selectors;
        }

        String topic() {
            return topic;
        }

        Schema schema() {
            return schema;
        }

        public boolean matches(Item item) {
            return schema.matches(item.schema()).matched();
        }

    }

    static class DefaultItemTemplates<K, V> implements ItemTemplates<K, V> {

        private final List<ItemTemplate<K, V>> templates;

        DefaultItemTemplates(List<ItemTemplate<K, V>> templates) {
            this.templates = Collections.unmodifiableList(templates);
        }

        public Stream<Item> expand(MappedRecord record) {
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

        @Override
        public Optional<Selectors<K, V>> selectorsByName(String name) {
            return selectors().filter(s -> s.schema().name().id().equals(name)).findFirst();
        }
    }

}
