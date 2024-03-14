
/*
 * Copyright (C) 2024 Lightstreamer Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package com.lightstreamer.kafka_connector.adapters.mapping;

import static com.lightstreamer.kafka_connector.adapters.mapping.ExpressionException.reThrowInvalidExpression;

import com.lightstreamer.kafka_connector.adapters.config.TopicsConfig;
import com.lightstreamer.kafka_connector.adapters.config.TopicsConfig.ItemReference;
import com.lightstreamer.kafka_connector.adapters.config.TopicsConfig.TopicConfiguration;
import com.lightstreamer.kafka_connector.adapters.mapping.ItemExpressionEvaluator.Result;
import com.lightstreamer.kafka_connector.adapters.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.Schema;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.Selectors;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.Selectors.Selected;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Items {

    public interface Item {

        Schema schema();

        Object itemHandle();

        Map<String, String> values();

        String valueAt(String key);

        boolean matches(Item other);
    }

    public interface ItemTemplates<K, V> {

        Stream<Item> expand(MappedRecord record);

        boolean matches(Item item);

        Stream<Selectors<K, V>> selectors();

        Set<String> topics();

        public Set<Item> routes(MappedRecord record, Collection<? extends Item> subscribed);
    }

    public static Item itemFrom(String input, Object itemHandle) throws ExpressionException {
        Result result = ItemExpressionEvaluator.subscribed().eval(input);
        return new DefaultItem(itemHandle, result.prefix(), result.params());
    }

    public static Item itemFrom(Object itemHandle, String prefix, Map<String, String> values) {
        return new DefaultItem(itemHandle, prefix, values);
    }

    public static <K, V> ItemTemplates<K, V> templatesFrom(
            TopicsConfig topcisConfig, Selected<K, V> selected) {

        List<ItemTemplate<K, V>> templates = new ArrayList<>();
        for (TopicConfiguration topicConfig : topcisConfig.configurations()) {
            ItemReference itemReference = topicConfig.itemReference();
            if (itemReference.isTemplate()) {
                try {
                    Result result =
                            ItemExpressionEvaluator.template().eval(itemReference.templateValue());
                    Selectors<K, V> selectors =
                            Selectors.from(selected, result.prefix(), result.params());
                    templates.add(new ItemTemplate<>(topicConfig.topic(), selectors));
                } catch (ExpressionException e) {
                    reThrowInvalidExpression(
                            e, itemReference.templateKey(), itemReference.templateValue());
                }
            } else {
                Selectors<K, V> selectors =
                        Selectors.from(selected, itemReference.itemName(), Collections.emptyMap());
                templates.add(new ItemTemplate<>(topicConfig.topic(), selectors));
            }
        }
        return new DefaultItemTemplates<>(templates);
    }

    static class DefaultItem implements Item {

        private final Object itemHandle;

        private final Map<String, String> valuesMap;

        private final Schema schema;

        private final String str;

        DefaultItem(Object itemHandle, String prefix, Map<String, String> values) {
            this.valuesMap = values;
            this.itemHandle = itemHandle;
            this.schema = Schema.from(prefix, values.keySet());
            this.str = String.format("%s-<%s>", prefix, values.toString());
        }

        @Override
        public int hashCode() {
            return Objects.hash(itemHandle, valuesMap, schema);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;

            return obj instanceof DefaultItem other
                    && Objects.equals(itemHandle, other.itemHandle)
                    && Objects.equals(valuesMap, other.valuesMap)
                    && Objects.equals(schema, other.schema);
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
        public String valueAt(String key) {
            return valuesMap.get(key);
        }

        @Override
        public boolean matches(Item other) {
            if (!schema.matches(other.schema())) {
                return false;
            }

            return schema.keys().stream().allMatch(key -> valueAt(key).equals(other.valueAt(key)));
        }

        @Override
        public String toString() {
            return str;
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
                return Optional.of(new DefaultItem("", schema.name(), values));
            }

            return Optional.empty();
        }

        Selectors<K, V> selectors() {
            return selectors;
        }

        String topic() {
            return topic;
        }

        public boolean matches(Item item) {
            return schema.matches(item.schema());
        }
    }

    static class DefaultItemTemplates<K, V> implements ItemTemplates<K, V> {

        private final List<ItemTemplate<K, V>> templates;

        DefaultItemTemplates(List<ItemTemplate<K, V>> templates) {
            this.templates = Collections.unmodifiableList(templates);
        }

        @Override
        public Stream<Item> expand(MappedRecord record) {
            return templates.stream().flatMap(t -> t.expand(record).stream()).distinct();
        }

        @Override
        public Set<Item> routes(MappedRecord record, Collection<? extends Item> subscribed) {
            List<Item> expandeditems = expand(record).toList();
            return subscribed.stream()
                    .filter(s -> expandeditems.stream().anyMatch(expanded -> expanded.matches(s)))
                    .collect(Collectors.toSet());
        }

        @Override
        public boolean matches(Item item) {
            return templates.stream().anyMatch(i -> i.matches(item));
        }

        @Override
        public Stream<Selectors<K, V>> selectors() {
            return templates.stream().map(ItemTemplate::selectors).distinct();
        }

        @Override
        public Set<String> topics() {
            return templates.stream().map(ItemTemplate::topic).collect(Collectors.toSet());
        }
    }
}
