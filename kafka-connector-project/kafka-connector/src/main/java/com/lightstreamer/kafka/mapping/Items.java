
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

package com.lightstreamer.kafka.mapping;

import static com.lightstreamer.kafka.mapping.selectors.ExpressionException.reThrowInvalidExpression;

import com.lightstreamer.kafka.config.TopicsConfig;
import com.lightstreamer.kafka.config.TopicsConfig.ItemReference;
import com.lightstreamer.kafka.config.TopicsConfig.TopicConfiguration;
import com.lightstreamer.kafka.mapping.ItemExpressionEvaluator.Result;
import com.lightstreamer.kafka.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.mapping.selectors.ExpressionException;
import com.lightstreamer.kafka.mapping.selectors.Schema;
import com.lightstreamer.kafka.mapping.selectors.SelectorSuppliers;
import com.lightstreamer.kafka.mapping.selectors.ValuesExtractor;

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

        Map<String, String> values();

        String valueAt(String key);

        boolean matches(Item other);
    }

    public interface SubscribedItem extends Item {

        Object itemHandle();
    }

    public interface ItemTemplates<K, V> {

        Stream<Item> expand(MappedRecord record);

        boolean matches(Item item);

        Stream<ValuesExtractor<K, V>> extractors();

        Set<String> topics();

        public Set<SubscribedItem> routes(
                MappedRecord record, Collection<? extends SubscribedItem> subscribed);
    }

    public static SubscribedItem subscribedFrom(String input) throws ExpressionException {
        return susbcribedFrom(input, input);
    }

    public static SubscribedItem susbcribedFrom(String input, Object itemHandle)
            throws ExpressionException {
        Result result = ItemExpressionEvaluator.subscribed().eval(input);
        return subscribedFrom(itemHandle, result.prefix(), result.params());
    }

    public static SubscribedItem subscribedFrom(
            Object itemHandle, String prefix, Map<String, String> values) {
        return new DefaultSubscribedItem(itemHandle, prefix, values);
    }

    public static Item from(String prefix, Map<String, String> values) {
        return new DefaultItem(prefix, values);
    }

    public static <K, V> ItemTemplates<K, V> templatesFrom(
            TopicsConfig topcisConfig, SelectorSuppliers<K, V> sSuppliers) {
        List<ItemTemplate<K, V>> templates = new ArrayList<>();
        for (TopicConfiguration topicConfig : topcisConfig.configurations()) {
            ItemReference reference = topicConfig.itemReference();
            if (reference.isTemplate()) {
                try {
                    Result result =
                            ItemExpressionEvaluator.template().eval(reference.templateValue());
                    ValuesExtractor<K, V> extractor =
                            ValuesExtractor.<K, V>builder()
                                    .withSuppliers(sSuppliers)
                                    .withSchemaName(result.prefix())
                                    .withExpressions(result.params())
                                    .build();
                    templates.add(new ItemTemplate<>(topicConfig.topic(), extractor));
                } catch (ExpressionException e) {
                    reThrowInvalidExpression(e, reference.templateKey(), reference.templateValue());
                }
            } else {
                ValuesExtractor<K, V> extractor =
                        ValuesExtractor.<K, V>builder()
                                .withSuppliers(sSuppliers)
                                .withSchemaName(reference.itemName())
                                .withExpressions(Collections.emptyMap())
                                .build();
                templates.add(new ItemTemplate<>(topicConfig.topic(), extractor));
            }
        }
        return new DefaultItemTemplates<>(templates);
    }

    static class DefaultItem implements Item {

        private final Map<String, String> valuesMap;
        private final Schema schema;
        private final String str;

        DefaultItem(String prefix, Map<String, String> values) {
            this.valuesMap = values;
            this.schema = Schema.from(prefix, values.keySet());
            this.str =
                    String.format("(%s-<%s>), {%s}", prefix, values.toString(), schema.toString());
        }

        @Override
        public int hashCode() {
            return Objects.hash(valuesMap, schema);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;

            return obj instanceof DefaultItem other
                    && Objects.equals(valuesMap, other.valuesMap)
                    && Objects.equals(schema, other.schema);
        }

        @Override
        public Schema schema() {
            return schema;
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

    static class DefaultSubscribedItem implements SubscribedItem {

        private final Object itemHandle;
        private final DefaultItem wrappedItem;

        DefaultSubscribedItem(Object itemHandle, String prefix, Map<String, String> values) {
            wrappedItem = new DefaultItem(prefix, values);
            this.itemHandle = itemHandle;
        }

        @Override
        public int hashCode() {
            return Objects.hash(itemHandle, wrappedItem);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            return obj instanceof DefaultSubscribedItem other
                    && wrappedItem.equals(other.wrappedItem)
                    && Objects.equals(itemHandle, other.itemHandle);
        }

        @Override
        public Schema schema() {
            return wrappedItem.schema;
        }

        @Override
        public Map<String, String> values() {
            return wrappedItem.values();
        }

        @Override
        public String valueAt(String key) {
            return wrappedItem.valueAt(key);
        }

        @Override
        public boolean matches(Item other) {
            return wrappedItem.matches(other);
        }

        @Override
        public Object itemHandle() {
            return itemHandle;
        }
    }

    static class ItemTemplate<K, V> {

        private final Schema schema;
        private final String topic;
        private final ValuesExtractor<K, V> extractor;

        ItemTemplate(String topic, ValuesExtractor<K, V> extractor) {
            this.topic = Objects.requireNonNull(topic);
            this.extractor = Objects.requireNonNull(extractor);
            this.schema = extractor.schema();
        }

        Optional<Item> expand(MappedRecord record) {
            if (record.topic().equals(this.topic)) {
                Map<String, String> values = record.filter(extractor);
                return Optional.of(new DefaultItem(schema.name(), values));
            }

            return Optional.empty();
        }

        ValuesExtractor<K, V> selectors() {
            return extractor;
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
            return templates.stream()
                    .flatMap(template -> template.expand(record).stream())
                    .distinct();
        }

        @Override
        public Set<SubscribedItem> routes(
                MappedRecord record, Collection<? extends SubscribedItem> subscribed) {
            return subscribed.stream()
                    .filter(sItem -> expand(record).anyMatch(eItem -> eItem.matches(sItem)))
                    .collect(Collectors.toSet());
        }

        @Override
        public boolean matches(Item item) {
            return templates.stream().anyMatch(i -> i.matches(item));
        }

        @Override
        public Stream<ValuesExtractor<K, V>> extractors() {
            return templates.stream().map(ItemTemplate::selectors).distinct();
        }

        @Override
        public Set<String> topics() {
            return templates.stream().map(ItemTemplate::topic).collect(Collectors.toSet());
        }
    }
}
