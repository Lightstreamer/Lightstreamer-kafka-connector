
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
import com.lightstreamer.kafka_connector.adapters.config.TopicsConfig.TopicConfiguration;
import com.lightstreamer.kafka_connector.adapters.mapping.ItemExpressionEvaluator.Result;
import com.lightstreamer.kafka_connector.adapters.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.Schema;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.Schema.MatchResult;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.Selectors;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.Selectors.SelectorsSupplier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

public class Items {

    public interface Item {

        Schema schema();

        Object itemHandle();

        Map<String, String> values();

        boolean matches(Item other);
    }

    public interface ItemTemplates<K, V> {

        Stream<Item> expand(MappedRecord record);

        boolean matches(Item item);

        Stream<Selectors<K, V>> selectors();

        Stream<String> topics();
    }

    public static Item itemFrom(String input, Object itemHandle) throws ExpressionException {
        Result result = ItemExpressionEvaluator.subscribed().eval(input);
        return new DefaultItem(itemHandle, result.prefix(), result.params());
    }

    public static Item itemFrom(Object itemHandle, String prefix, Map<String, String> values) {
        return new DefaultItem(itemHandle, prefix, values);
    }

    public static <K, V> ItemTemplates<K, V> templatesFrom(
            TopicsConfig topcisConfig, SelectorsSupplier<K, V> selectorsSupplier) {

        List<ItemTemplate<K, V>> templates = new ArrayList<>();
        for (TopicConfiguration config : topcisConfig.configurations()) {
            try {
                Result result = ItemExpressionEvaluator.template().eval(config.itemTemplateValue());
                Selectors<K, V> selectors =
                        Selectors.from(selectorsSupplier, result.prefix(), result.params());
                templates.add(new ItemTemplate<>(config.topic(), selectors));
            } catch (ExpressionException e) {
                reThrowInvalidExpression(e, config.itemTemplateKey(), config.itemTemplateValue());
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

            return obj instanceof Item other
                    && Objects.equals(itemHandle, other.itemHandle())
                    && Objects.equals(valuesMap, other.values())
                    && Objects.equals(schema, other.schema());
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

            return result.matchedKeys().stream()
                    .allMatch(key -> valuesMap.get(key).equals(other.values().get(key)));
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
            return templates.stream().map(ItemTemplate::selectors).distinct();
        }

        @Override
        public Stream<String> topics() {
            return templates.stream().map(ItemTemplate::topic).distinct();
        }
    }
}
