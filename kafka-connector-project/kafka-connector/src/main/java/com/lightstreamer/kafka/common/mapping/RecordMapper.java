
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

package com.lightstreamer.kafka.common.mapping;

import static java.util.Collections.emptySet;

import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.common.mapping.selectors.DataExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.Schema;
import com.lightstreamer.kafka.common.mapping.selectors.SchemaAndValues;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public interface RecordMapper<K, V> {

    interface MappedRecord {

        Set<SchemaAndValues> expanded();

        Map<String, String> fieldsMap();

        Set<SubscribedItem> route(Collection<? extends SubscribedItem> subscribed);
    }

    Set<DataExtractor<K, V>> getExtractorsByTopicName(String topicName);

    MappedRecord map(KafkaRecord<K, V> record) throws ValueException;

    boolean hasExtractors();

    boolean hasFieldExtractor();

    static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    static class NOPDataExtractor<K, V> implements DataExtractor<K, V> {

        @Override
        public SchemaAndValues extractData(KafkaRecord<K, V> record) {
            return SchemaAndValues.nop();
        }

        @Override
        public Schema schema() {
            return SchemaAndValues.nop().schema();
        }
    }

    static class Builder<K, V> {

        static final DataExtractor<?, ?> NOP = new NOPDataExtractor<>();

        final Map<String, Set<DataExtractor<K, V>>> extractorsByTopicName = new HashMap<>();

        @SuppressWarnings("unchecked")
        DataExtractor<K, V> fieldExtractor = (DataExtractor<K, V>) NOP;

        private Builder() {}

        public Builder<K, V> withTemplateExtractors(
                Map<String, Set<DataExtractor<K, V>>> templateExtractors) {
            this.extractorsByTopicName.putAll(templateExtractors);
            return this;
        }

        public final Builder<K, V> withTemplateExtractor(
                String topic, DataExtractor<K, V> templateExtractor) {
            this.extractorsByTopicName.compute(
                    topic,
                    (t, extractors) -> {
                        if (extractors == null) {
                            extractors = new HashSet<>();
                        }
                        extractors.add(templateExtractor);
                        return extractors;
                    });
            return this;
        }

        public final Builder<K, V> withFieldExtractor(DataExtractor<K, V> extractor) {
            this.fieldExtractor = extractor;
            return this;
        }

        public RecordMapper<K, V> build() {
            return new DefaultRecordMapper<>(this);
        }
    }
}

class DefaultRecordMapper<K, V> implements RecordMapper<K, V> {

    protected static Logger log = LoggerFactory.getLogger(DefaultRecordMapper.class);

    private final Map<String, Set<DataExtractor<K, V>>> templateExtractors;
    private final DataExtractor<K, V> fieldExtractor;

    DefaultRecordMapper(Builder<K, V> builder) {
        this.templateExtractors = Collections.unmodifiableMap(builder.extractorsByTopicName);
        this.fieldExtractor = builder.fieldExtractor;
    }

    @Override
    public Set<DataExtractor<K, V>> getExtractorsByTopicName(String topicName) {
        return templateExtractors.get(topicName);
    }

    public boolean hasExtractors() {
        return !templateExtractors.isEmpty();
    }

    public boolean hasFieldExtractor() {
        return fieldExtractor != Builder.NOP;
    }

    @Override
    public MappedRecord map(KafkaRecord<K, V> record) throws ValueException {
        var extractors = templateExtractors.getOrDefault(record.topic(), emptySet());
        if (extractors.isEmpty()) {
            return DefaultMappedRecord.NOPRecord;
        }

        Set<SchemaAndValues> set = new HashSet<>();
        for (DataExtractor<K, V> dataExtractor : extractors) {
            set.add(dataExtractor.extractData(record));
        }

        SchemaAndValues mappedFields = fieldExtractor.extractData(record);
        return new DefaultMappedRecord(set, mappedFields);
    }
}

final class DefaultMappedRecord implements MappedRecord {

    static final DefaultMappedRecord NOPRecord = new DefaultMappedRecord();

    private final SchemaAndValues fieldsMap;
    private final SchemaAndValues[] indexedTemplates;

    DefaultMappedRecord() {
        this(emptySet(), SchemaAndValues.nop());
    }

    DefaultMappedRecord(Set<SchemaAndValues> expandedTemplates, SchemaAndValues fieldsMap) {
        this.indexedTemplates = expandedTemplates.toArray(new SchemaAndValues[] {});
        this.fieldsMap = fieldsMap;
    }

    @Override
    public Set<SchemaAndValues> expanded() {
        return Set.of(indexedTemplates);
    }

    @Override
    public Set<SubscribedItem> route(Collection<? extends SubscribedItem> subscribedItems) {
        Set<SubscribedItem> result = new HashSet<>();

        // The following seems the most performant loop way
        // to popoulate the set of routable subscriptions.
        for (SubscribedItem item : subscribedItems) {
            for (SchemaAndValues e : indexedTemplates) {
                if (e.matches(item)) {
                    result.add(item);
                    break;
                }
            }
        }
        return result;
    }

    @Override
    public Map<String, String> fieldsMap() {
        return fieldsMap.values();
    }

    @Override
    public String toString() {
        String data =
                Arrays.stream(indexedTemplates)
                        .map(v -> v.values().toString())
                        .collect(Collectors.joining(", "));
        return String.format(
                "MappedRecord [expandedTemplates=[%s], fieldsMap=%s]", data, fieldsMap);
    }
}
