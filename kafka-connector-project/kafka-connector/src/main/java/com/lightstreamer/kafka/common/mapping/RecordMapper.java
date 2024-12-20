
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public interface RecordMapper<K, V> {

    interface MappedRecord {

        Set<SchemaAndValues> expanded();

        Map<String, String> fieldsMap();

        Set<SubscribedItem> route(Collection<? extends SubscribedItem> subscribed);
    }

    Set<DataExtractor<K, V>> getExtractorsByTopicSubscription(String topicName);

    MappedRecord map(KafkaRecord<K, V> record) throws ValueException;

    boolean hasExtractors();

    boolean hasFieldExtractor();

    boolean isRegexEnabled();

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

        final Map<String, Set<DataExtractor<K, V>>> extractorsByTopicSubscription = new HashMap<>();

        @SuppressWarnings("unchecked")
        DataExtractor<K, V> fieldExtractor = (DataExtractor<K, V>) NOP;

        boolean regexEnabled = false;

        private Builder() {}

        public Builder<K, V> withTemplateExtractors(
                Map<String, Set<DataExtractor<K, V>>> templateExtractors) {
            this.extractorsByTopicSubscription.putAll(templateExtractors);
            return this;
        }

        public final Builder<K, V> withTemplateExtractor(
                String subsscription, DataExtractor<K, V> templateExtractor) {
            this.extractorsByTopicSubscription.compute(
                    subsscription,
                    (t, extractors) -> {
                        if (extractors == null) {
                            extractors = new HashSet<>();
                        }
                        extractors.add(templateExtractor);
                        return extractors;
                    });
            return this;
        }

        public final Builder<K, V> enableRegex(boolean enable) {
            this.regexEnabled = enable;
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

final class DefaultRecordMapper<K, V> implements RecordMapper<K, V> {

    protected static Logger log = LoggerFactory.getLogger(DefaultRecordMapper.class);

    interface ExtractorsSupplier<K, V> {

        Collection<DataExtractor<K, V>> getExtractors(String topic);
    }

    static record PatternAndExtractors<K, V>(
            Pattern pattern, Set<DataExtractor<K, V>> extractors) {}

    private final DataExtractor<K, V> fieldExtractor;
    private final Map<String, Set<DataExtractor<K, V>>> templateExtractors;
    private final Collection<PatternAndExtractors<K, V>> patterns;
    private final ExtractorsSupplier<K, V> extractorsSupplier;
    private final boolean regexEnabled;

    DefaultRecordMapper(Builder<K, V> builder) {
        this.fieldExtractor = builder.fieldExtractor;
        this.templateExtractors =
                Collections.unmodifiableMap(builder.extractorsByTopicSubscription);
        this.regexEnabled = builder.regexEnabled;
        this.patterns = mayFillPatternsList(regexEnabled);
        this.extractorsSupplier =
                regexEnabled ? this::getMatchingExtractors : this::getAssociatedExtractors;
    }

    private Collection<PatternAndExtractors<K, V>> mayFillPatternsList(boolean regex) {
        if (!regex) {
            return Collections.emptyList();
        }

        Collection<PatternAndExtractors<K, V>> pe = new ArrayList<>();
        Set<String> topics = templateExtractors.keySet();
        for (String topicRegEx : topics) {
            pe.add(
                    new PatternAndExtractors<>(
                            Pattern.compile(topicRegEx), templateExtractors.get(topicRegEx)));
        }
        return pe;
    }

    private Collection<DataExtractor<K, V>> getAssociatedExtractors(String topic) {
        return templateExtractors.getOrDefault(topic, emptySet());
    }

    private Collection<DataExtractor<K, V>> getMatchingExtractors(String topic) {
        Collection<DataExtractor<K, V>> extractors = new ArrayList<>();
        for (PatternAndExtractors<K, V> p : patterns) {
            Matcher matcher = p.pattern().matcher(topic);
            if (matcher.matches()) {
                extractors.addAll(p.extractors());
            }
        }
        return extractors;
    }

    @Override
    public Set<DataExtractor<K, V>> getExtractorsByTopicSubscription(String topicName) {
        return templateExtractors.get(topicName);
    }

    public boolean hasExtractors() {
        return !templateExtractors.isEmpty();
    }

    public boolean hasFieldExtractor() {
        return fieldExtractor != Builder.NOP;
    }

    @Override
    public boolean isRegexEnabled() {
        return this.regexEnabled;
    }

    @Override
    public MappedRecord map(KafkaRecord<K, V> record) throws ValueException {
        var extractors = extractorsSupplier.getExtractors(record.topic());

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
