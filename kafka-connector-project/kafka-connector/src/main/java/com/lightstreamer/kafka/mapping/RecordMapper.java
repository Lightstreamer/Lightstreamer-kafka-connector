
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

import com.lightstreamer.kafka.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.mapping.selectors.Value;
import com.lightstreamer.kafka.mapping.selectors.ValueException;
import com.lightstreamer.kafka.mapping.selectors.ValuesContainer;
import com.lightstreamer.kafka.mapping.selectors.ValuesExtractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface RecordMapper<K, V> {

    interface MappedRecord {

        String topic();

        int mappedValuesSize();

        Map<String, String> filter(ValuesExtractor<?, ?> extractor);
    }

    int selectorsSize();

    MappedRecord map(KafkaRecord<K, V> record);

    static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    static class Builder<K, V> {

        final Set<ValuesExtractor<K, V>> allExtractors = new HashSet<>();

        private Builder() {}

        public Builder<K, V> withExtractor(Stream<ValuesExtractor<K, V>> extractor) {
            allExtractors.addAll(extractor.toList());
            return this;
        }

        public final Builder<K, V> withExtractor(ValuesExtractor<K, V> extractor) {
            allExtractors.add(extractor);
            return this;
        }

        public RecordMapper<K, V> build() {
            return new DefaultRecordMapper<>(this);
        }
    }
}

class DefaultRecordMapper<K, V> implements RecordMapper<K, V> {

    protected static Logger log = LoggerFactory.getLogger(DefaultRecordMapper.class);

    private final Set<ValuesExtractor<K, V>> extractors;

    DefaultRecordMapper(Builder<K, V> builder) {
        this.extractors = Collections.unmodifiableSet(builder.allExtractors);
    }

    @Override
    public MappedRecord map(KafkaRecord<K, V> record) throws ValueException {
        return new DefaultMappedRecord(
                record.topic(),
                extractors.stream()
                        .map(ve -> ve.extractValues(record))
                        .collect(Collectors.toSet()));
    }

    @Override
    public int selectorsSize() {
        return this.extractors.size();
    }
}

class DefaultMappedRecord implements MappedRecord {

    private final String topic;

    private final Set<ValuesContainer> valuesContainers;

    DefaultMappedRecord(String topic, Set<ValuesContainer> valuesContainers) {
        this.topic = topic;
        this.valuesContainers = valuesContainers;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public int mappedValuesSize() {
        return valuesContainers.stream().mapToInt(v -> v.values().size()).sum();
    }

    @Override
    public Map<String, String> filter(ValuesExtractor<?, ?> extractor) {
        Map<String, String> eventsMap = new HashMap<>();
        valuesContainers.stream()
                .filter(container -> container.extractor().equals(extractor))
                .flatMap(container -> container.values().stream())
                .forEach(value -> eventsMap.put(value.name(), value.text()));
        return eventsMap;
    }

    @Override
    public String toString() {
        String values =
                valuesContainers.stream()
                        .flatMap(v -> v.values().stream())
                        .map(Value::toString)
                        .collect(Collectors.joining(","));
        return String.format("MappedRecord [topic=[%s],values=[%s]]", topic, values);
    }
}
