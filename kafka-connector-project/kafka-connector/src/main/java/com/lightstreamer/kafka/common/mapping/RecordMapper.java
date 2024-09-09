
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

import com.lightstreamer.kafka.common.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.common.mapping.selectors.Data;
import com.lightstreamer.kafka.common.mapping.selectors.DataContainer;
import com.lightstreamer.kafka.common.mapping.selectors.DataExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;

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

        Map<String, String> filter(DataExtractor<?, ?> extractor);

        Map<String, String> filter2_0(DataExtractor<?, ?> extractor);
    }

    int selectorsSize();

    MappedRecord map(KafkaRecord<K, V> record) throws ValueException;

    MappedRecord map_old(KafkaRecord<K, V> record) throws ValueException;

    MappedRecord map2_0(KafkaRecord<K, V> record) throws ValueException;

    static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    static class Builder<K, V> {

        final Set<DataExtractor<K, V>> allExtractors = new HashSet<>();

        private Builder() {}

        public Builder<K, V> withExtractor(Stream<DataExtractor<K, V>> extractor) {
            allExtractors.addAll(extractor.toList());
            return this;
        }

        public final Builder<K, V> withExtractor(DataExtractor<K, V> extractor) {
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

    private final Set<DataExtractor<K, V>> extractors;

    DefaultRecordMapper(Builder<K, V> builder) {
        this.extractors = Collections.unmodifiableSet(builder.allExtractors);
    }

    @Override
    public MappedRecord map(KafkaRecord<K, V> record) throws ValueException {
        Set<DataContainer> set = new HashSet<>();
        for (DataExtractor<K, V> dataExtractor : extractors) {
            set.add(dataExtractor.extractData(record));
        }
        return new DefaultMappedRecord(record.topic(), set);
    }

    @Override
    public MappedRecord map2_0(KafkaRecord<K, V> record) throws ValueException {
        Set<DataContainer> set = new HashSet<>();
        for (DataExtractor<K, V> dataExtractor : extractors) {
            DataContainer data = dataExtractor.extractData2_0(record);
            set.add(data);
        }
        return new DefaultMappedRecord(record.topic(), set);
    }

    @Override
    public MappedRecord map_old(KafkaRecord<K, V> record) throws ValueException {
        return new DefaultMappedRecord(
                record.topic(),
                extractors.stream().map(ve -> ve.extractData(record)).collect(Collectors.toSet()));
    }

    @Override
    public int selectorsSize() {
        return this.extractors.size();
    }
}

class DefaultMappedRecord implements MappedRecord {

    private final String topic;

    private final Set<DataContainer> dataContainers;

    DefaultMappedRecord(String topic, Set<DataContainer> dataContainers) {
        this.topic = topic;
        this.dataContainers = dataContainers;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public int mappedValuesSize() {
        return dataContainers.stream().mapToInt(v -> v.data().size()).sum();
    }

    @Override
    public Map<String, String> filter(DataExtractor<?, ?> extractor) {
        Map<String, String> eventsMap = new HashMap<>();
        dataContainers.stream()
                .filter(container -> container.extractor().equals(extractor))
                .flatMap(container -> container.data().stream())
                .forEach(value -> eventsMap.put(value.name(), value.text()));
        return eventsMap;
    }

    @Override
    public Map<String, String> filter2_0(DataExtractor<?, ?> extractor) {
        for (DataContainer dataContainer : dataContainers) {
            if (dataContainer.extractor().equals(extractor)) {
                return dataContainer.dataAsMap();
            }
        }
        return Collections.emptyMap();
        // return dataContainers.stream()
        //         .filter(container -> container.extractor().equals(extractor))
        //         .findFirst()
        //         .map(container -> container.dataAsMap()).orElseThrow();
    }

    @Override
    public String toString() {
        String data =
                dataContainers.stream()
                        .flatMap(v -> v.data().stream())
                        .map(Data::toString)
                        .collect(Collectors.joining(","));
        return String.format("MappedRecord [topic=[%s],values=[%s]]", topic, data);
    }
}
