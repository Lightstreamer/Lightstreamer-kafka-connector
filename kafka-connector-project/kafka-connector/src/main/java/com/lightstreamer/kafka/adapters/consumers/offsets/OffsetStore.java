
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

package com.lightstreamer.kafka.adapters.consumers.offsets;

import com.lightstreamer.kafka.common.utils.Split;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public interface OffsetStore {

    static Supplier<Collection<Long>> SUPPLIER = ArrayList::new;

    static String SEPARATOR = ",";

    static Predicate<String> NOT_EMPTY_STRING = ((Predicate<String>) String::isEmpty).negate();

    static String encode(Collection<Long> offsets) {
        return offsets.stream().map(String::valueOf).collect(Collectors.joining(SEPARATOR));
    }

    static Collection<Long> decode(String str) {
        if (str.isBlank()) {
            return Collections.emptyList();
        }
        return Split.byComma(str).stream()
                .filter(NOT_EMPTY_STRING)
                .map(Long::valueOf)
                .sorted()
                .collect(Collectors.toCollection(SUPPLIER));
    }

    static String append(String str, long offset) {
        String prefix = str.isEmpty() ? "" : str + SEPARATOR;
        return prefix + offset;
    }

    void save(ConsumerRecord<?, ?> record);

    default Map<TopicPartition, OffsetAndMetadata> getStore() {
        return Collections.emptyMap();
    }
}
