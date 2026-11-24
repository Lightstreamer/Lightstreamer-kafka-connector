
/*
 * Copyright (C) 2026 Lightstreamer Srl
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

package com.lightstreamer.kafka.common.records;

import static com.google.common.truth.Truth.assertThat;

import com.lightstreamer.kafka.common.records.KafkaRecord.KafkaHeader;
import com.lightstreamer.kafka.common.records.KafkaRecord.KafkaHeaders;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

class KafkaHeadersTest {

    /** Builds a {@link KafkaHeaders} from ordered key/value pairs through one of the sources. */
    interface HeadersSource {
        KafkaHeaders build(List<Entry<String, byte[]>> entries);
    }

    static Stream<Named<HeadersSource>> sources() {
        return Stream.of(
                Named.of(
                        "RecordHeaders",
                        entries -> {
                            RecordHeaders headers = new RecordHeaders();
                            entries.forEach(e -> headers.add(e.getKey(), e.getValue()));
                            return KafkaHeaders.from(headers);
                        }),
                Named.of(
                        "ConnectHeaders",
                        entries -> {
                            ConnectHeaders headers = new ConnectHeaders();
                            entries.forEach(e -> headers.addBytes(e.getKey(), e.getValue()));
                            return KafkaHeaders.from(headers);
                        }));
    }

    @ParameterizedTest
    @MethodSource("sources")
    void shouldCreateKafkaHeaders(HeadersSource source) {
        byte[] value = "test-value".getBytes();

        KafkaHeaders headers = source.build(List.of(Map.entry("test-key", value)));

        assertThat(headers.has("test-key")).isTrue();
        assertThat(headers.size()).isEqualTo(1);
        KafkaHeader header = headers.get(0);
        assertThat(header.key()).isEqualTo("test-key");
        assertThat(header.value()).isEqualTo(value);
    }

    @ParameterizedTest
    @MethodSource("sources")
    void shouldKeepRepeatedKeys(HeadersSource source) {
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();

        KafkaHeaders headers =
                source.build(
                        List.of(
                                Map.entry("key1", value1),
                                Map.entry("key2", value2),
                                Map.entry("key1", value1)));

        assertThat(headers.has("key1")).isTrue();
        assertThat(headers.has("key2")).isTrue();
        assertThat(headers.size()).isEqualTo(3);
        assertThat(headers.headers("key1")).hasSize(2);
    }

    @ParameterizedTest
    @MethodSource("sources")
    void shouldCreateEmptyKafkaHeaders(HeadersSource source) {
        KafkaHeaders headers = source.build(List.of());

        assertThat(headers.size()).isEqualTo(0);
        assertThat(headers.has("any-key")).isFalse();
    }
}
