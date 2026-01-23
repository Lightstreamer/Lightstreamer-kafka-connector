
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
import org.junit.Test;

public class KafkaHeadersTest {

    @Test
    public void shouldCreateKafkaHeadersFromRecordHeaders() {
        byte[] value = "test-value".getBytes();
        org.apache.kafka.common.header.Headers recordHeaders =
                new RecordHeaders().add("test-key", value);

        KafkaHeaders headers = KafkaHeaders.from(recordHeaders);

        assertThat(headers.has("test-key")).isTrue();
        assertThat(headers.size()).isEqualTo(1);
        KafkaHeader header = headers.get(0);
        assertThat(header.key()).isEqualTo("test-key");
        assertThat(header.value()).isEqualTo(value);
    }

    @Test
    public void shouldCreateKafkaHeadersFromRecordHeadersMultiple() {
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        org.apache.kafka.common.header.Headers recordHeaders =
                new RecordHeaders().add("key1", value1).add("key2", value2).add("key1", value1);

        KafkaHeaders headers = KafkaHeaders.from(recordHeaders);

        assertThat(headers.has("key1")).isTrue();
        assertThat(headers.has("key2")).isTrue();
        assertThat(headers.size()).isEqualTo(3);
        assertThat(headers.headers("key1")).hasSize(2);
    }

    @Test
    public void shouldCreateKafkaHeadersFromConnectHeaders() {
        byte[] value = "connect-value".getBytes();
        org.apache.kafka.connect.header.Headers connectHeaders =
                new ConnectHeaders().addBytes("connect-key", value);

        KafkaHeaders headers = KafkaHeaders.from(connectHeaders);

        assertThat(headers.has("connect-key")).isTrue();
        assertThat(headers.size()).isEqualTo(1);
        KafkaHeader header = headers.get(0);
        assertThat(header.key()).isEqualTo("connect-key");
        assertThat(header.value()).isEqualTo(value);
    }

    @Test
    public void shouldCreateKafkaHeadersFromConnectHeadersMultiple() {
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        org.apache.kafka.connect.header.Headers connectHeaders =
                new ConnectHeaders()
                        .addBytes("key1", value1)
                        .addBytes("key2", value2)
                        .addBytes("key1", value1);

        KafkaHeaders headers = KafkaHeaders.from(connectHeaders);

        assertThat(headers.has("key1")).isTrue();
        assertThat(headers.has("key2")).isTrue();
        assertThat(headers.size()).isEqualTo(3);
        assertThat(headers.headers("key1")).hasSize(2);
    }

    @Test
    public void shouldCreateKafkaHeadersFromEmptyRecordHeaders() {
        org.apache.kafka.common.header.Headers recordHeaders = new RecordHeaders();

        KafkaHeaders headers = KafkaHeaders.from(recordHeaders);

        assertThat(headers.size()).isEqualTo(0);
        assertThat(headers.has("any-key")).isFalse();
    }

    @Test
    public void shouldCreateKafkaHeadersFromEmptyConnectHeaders() {
        org.apache.kafka.connect.header.Headers connectHeaders = new ConnectHeaders();

        KafkaRecord.KafkaHeaders headers = KafkaRecord.KafkaHeaders.from(connectHeaders);

        assertThat(headers.size()).isEqualTo(0);
        assertThat(headers.has("any-key")).isFalse();
    }
}
