
/*
 * Copyright (C) 2025 Lightstreamer Srl
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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

public final class KafkaSinkRecord implements KafkaRecord<Object, Object> {

    private final SinkRecord record;

    KafkaSinkRecord(SinkRecord record) {
        this.record = record;
    }

    @Override
    public Object key() {
        return record.key();
    }

    @Override
    public Object value() {
        return record.value();
    }

    @Override
    public boolean isPayloadNull() {
        return record.value() == null;
    }

    @Override
    public long timestamp() {
        return record.timestamp();
    }

    @Override
    public long offset() {
        return record.kafkaOffset();
    }

    @Override
    public String topic() {
        return record.topic();
    }

    @Override
    public int partition() {
        return record.kafkaPartition();
    }

    public Schema keySchema() {
        return record.keySchema();
    }

    public Schema valueSchema() {
        return record.valueSchema();
    }

    @Override
    public KafkaHeaders headers() {
        return new KafkaConnectHeaders(record.headers());
    }
}
