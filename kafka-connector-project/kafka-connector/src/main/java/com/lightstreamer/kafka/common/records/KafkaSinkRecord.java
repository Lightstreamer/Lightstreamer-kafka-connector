
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

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * A {@link KafkaRecord} implementation that wraps a Kafka Connect {@link SinkRecord}.
 *
 * <p>This class provides access to {@link SinkRecord} properties through the {@link KafkaRecord}
 * interface, allowing unified handling of records from different sources. It also provides access
 * to schema information via {@link #keySchemaAndValue()} and {@link #valueSchemaAndValue()}.
 */
public final class KafkaSinkRecord implements KafkaRecord<Object, Object> {

    /** The underlying Kafka Connect {@link SinkRecord}. */
    private final SinkRecord record;

    /**
     * Constructs a {@link KafkaSinkRecord} wrapping the given {@link SinkRecord}.
     *
     * @param record the Kafka Connect {@link SinkRecord} to wrap
     */
    KafkaSinkRecord(SinkRecord record) {
        this.record = record;
    }

    /**
     * Returns the key from the wrapped {@link SinkRecord}.
     *
     * @return the record key
     */
    @Override
    public Object key() {
        return record.key();
    }

    /**
     * Returns the value from the wrapped {@link SinkRecord}.
     *
     * @return the record value
     */
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

    /**
     * Returns the key with its schema from the wrapped {@link SinkRecord}.
     *
     * @return a {@link SchemaAndValue} containing the key schema and value
     */
    public SchemaAndValue keySchemaAndValue() {
        return new SchemaAndValue(record.keySchema(), record.key());
    }

    /**
     * Returns the value with its schema from the wrapped {@link SinkRecord}.
     *
     * @return a {@link SchemaAndValue} containing the value schema and value
     */
    public SchemaAndValue valueSchemaAndValue() {
        return new SchemaAndValue(record.valueSchema(), record.value());
    }

    /**
     * Returns the headers from the wrapped {@link SinkRecord}.
     *
     * @return the record headers
     */
    @Override
    public KafkaHeaders headers() {
        return new KafkaHeadersImpl(record.headers());
    }

    @Override
    public RecordBatch<Object, Object> getBatch() {
        return null;
    }
}
