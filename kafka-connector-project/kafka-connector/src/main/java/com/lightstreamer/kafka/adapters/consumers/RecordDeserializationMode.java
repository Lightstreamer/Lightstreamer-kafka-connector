
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

package com.lightstreamer.kafka.adapters.consumers;

import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.common.records.KafkaRecord.DeserializerPair;
import com.lightstreamer.kafka.common.records.RecordBatch;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;

import java.time.Duration;

/**
 * Encapsulates the deserialization strategy for Kafka records polled as raw bytes.
 *
 * <p>Subclasses define whether deserialization happens eagerly (at poll time) or is deferred (on
 * first access). Use the {@link #forTiming(DeserializationTiming, DeserializerPair, Logger)}
 * factory method to obtain the appropriate implementation.
 *
 * @param <K> the type of the key in the Kafka record
 * @param <V> the type of the value in the Kafka record
 */
public abstract class RecordDeserializationMode<K, V> {

    /** Defines when deserialization of Kafka record keys and values is performed. */
    public enum DeserializationTiming {
        DEFERRED,
        EAGER
    }

    protected final DeserializationTiming timing;
    protected final KafkaRecord.DeserializerPair<K, V> deserializerPair;
    protected final Logger logger;

    RecordDeserializationMode(
            DeserializationTiming timing,
            KafkaRecord.DeserializerPair<K, V> deserializerPair,
            Logger logger) {
        this.timing = timing;
        this.deserializerPair = deserializerPair;
        if (logger == null) {
            throw new NullPointerException("logger must not be null");
        }
        this.logger = logger;
    }

    /**
     * Converts raw polled records into a typed {@link RecordBatch}.
     *
     * @param records the raw records returned by {@link Consumer#poll(Duration)}
     * @param joinable whether the batch should support synchronous join semantics
     * @return a new {@link RecordBatch} containing the deserialized records
     */
    public abstract RecordBatch<K, V> toBatch(
            ConsumerRecords<byte[], byte[]> records, boolean joinable);

    /**
     * Converts raw polled records into a non-joinable {@link RecordBatch}.
     *
     * @param records the raw records returned by {@link Consumer#poll(Duration)}
     * @return a new {@link RecordBatch} containing the deserialized records
     */
    public RecordBatch<K, V> toBatch(ConsumerRecords<byte[], byte[]> records) {
        return toBatch(records, false);
    }

    /**
     * Returns the {@link DeserializationTiming} strategy used by this instance.
     *
     * @return the deserialization timing
     */
    public DeserializationTiming getTiming() {
        return timing;
    }

    /**
     * Creates a {@code RecordDeserializationMode} for the specified timing strategy.
     *
     * <p>In eager mode, records that fail deserialization are skipped and logged individually,
     * unless all records in a batch fail (indicating a systemic configuration error).
     *
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     * @param timing the {@link DeserializationTiming} to use
     * @param deserializerPair the {@link DeserializerPair} for key and value deserialization
     * @param logger the {@link Logger} to use for logging deserialization errors
     * @return a new {@code RecordDeserializationMode} instance
     */
    public static <K, V> RecordDeserializationMode<K, V> forTiming(
            DeserializationTiming timing, DeserializerPair<K, V> deserializerPair, Logger logger) {
        switch (timing) {
            case DEFERRED:
                return new DeferredDeserializationMode<>(deserializerPair, logger);
            case EAGER:
                return new EagerDeserializationMode<>(deserializerPair, logger);
            default:
                throw new IllegalArgumentException("Unknown timing: " + timing);
        }
    }

    /**
     * A {@code RecordDeserializationMode} that defers deserialization until first record access.
     *
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     */
    private static class DeferredDeserializationMode<K, V> extends RecordDeserializationMode<K, V> {

        DeferredDeserializationMode(
                KafkaRecord.DeserializerPair<K, V> deserializerPair, Logger logger) {
            super(DeserializationTiming.DEFERRED, deserializerPair, logger);
        }

        @Override
        public RecordBatch<K, V> toBatch(
                ConsumerRecords<byte[], byte[]> records, boolean joinable) {
            return RecordBatch.batchFromDeferred(records, deserializerPair, joinable);
        }
    }

    /**
     * A {@code RecordDeserializationMode} that deserializes records eagerly at poll time.
     *
     * <p>Records that fail deserialization are skipped and logged, unless all records in a batch
     * fail (indicating a systemic configuration error).
     *
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     */
    private static class EagerDeserializationMode<K, V> extends RecordDeserializationMode<K, V> {

        EagerDeserializationMode(
                KafkaRecord.DeserializerPair<K, V> deserializerPair, Logger logger) {
            super(DeserializationTiming.EAGER, deserializerPair, logger);
        }

        @Override
        public RecordBatch<K, V> toBatch(
                ConsumerRecords<byte[], byte[]> records, boolean joinable) {
            return RecordBatch.batchFromEager(
                    records,
                    deserializerPair,
                    joinable,
                    (record, ex) ->
                            logger.atWarn()
                                    .setCause(ex)
                                    .log(
                                            "Skipping record due to deserialization"
                                                    + " error [topic={}, partition={},"
                                                    + " offset={}]",
                                            record.topic(),
                                            record.partition(),
                                            record.offset()));
        }
    }
}
