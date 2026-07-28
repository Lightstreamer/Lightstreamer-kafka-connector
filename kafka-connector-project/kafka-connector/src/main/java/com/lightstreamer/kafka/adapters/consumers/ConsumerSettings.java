
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

package com.lightstreamer.kafka.adapters.consumers;

import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.ConsumerMode;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeFrom;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.selectors.FieldsExtractor;
import com.lightstreamer.kafka.common.records.KafkaRecord;

import java.util.Properties;

/** Namespace for the immutable specification records that describe a Kafka consumer connection. */
public interface ConsumerSettings {

    /**
     * Immutable specification for a single Kafka consumer connection. Groups the parameters that
     * drive consumer creation, subscription/assignment, and the record-processing pipeline.
     *
     * @see RecordPipeline
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     * @param connectionName logical name of the connection (used for logging and monitoring)
     * @param consumerProperties Kafka consumer configuration properties
     * @param deserializerPair the {@link KafkaRecord.DeserializerPair} of key and value
     *     deserializers for raw Kafka records
     * @param consumerMode the {@link ConsumerMode} that selects group-based subscription vs manual
     *     partition assignment
     * @param recordConsumeFrom the {@link RecordConsumeFrom} that dictates the starting position of
     *     newly assigned partitions
     * @param pipeline the {@link RecordPipeline} that turns polled records into item updates
     */
    record ConnectionSpec<K, V>(
            String connectionName,
            Properties consumerProperties,
            KafkaRecord.DeserializerPair<K, V> deserializerPair,
            ConsumerMode consumerMode,
            RecordConsumeFrom recordConsumeFrom,
            RecordPipeline<K, V> pipeline) {

        /**
         * Returns whether the consumer uses manual partition assignment.
         *
         * @return {@code true} if in {@link ConsumerMode#MANUAL} mode, {@code false} otherwise
         */
        public boolean isManual() {
            return consumerMode == ConsumerMode.MANUAL;
        }
    }

    /**
     * Immutable specification for the record-processing pipeline of a Kafka connection. Groups the
     * routing, extraction, error-handling, command-mode and concurrency parameters used to turn
     * polled Kafka records into Lightstreamer item updates.
     *
     * @see ConnectionSpec
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     * @param itemTemplates {@link ItemTemplates} that map Kafka records to subscribable items
     * @param fieldsExtractor the {@link FieldsExtractor} that extracts field values from
     *     deserialized records
     * @param errorHandlingStrategy the {@link RecordErrorHandlingStrategy} that determines how
     *     deserialization or extraction errors are handled
     * @param processAsCommand {@code true} if records must be processed as COMMAND-mode updates,
     *     {@code false} otherwise
     * @param concurrency the {@link Concurrency} settings (thread count and ordering strategy) for
     *     record processing
     */
    record RecordPipeline<K, V>(
            ItemTemplates<K, V> itemTemplates,
            FieldsExtractor<K, V> fieldsExtractor,
            RecordErrorHandlingStrategy errorHandlingStrategy,
            boolean processAsCommand,
            Concurrency concurrency) {
        /**
         * Thread concurrency settings for record processing.
         *
         * @param orderStrategy the {@link RecordConsumeWithOrderStrategy} ordering guarantee when
         *     processing records in parallel
         * @param threads the number of threads dedicated to record processing, or {@code -1} to
         *     request auto-detection based on the number of available CPU cores
         */
        public record Concurrency(RecordConsumeWithOrderStrategy orderStrategy, int threads) {

            /**
             * Returns whether record processing runs in parallel — i.e., uses any {@code threads}
             * value other than {@code 1}. This includes the {@code -1} sentinel that requests
             * auto-detection based on the number of available CPU cores.
             *
             * @return {@code true} if parallel, {@code false} otherwise
             */
            public boolean isParallel() {
                return threads() != 1;
            }
        }
    }
}
