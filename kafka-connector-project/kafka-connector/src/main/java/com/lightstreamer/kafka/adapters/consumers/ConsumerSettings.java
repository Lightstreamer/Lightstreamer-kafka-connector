
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

import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandModeStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.selectors.FieldsExtractor;
import com.lightstreamer.kafka.common.records.KafkaRecord;

import java.util.Properties;

/** Holds the configuration records that define how a Kafka consumer connection is set up. */
public interface ConsumerSettings {

    /**
     * Immutable specification for a single Kafka consumer connection. Groups all parameters that
     * drive consumer creation, record processing, snapshot delivery, and subscription management.
     *
     * @param <K> the deserialized key type
     * @param <V> the deserialized value type
     * @param connectionName logical name of the connection (used for logging)
     * @param consumerProperties Kafka consumer configuration properties
     * @param itemTemplates templates that map Kafka records to subscribable items
     * @param fieldsExtractor extracts field values from deserialized records
     * @param deserializerPair key and value deserializers for raw Kafka records
     * @param errorHandlingStrategy how deserialization or extraction errors are handled
     * @param commandModeStrategy COMMAND mode behavior ({@code ENFORCE}, {@code AUTO}, or none)
     * @param concurrency thread count and ordering strategy for record processing
     */
    record ConnectionSpec<K, V>(
            String connectionName,
            Properties consumerProperties,
            ItemTemplates<K, V> itemTemplates,
            FieldsExtractor<K, V> fieldsExtractor,
            KafkaRecord.DeserializerPair<K, V> deserializerPair,
            RecordErrorHandlingStrategy errorHandlingStrategy,
            CommandModeStrategy commandModeStrategy,
            Concurrency concurrency) {}

    /**
     * Thread concurrency settings for record processing.
     *
     * @param orderStrategy the ordering guarantee when processing records in parallel
     * @param threads the number of threads dedicated to record processing
     */
    record Concurrency(RecordConsumeWithOrderStrategy orderStrategy, int threads) {

        /**
         * Returns whether record processing uses more than one thread.
         *
         * @return {@code true} if parallel, {@code false} otherwise
         */
        public boolean isParallel() {
            return threads() != 1;
        }
    }
}
