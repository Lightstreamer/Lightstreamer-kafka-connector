
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

package com.lightstreamer.kafka.adapters.consumers.snapshot;

import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandModeStrategy;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.records.KafkaRecord;

/**
 * Projection of connection settings required by {@link SnapshotDeliveryStrategy} implementations.
 * Decouples the snapshot package from the parent consumer package by defining only the accessors
 * that snapshot strategies actually use.
 *
 * @param <K> the deserialized key type
 * @param <V> the deserialized value type
 */
public interface SnapshotConnectionSpec<K, V> {

    String connectionName();

    ItemTemplates<K, V> itemTemplates();

    KafkaRecord.DeserializerPair<K, V> deserializerPair();

    CommandModeStrategy commandModeStrategy();
}
