
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

import com.lightstreamer.kafka.common.mapping.selectors.ValueException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

import java.util.Map;

public interface OffsetService extends ConsumerRebalanceListener {

    @FunctionalInterface
    interface OffsetStoreSupplier {

        OffsetStore newOffsetStore(Map<TopicPartition, OffsetAndMetadata> offsets);
    }

    default void initStore(boolean fromLatest) {
        initStore(fromLatest, Offsets.OffsetStoreImpl::new);
    }

    void initStore(boolean flag, OffsetStoreSupplier storeSupplier);

    boolean isNotAlreadyConsumed(ConsumerRecord<?, ?> record);

    void commitSync();

    void commitAsync();

    void updateOffsets(ConsumerRecord<?, ?> record);

    void onAsyncFailure(ValueException ve);

    ValueException getFirstFailure();

    static OffsetService newOffsetService(Consumer<?, ?> consumer, Logger log) {
        return new Offsets.OffsetServiceImpl(consumer, log);
    }

    static OffsetService newOffsetService(Consumer<?, ?> consumer) {
        return new Offsets.OffsetServiceImpl(consumer);
    }
}
