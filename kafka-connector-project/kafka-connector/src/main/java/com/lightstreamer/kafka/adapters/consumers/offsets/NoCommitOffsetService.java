
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

package com.lightstreamer.kafka.adapters.consumers.offsets;

import com.lightstreamer.kafka.common.records.KafkaRecord;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * A no-operation {@link OffsetService} that never commits offsets to Kafka.
 *
 * <p>Designed for the implicit item snapshot strategy where the consumer uses manual partition
 * assignment ({@code assign()} + {@code seekToBeginning()}) and always reads from the beginning on
 * startup. Since no consumer group is involved and resume semantics are not needed, offset tracking
 * and committing are unnecessary overhead.
 *
 * <p>All lifecycle methods are no-ops. The {@link
 * org.apache.kafka.clients.consumer.ConsumerRebalanceListener} callbacks log partition changes but
 * perform no commit operations — rebalance events do not occur with manual assignment, but the
 * implementation handles them gracefully in case of unexpected invocation.
 */
final class NoCommitOffsetService implements OffsetService {

    private final Logger logger;

    NoCommitOffsetService(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void maybeCommit() {
        // No-op: offsets are never committed
    }

    @Override
    public void updateOffsets(KafkaRecord<?, ?> record) {
        // No-op: offsets are not tracked
    }

    @Override
    public void onAsyncFailure(Throwable th) {
        logger.atWarn().setCause(th).log("Async failure reported to no-commit offset service");
    }

    @Override
    public void onConsumerShutdown() {
        logger.atDebug().log("Consumer shutdown — no offsets to commit");
    }

    @Override
    public Throwable getFirstFailure() {
        return null;
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> offsetsSnapshot() {
        return Collections.emptyMap();
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        logger.atDebug().log("Partitions assigned: {}", partitions);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.atDebug().log("Partitions revoked: {} — no offsets to commit", partitions);
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        logger.atDebug().log("Partitions lost: {}", partitions);
    }
}
