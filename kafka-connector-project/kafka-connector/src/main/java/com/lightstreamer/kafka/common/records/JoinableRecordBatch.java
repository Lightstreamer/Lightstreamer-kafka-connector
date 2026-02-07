
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

import java.util.concurrent.CountDownLatch;

/**
 * {@link RecordBatch} implementation with synchronous join support.
 *
 * <p>This implementation extends {@link NotifyingRecordBatch} and adds a {@link CountDownLatch} to
 * enable synchronous waiting via the {@link #join()} method. It is used when processing must be
 * coordinated between threads, such as when batching offset commits.
 *
 * <p><b>Use case:</b> Scenarios requiring synchronous coordination, such as:
 *
 * <ul>
 *   <li>Committing offsets only after all records in a batch are processed
 *   <li>Collecting results from worker threads before proceeding
 *   <li>Enforcing ordering between poll cycles
 * </ul>
 *
 * <p><b>Thread Safety:</b> Safe for concurrent calls from multiple worker threads. The {@link
 * #join()} method blocks until all records have been processed.
 *
 * <p><b>Performance:</b> Higher synchronization overhead than {@link NotifyingRecordBatch} due to
 * the additional {@link CountDownLatch} and coordination. Use {@link NotifyingRecordBatch} with
 * callbacks for maximum throughput scenarios.
 *
 * @param <K> the type of the record key
 * @param <V> the type of the record value
 * @see NotifyingRecordBatch
 */
public class JoinableRecordBatch<K, V> extends NotifyingRecordBatch<K, V> {

    private final CountDownLatch latch;

    /**
     * Constructs a new {@code JoinableRecordBatch} with the specified capacity.
     *
     * <p>A {@link CountDownLatch} is initialized with the record count to track completion.
     *
     * @param recordCount the expected number of records in this batch
     */
    public JoinableRecordBatch(int recordCount) {
        super(recordCount);
        this.latch = new CountDownLatch(recordCount);
    }

    @Override
    public void recordProcessed(RecordBatchListener listener) {
        latch.countDown();
        super.recordProcessed(listener);
    }

    @Override
    public void join() {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                    "Interrupted while waiting for records batch to complete", e);
        }
    }
}
