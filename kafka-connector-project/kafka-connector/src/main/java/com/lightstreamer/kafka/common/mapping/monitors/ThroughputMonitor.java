
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

package com.lightstreamer.kafka.common.mapping.monitors;

import com.lightstreamer.kafka.common.records.RecordBatch;

import org.slf4j.Logger;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Monitor for tracking record processing throughput and lag.
 *
 * <p>This monitor tracks both received and processed record counts, computing throughput rates and
 * current lag (received - processed) at configurable intervals. Statistics are reported via the
 * configured logger.
 *
 * <p><b>Thread Safety:</b> All counters use {@link AtomicLong} for thread-safe updates from
 * multiple worker threads.
 *
 * @see ProcessingMonitor
 * @see BaseMonitor
 */
public class ThroughputMonitor extends BaseMonitor implements ProcessingMonitor {

    private static final String LOGGER_SUFFIX = "Performance";

    private final AtomicLong totalProcessedRecords = new AtomicLong(0);
    private final AtomicLong totalReceivedRecords = new AtomicLong(0);

    private final AtomicLong lastProcessedCount = new AtomicLong(0);
    private final AtomicLong lastReceivedCount = new AtomicLong(0);

    /**
     * Constructs a new {@code ThroughputMonitor} with the specified logger and check interval.
     *
     * @param logger the logger for reporting throughput statistics
     * @param checkInterval the interval between throughput reports
     */
    public ThroughputMonitor(Logger logger, Duration checkInterval) {
        super(logger, LOGGER_SUFFIX, checkInterval);
    }

    /**
     * Records the number of records received from Kafka.
     *
     * <p>This method should be called after each poll to track incoming record counts.
     *
     * @param records the number of records received
     */
    public void countReceived(int records) {
        totalReceivedRecords.addAndGet(records);
    }

    @Override
    protected void performCheck(double timeWindowSec) {
        long currentTotalReceived = totalReceivedRecords.get();
        long lastTotalReceived = lastReceivedCount.getAndSet(currentTotalReceived);

        long currentTotalProcessed = totalProcessedRecords.get();
        long lastTotalProcessed = lastProcessedCount.getAndSet(currentTotalProcessed);

        long processedRecordsInWindow = currentTotalProcessed - lastTotalProcessed;
        long receivedRecordsInWindow = currentTotalReceived - lastTotalReceived;
        long currentLag = currentTotalReceived - currentTotalProcessed;

        if (timeWindowSec > 0) {
            double recvThroughput = receivedRecordsInWindow / timeWindowSec / 1000.0;
            double avgThroughput = processedRecordsInWindow / timeWindowSec / 1000.0;
            String message =
                    String.format(
                            "Incoming avg=%.1fk msg/sec, Processed avg=%.1fk msg/sec, Lag=%d msgs",
                            recvThroughput, avgThroughput, currentLag);
            logger.info(message);
        }
    }

    @Override
    public void onBatchComplete(RecordBatch<?, ?> batch) {
        totalProcessedRecords.addAndGet(batch.count());
    }
}
