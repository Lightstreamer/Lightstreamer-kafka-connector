
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

package com.lightstreamer.kafka.adapters.consumers.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Centralized performance monitoring with sliding window measurement. Provides accurate throughput
 * statistics without synchronization overhead.
 */
class PerformanceMonitor {

    private static final String LOGGER_SUFFIX = "Performance";

    private final String monitorName;
    private final AtomicLong totalProcessedRecords = new AtomicLong(0);

    private final Logger statsLogger;
    private final Logger ringBuffersUtilizationLogger;
    private final AtomicLong lastCheck = new AtomicLong(System.nanoTime());
    private final AtomicLong lastReportedCount = new AtomicLong(0);

    // Ring buffer utilization monitoring
    private volatile long lastUtilizationCheck = System.nanoTime();
    private static final long UTILIZATION_CHECK_INTERVAL_NS =
            2_000_000_000L; // Check every 2 seconds
    private volatile int[] peakUtilizationSinceLastReport; // Track peak utilization per buffer

    PerformanceMonitor(String monitorName, Logger logger) {
        this.monitorName = monitorName;
        this.statsLogger = LoggerFactory.getLogger(logger.getName() + LOGGER_SUFFIX);
        this.ringBuffersUtilizationLogger =
                LoggerFactory.getLogger(logger.getName() + "RingBuffersUtilization");
    }

    void count(int records) {
        totalProcessedRecords.addAndGet(records);
    }

    /** Check and potentially log performance stats every 5 seconds using sliding window */
    void checkStats() {
        long currentTime = System.nanoTime();
        long lastCheckTime = lastCheck.get();

        // Report stats every 5 seconds
        if (currentTime - lastCheckTime > 5_000_000_000L
                && lastCheck.compareAndSet(lastCheckTime, currentTime)) {
            long currentTotal = totalProcessedRecords.get();
            long lastTotal = lastReportedCount.getAndSet(currentTotal);

            long recordsInWindow = currentTotal - lastTotal;
            long timeWindowMs = currentTime - lastCheckTime;

            if (recordsInWindow > 0 && timeWindowMs > 0) {
                long avgThroughput = (recordsInWindow * 1000L) / timeWindowMs;
                statsLogger.atInfo().log(
                        "{} processing stats: {} records processed in {}ms window, avg {}k records/sec",
                        monitorName,
                        recordsInWindow,
                        timeWindowMs,
                        avgThroughput / 1000.0);
            }
        }
    }

    /**
     * Monitor and graphically display ring buffer utilization rates. Provides visual representation
     * of buffer load across all threads.
     */
    void checkRingBufferUtilization(BlockingQueue<?>[] ringBuffers, int ringBufferCapacity) {
        // Initialize peak tracking if not already done
        if (peakUtilizationSinceLastReport == null) {
            peakUtilizationSinceLastReport = new int[ringBuffers.length];
        }

        // Update peak utilization continuously
        for (int i = 0; i < ringBuffers.length; i++) {
            int currentUtilization = (ringBuffers[i].size() * 100) / ringBufferCapacity;
            peakUtilizationSinceLastReport[i] =
                    Math.max(peakUtilizationSinceLastReport[i], currentUtilization);
        }

        long currentTime = System.nanoTime();
        if (currentTime - lastUtilizationCheck < UTILIZATION_CHECK_INTERVAL_NS) {
            return;
        }
        lastUtilizationCheck = currentTime;

        int actualThreads = ringBuffers.length;
        StringBuilder utilizationReport = new StringBuilder();
        utilizationReport.append("\n┌─ Ring Buffer Utilization Report (2s interval) ─┐\n");

        int totalUsed = 0;
        int totalCapacity = ringBufferCapacity * actualThreads;
        int totalPeakUsed = 0;

        for (int i = 0; i < actualThreads; i++) {
            int currentSize = ringBuffers[i].size();
            int utilization = (currentSize * 100) / ringBufferCapacity;
            int peakUtilization = peakUtilizationSinceLastReport[i];
            totalUsed += currentSize;
            totalPeakUsed += (peakUtilization * ringBufferCapacity) / 100;

            // Create visual bars (20 chars wide)
            String currentBar = createUtilizationBar(utilization, 20);
            String peakBar = createUtilizationBar(peakUtilization, 20);

            utilizationReport.append(
                    String.format(
                            "│ Buf[%2d]: Now[%s] %3d%% Peak[%s] %3d%% (%d/%d)\n",
                            i,
                            currentBar,
                            utilization,
                            peakBar,
                            peakUtilization,
                            currentSize,
                            ringBufferCapacity));
        }

        // Overall utilization
        int overallUtilization = (totalUsed * 100) / totalCapacity;
        int overallPeakUtilization = (totalPeakUsed * 100) / totalCapacity;
        String overallBar = createUtilizationBar(overallUtilization, 25);
        String overallPeakBar = createUtilizationBar(overallPeakUtilization, 25);

        utilizationReport.append("├─────────────────────────────────────────────────┤\n");
        utilizationReport.append(
                String.format(
                        "│ Overall Now: [%s] %3d%% (%d/%d)\n",
                        overallBar, overallUtilization, totalUsed, totalCapacity));
        utilizationReport.append(
                String.format(
                        "│ Overall Peak:[%s] %3d%% (%d/%d)\n",
                        overallPeakBar, overallPeakUtilization, totalPeakUsed, totalCapacity));
        utilizationReport.append("└─────────────────────────────────────────────────┘");

        // Log based on peak utilization (more meaningful than current)
        if (overallPeakUtilization > 80) {
            statsLogger.atInfo().log(
                    "HIGH peak ring buffer utilization detected:{}", utilizationReport);
        } else if (overallPeakUtilization > 40 || overallUtilization > 20) {
            statsLogger.atInfo().log("Ring buffer utilization status:{}", utilizationReport);
        } else {
            statsLogger.atInfo().log(
                    "Ring buffer utilization status (low activity):{}", utilizationReport);
        }

        // Reset peak tracking for next interval
        Arrays.fill(peakUtilizationSinceLastReport, 0);
    }

    /**
     * Create a visual utilization bar with color indicators.
     *
     * @param utilizationPercent The utilization percentage (0-100)
     * @param width The width of the bar in characters
     * @return A string representing the visual bar
     */
    private String createUtilizationBar(int utilizationPercent, int width) {
        int filledChars = (utilizationPercent * width) / 100;
        StringBuilder bar = new StringBuilder();

        // Different characters for different utilization levels
        char indicator;
        if (utilizationPercent > 90) {
            indicator = '█'; // High utilization - solid block
        } else if (utilizationPercent > 70) {
            indicator = '▓'; // Medium-high utilization - dark shade
        } else if (utilizationPercent > 40) {
            indicator = '▒'; // Medium utilization - medium shade
        } else {
            indicator = '░'; // Low utilization - light shade
        }

        for (int i = 0; i < width; i++) {
            if (i < filledChars) {
                bar.append(indicator); // High utilization - solid block
            } else {
                bar.append('·'); // Empty space
            }
        }

        return bar.toString();
    }
}
