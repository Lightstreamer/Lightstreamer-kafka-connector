
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

import org.slf4j.Logger;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;

/**
 * Monitor for tracking ring buffer utilization in parallel processing.
 *
 * <p>This monitor provides visual reporting of ring buffer fill levels, tracking both current and
 * peak utilization since the last report. Utilization is displayed as ASCII bar charts for easy
 * interpretation.
 *
 * <p><b>Utilization Levels:</b>
 *
 * <ul>
 *   <li>{@code █} - High utilization (>90%)
 *   <li>{@code ▓} - Medium-high utilization (>70%)
 *   <li>{@code ▒} - Medium utilization (>40%)
 *   <li>{@code ░} - Low utilization (≤40%)
 * </ul>
 *
 * <p><b>Thread Safety:</b> Safe for concurrent access. Peak utilization tracking uses volatile
 * semantics.
 *
 * @see BaseMonitor
 * @see Monitors#createRingUtilizationMonitor
 */
class RingUtilizationMonitor extends BaseMonitor {

    private static final String LOGGER_SUFFIX = "RingUtilization";

    // Ring buffer utilization monitoring
    private final BlockingQueue<?>[] ringBuffers;
    // Assuming all ring buffers have the same capacity
    private final int ringBufferCapacity;
    // Track peak utilization since last report to avoid logging transient spikes
    private volatile int[] peakUtilizationSinceLastReport;

    /**
     * Constructs a new {@code RingUtilizationMonitor} for the specified ring buffers.
     *
     * @param logger the logger for reporting utilization statistics
     * @param ringBuffers the array of ring buffers to monitor
     * @param ringBufferCapacity the capacity of each ring buffer (assumed uniform)
     * @param checkInterval the interval between utilization reports
     */
    RingUtilizationMonitor(
            Logger logger,
            BlockingQueue<?>[] ringBuffers,
            int ringBufferCapacity,
            Duration checkInterval) {
        super(logger, LOGGER_SUFFIX, checkInterval);
        this.ringBuffers = ringBuffers;
        this.ringBufferCapacity = ringBufferCapacity;
        this.peakUtilizationSinceLastReport = new int[ringBuffers.length];
    }

    @Override
    protected void performCheck(double timeWindowSec) {
        // Update peak utilization continuously
        for (int i = 0; i < ringBuffers.length; i++) {
            int currentUtilization = (ringBuffers[i].size() * 100) / ringBufferCapacity;
            peakUtilizationSinceLastReport[i] =
                    Math.max(peakUtilizationSinceLastReport[i], currentUtilization);
        }

        int numOfRingBuffers = ringBuffers.length;
        StringBuilder utilizationReport = new StringBuilder();
        utilizationReport.append(
                String.format(
                        "\n┌─ Ring Buffer Utilization Report (%.1fs interval) ─┐\n",
                        timeWindowSec));

        int totalUsed = 0;
        int totalCapacity = ringBufferCapacity * numOfRingBuffers;
        int totalPeakUsed = 0;

        for (int i = 0; i < numOfRingBuffers; i++) {
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
            logger.atInfo().log("HIGH peak ring buffer utilization detected:{}", utilizationReport);
        } else if (overallPeakUtilization > 40 || overallUtilization > 20) {
            logger.atInfo().log("Ring buffer utilization status:{}", utilizationReport);
        } else {
            logger.atInfo().log(
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
