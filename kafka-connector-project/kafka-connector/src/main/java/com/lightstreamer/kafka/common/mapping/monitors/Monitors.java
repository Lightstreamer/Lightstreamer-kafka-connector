
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
import java.util.concurrent.BlockingQueue;

/**
 * Factory class for creating monitor instances.
 *
 * <p>This class provides static factory methods for creating different types of monitors used in
 * the record processing pipeline. It encapsulates the instantiation of concrete monitor
 * implementations.
 *
 * @see ProcessingMonitor
 * @see Monitor
 */
public class Monitors {

    /**
     * Creates a new processing monitor for tracking batch processing throughput.
     *
     * @param logger the logger for reporting statistics
     * @param checkInterval the interval between statistics reports
     * @return a new {@link ProcessingMonitor} instance
     */
    public static ProcessingMonitor createProcessingMonitor(Logger logger, Duration checkInterval) {
        return new ThroughputMonitor(logger, checkInterval);
    }

    /**
     * Creates a new ring buffer utilization monitor.
     *
     * @param logger the logger for reporting utilization statistics
     * @param ringBuffers the array of ring buffers to monitor
     * @param ringBufferCapacity the capacity of each ring buffer
     * @param checkInterval the interval between utilization reports
     * @return a new {@link Monitor} instance for ring buffer utilization
     */
    public static Monitor createRingUtilizationMonitor(
            Logger logger,
            BlockingQueue<?>[] ringBuffers,
            int ringBufferCapacity,
            Duration checkInterval) {
        return new RingUtilizationMonitor(logger, ringBuffers, ringBufferCapacity, checkInterval);
    }
}
