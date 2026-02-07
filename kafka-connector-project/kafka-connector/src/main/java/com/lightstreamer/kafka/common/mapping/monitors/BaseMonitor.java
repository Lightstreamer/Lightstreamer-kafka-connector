
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
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Abstract base class for monitors that perform periodic checks at configurable intervals.
 *
 * <p>This class implements the interval-based checking logic using compare-and-swap for thread
 * safety. Subclasses only need to implement {@link #performCheck(double)} to define the actual
 * monitoring behavior.
 *
 * <p><b>Thread Safety:</b> The {@link #check()} method is thread-safe and uses atomic operations to
 * ensure only one thread performs the check when the interval elapses.
 *
 * @see Monitor
 * @see Monitors
 * @see ThroughputMonitor
 * @see RingUtilizationMonitor
 */
abstract class BaseMonitor implements Monitor {

    private final AtomicLong lastCheck = new AtomicLong(System.nanoTime());
    private final long checkIntervalNs;

    /** Logger for subclass reporting, configured with a suffix appended to the parent logger. */
    protected final Logger logger;

    /**
     * Constructs a new {@code BaseMonitor} with the specified logger and check interval.
     *
     * @param logger the parent logger to derive the monitor's logger from
     * @param suffix the suffix to append to the logger name (e.g., "Performance")
     * @param checkInterval the minimum interval between checks
     */
    public BaseMonitor(Logger logger, String suffix, Duration checkInterval) {
        this.logger = LoggerFactory.getLogger(logger.getName() + suffix);
        this.checkIntervalNs = checkInterval.toNanos();
    }

    @Override
    public final Duration getCheckInterval() {
        return Duration.ofNanos(checkIntervalNs);
    }

    @Override
    public final void check() {
        long currentTime = System.nanoTime();
        long lastCheckTime = lastCheck.get();
        long timeWindowNs = currentTime - lastCheckTime;

        if (timeWindowNs > checkIntervalNs && lastCheck.compareAndSet(lastCheckTime, currentTime)) {
            double timeWindowSec = timeWindowNs / 1_000_000_000.0;
            performCheck(timeWindowSec);
        }
    }

    /**
     * Performs the actual monitoring check.
     *
     * <p>This method is called only when the configured interval has elapsed. Implementations
     * should collect and report their statistics.
     *
     * @param timeWindowSec the actual time elapsed since the last check, in seconds
     */
    protected abstract void performCheck(double timeWindowSec);
}
