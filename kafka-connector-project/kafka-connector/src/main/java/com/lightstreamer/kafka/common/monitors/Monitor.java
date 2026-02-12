
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

package com.lightstreamer.kafka.common.monitors;

import com.lightstreamer.kafka.common.monitors.metrics.Meter;

import java.time.Duration;

/**
 * A monitoring system that observes metrics over time and evaluates aggregate functions at regular
 * intervals.
 *
 * <p>Monitors follow a strict lifecycle: observers must be registered via {@link #observe(Meter)}
 * before calling {@link #start(Duration)}, after which no new observers can be added. The monitor
 * periodically scrapes meter values and evaluates configured aggregate functions (latest, rate,
 * average, etc.), reporting results through the configured {@code Reporter}.
 *
 * <p>Thread Safety: Implementations must be thread-safe and support concurrent access to all
 * methods. However, the lifecycle progression (observe → start → stop) should be coordinated by the
 * caller to ensure predictable behavior.
 *
 * <p>Typical usage:
 *
 * <pre>{@code
 * Monitor monitor = ...
 * monitor.observe(counter).enableRate().enableLatest();
 * monitor.observe(gauge).enableAverage().enableMax();
 * monitor.start(Duration.ofSeconds(1)); // Evaluate every second
 * // ... application runs ...
 * monitor.stop();
 * }</pre>
 */
public interface Monitor {

    /**
     * Creates an observer for the specified meter and returns a builder for configuring aggregate
     * functions.
     *
     * <p>The returned {@link Observer} provides a fluent API for enabling functions like {@code
     * enableRate()}, {@code enableLatest()}, {@code enableAverage()}, etc. The observer will
     * collect meter samples at the monitor's scrape interval and evaluate enabled functions at the
     * evaluation interval.
     *
     * <p>This method can only be called before {@link #start(Duration)} is invoked. Attempting to
     * add observers after starting will result in an {@code IllegalStateException}.
     *
     * @param meter the meter to observe; must not be {@code null}
     * @return an observer builder for configuring aggregate functions
     * @throws IllegalArgumentException if {@code meter} is {@code null}
     * @throws IllegalStateException if the monitor has already been started
     */
    Observer observe(Meter meter);

    /**
     * Starts the monitoring system with the specified evaluation interval.
     *
     * <p>Once started, the monitor will:
     *
     * <ul>
     *   <li>Scrape all registered meters at the configured scrape interval (typically more frequent
     *       than the evaluation interval)
     *   <li>Evaluate enabled aggregate functions and report results at the specified {@code
     *       evaluationInterval}
     *   <li>Block any further calls to {@link #observe(Meter)}
     * </ul>
     *
     * <p>The evaluation interval should typically be equal to or a multiple of the scrape interval.
     * Calling this method multiple times has no effect if the monitor is already running.
     *
     * @param evaluationInterval the interval at which to evaluate and report aggregate functions;
     *     must be positive and non-null
     * @throws IllegalArgumentException if {@code evaluationInterval} is {@code null}, zero, or
     *     negative
     */
    void start(Duration evaluationInterval);

    /**
     * Stops the monitoring system and releases associated resources.
     *
     * <p>This method performs a graceful shutdown, waiting for any in-progress scraping or
     * evaluation tasks to complete. Calling this method when the monitor is not running has no
     * effect. After stopping, the monitor can be restarted with {@link #start(Duration)}.
     */
    void stop();

    /**
     * Returns whether this monitor has been started and is currently running.
     *
     * @return {@code true} if the monitor is running, {@code false} otherwise
     */
    boolean isRunning();
}
