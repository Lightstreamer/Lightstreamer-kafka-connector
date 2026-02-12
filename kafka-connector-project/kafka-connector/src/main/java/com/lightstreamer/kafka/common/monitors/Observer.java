
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

import java.time.Duration;

/**
 * Builder interface for configuring aggregate functions on a monitored meter.
 *
 * <p>Provides a fluent API for enabling functions (rate, average, max, etc.) that will be evaluated
 * and reported at the monitor's evaluation interval. Each observer is associated with a single
 * meter and computes functions over a configurable time range.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * monitor.observe(requestCounter)
 *        .enableRate()
 *        .enableLatest()
 *        .withRangeInterval(Duration.ofMinutes(5));
 * }</pre>
 */
public interface Observer {

    /**
     * Enables the latest value function for this observer.
     *
     * <p>Reports the most recent meter value at each evaluation interval.
     *
     * @return this observer for method chaining
     */
    Observer enableLatest();

    /**
     * Enables the rate function for this observer.
     *
     * <p>Computes the average rate of change over the configured range interval using all available
     * data points within the window.
     *
     * @return this observer for method chaining
     */
    Observer enableRate();

    /**
     * Enables the instant rate function for this observer.
     *
     * <p>Computes the instantaneous rate of change using only the last two data points, providing a
     * more responsive measure than {@link #enableRate()}.
     *
     * @return this observer for method chaining
     */
    Observer enableIrate();

    /**
     * Enables the average function for this observer.
     *
     * <p>Computes the arithmetic mean of all meter values within the configured range interval.
     *
     * @return this observer for method chaining
     */
    Observer enableAverage();

    /**
     * Enables the maximum function for this observer.
     *
     * <p>Reports the highest meter value observed within the configured range interval.
     *
     * @return this observer for method chaining
     */
    Observer enableMax();

    /**
     * Enables the minimum function for this observer.
     *
     * <p>Reports the lowest meter value observed within the configured range interval.
     *
     * @return this observer for method chaining
     */
    Observer enableMin();

    /**
     * Sets the time range for aggregate function evaluation.
     *
     * <p>Functions like rate, average, max, and min compute their results over data points within
     * this time window, looking back from the current evaluation timestamp. The default range
     * interval is typically 1 minute.
     *
     * @param rangeInterval the time window for function evaluation; must be positive
     * @return this observer for method chaining
     * @throws IllegalArgumentException if {@code rangeInterval} is null, zero, or negative
     */
    Observer withRangeInterval(Duration rangeInterval);
}
