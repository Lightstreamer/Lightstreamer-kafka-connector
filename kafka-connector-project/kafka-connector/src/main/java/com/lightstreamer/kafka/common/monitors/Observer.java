
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

import com.lightstreamer.kafka.common.monitors.reporting.Reporter.MetricValueFormatter;
import com.lightstreamer.kafka.common.monitors.reporting.Reporters;

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
     * @param precision the number of decimal places for formatting (0-9)
     * @return this observer for method chaining
     */
    Observer enableLatest(int precision, MetricValueFormatter formatter);

    default Observer enableLatest() {
        return enableLatest(0, Reporters.DEFAULT_FORMATTER);
    }

    /**
     * Enables the rate function for this observer.
     *
     * <p>Computes the average rate of change over the configured range interval using all available
     * data points within the window.
     *
     * @param precision the number of decimal places for formatting (0-9)
     * @return this observer for method chaining
     */
    Observer enableRate(int precision, MetricValueFormatter formatter);

    default Observer enableRate() {
        return enableRate(2, Reporters.DEFAULT_FORMATTER);
    }

    /**
     * Enables the instant rate function for this observer.
     *
     * <p>Computes the instantaneous rate of change using only the last two data points, providing a
     * more responsive measure than {@link #enableRate()}.
     *
     * @param precision the number of decimal places for formatting (0-9)
     * @return this observer for method chaining
     */
    Observer enableIrate(int precision, MetricValueFormatter formatter);

    default Observer enableIrate() {
        return enableIrate(2, Reporters.DEFAULT_FORMATTER);
    }

    /**
     * Enables the average function for this observer.
     *
     * <p>Computes the arithmetic mean of all meter values within the configured range interval.
     *
     * @param precision the number of decimal places for formatting (0-9)
     * @return this observer for method chaining
     */
    Observer enableAverage(int precision, MetricValueFormatter formatter);

    default Observer enableAverage() {
        return enableAverage(2, Reporters.DEFAULT_FORMATTER);
    }

    /**
     * Enables the maximum function for this observer.
     *
     * <p>Reports the highest meter value observed within the configured range interval.
     *
     * @param precision the number of decimal places for formatting (0-9)
     * @return this observer for method chaining
     */
    Observer enableMax(int precision, MetricValueFormatter formatter);

    default Observer enableMax() {
        return enableMax(2, Reporters.DEFAULT_FORMATTER);
    }

    /**
     * Enables the minimum function for this observer.
     *
     * <p>Reports the lowest meter value observed within the configured range interval.
     *
     * @param precision the number of decimal places for formatting (0-9)
     * @return this observer for method chaining
     */
    Observer enableMin(int precision, MetricValueFormatter formatter);

    default Observer enableMin() {
        return enableMin(2, Reporters.DEFAULT_FORMATTER);
    }

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
