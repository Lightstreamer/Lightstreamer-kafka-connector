
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

package com.lightstreamer.kafka.common.monitors.reporting;

import java.util.List;

/**
 * Interface for reporting monitoring metrics.
 *
 * <p>Defines the contract for delivering monitoring information to various destinations such as
 * logs, consoles, or external monitoring systems. Implementations determine the output format and
 * delivery mechanism.
 *
 * <p><b>Thread Safety:</b> Implementations must be thread-safe as reporters are typically shared
 * across multiple threads in monitoring contexts (e.g., scheduled executors, concurrent metric
 * collection). The standard implementations in {@link Reporters} are thread-safe.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * Reporter reporter = Reporters.logReporter(logger);
 * reporter.report("requests [5m]", List.of(
 *     new MetricValue("avg", 1500.0, 2, "requests/sec"),
 *     new MetricValue("max", 2300.0, 2, "requests/sec")
 * ));
 * }</pre>
 */
public interface Reporter {

    /**
     * A metric value with formatting metadata.
     *
     * <p>Encapsulates all information needed to present a metric: name, computed value, display
     * precision, unit of measurement, and formatting strategy. The formatter can be customized
     * per-metric to enable different presentation styles.
     *
     * @param name the metric name (e.g., "avg", "rate", "max")
     * @param value the computed metric value (may be {@code NaN} if unavailable)
     * @param precision the number of decimal places for display (0-9)
     * @param unit the unit of measurement with decorators (e.g., "requests/sec", "bytes")
     * @param formatter the formatting strategy for this metric
     */
    record MetricValue(
            String name, double value, int precision, String unit, MetricValueFormatter formatter) {

        public MetricValue {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("Metric name cannot be null or empty");
            }
            if (unit == null || unit.isEmpty()) {
                throw new IllegalArgumentException("Metric unit cannot be null or empty");
            }
            if (formatter == null) {
                throw new IllegalArgumentException("Metric formatter cannot be null");
            }
            if (precision < 0 || precision > 9) {
                throw new IllegalArgumentException("Precision must be between 0 and 9");
            }
        }

        /**
         * Formats this metric value for display.
         *
         * @return formatted string such as "avg=1.5k requests/sec"
         */
        public String format() {
            return formatter.formatMetric(this);
        }
    }

    /**
     * Strategy for formatting metric values.
     *
     * <p>Implementations define how metric values are converted to human-readable strings, enabling
     * customization such as SI unit scaling, color-coding, threshold highlighting, or
     * locale-specific number formatting.
     */
    interface MetricValueFormatter {

        /**
         * Formats a metric value for display.
         *
         * @param metric the metric value to format
         * @return formatted string such as "avg=1.5k requests/sec"
         */
        String formatMetric(MetricValue metric);
    }

    /**
     * Reports a collection of metric values with contextual description.
     *
     * <p>Metrics typically originate from the same data source (e.g., a time series) and represent
     * different aggregate functions applied to that data. The description provides context about
     * the source and evaluation window.
     *
     * @param description contextual label for the metrics (e.g., "requests [5m]")
     * @param metrics the metric values to report
     */
    void report(String description, List<MetricValue> metrics);
}
