
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

import com.lightstreamer.kafka.common.monitors.reporting.Reporter.MetricValue;

import org.slf4j.Logger;

import java.util.List;

/**
 * Factory for standard {@link Reporter} implementations and formatters.
 *
 * <p>Provides ready-to-use reporter implementations for common destinations (logs, console) and a
 * default metric formatter with SI unit scaling.
 *
 * <p><b>Thread Safety:</b> All reporter implementations and formatters provided by this factory are
 * thread-safe and can be safely shared across multiple threads. The {@link #DEFAULT_FORMATTER} is a
 * stateless singleton safe for concurrent use.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * Reporter reporter = Reporters.logReporter(logger);
 * // or
 * Reporter reporter = Reporters.consoleReporter();
 * }</pre>
 */
public class Reporters {

    private Reporters() {
        // Prevent instantiation
    }

    /**
     * Default formatter applying SI scaling (k, M, G) for readability.
     *
     * <p>Formats numbers with appropriate scale factors:
     *
     * <ul>
     *   <li>Values ≥ 1B: scaled to billions (G)
     *   <li>Values ≥ 1M: scaled to millions (M)
     *   <li>Values ≥ 1k: scaled to thousands (k)
     *   <li>Values &lt; 1k: displayed as-is
     * </ul>
     */
    public static final Reporter.MetricValueFormatter DEFAULT_FORMATTER =
            DefaultMetricValueFormatter.getInstance();

    /**
     * Creates a reporter that outputs to an SLF4J logger.
     *
     * <p>Messages are logged at INFO level. Useful for production monitoring where metrics should
     * be captured in application logs.
     *
     * @param logger the SLF4J logger to use for output
     * @return a log-based reporter
     */
    public static Reporter logReporter(Logger logger) {
        return new LogReporter(logger);
    }

    /**
     * Creates a reporter that outputs to standard output.
     *
     * <p>Messages are printed to {@code System.out}. Useful for development, debugging, or
     * command-line applications.
     *
     * @return a console-based reporter
     */
    public static Reporter consoleReporter() {
        return new ConsoleReporter();
    }

    /**
     * Default implementation of {@link Reporter.MetricValueFormatter} with SI unit scaling.
     *
     * <p>Formats metric values by scaling large numbers with SI prefixes (k, M, G) and handling
     * special cases like NaN values. Subclasses can override formatting behavior for custom
     * presentation needs.
     *
     * <p><b>Thread Safety:</b> This class is stateless and thread-safe. The singleton instance can
     * be safely shared across multiple threads.
     */
    public static class DefaultMetricValueFormatter implements Reporter.MetricValueFormatter {

        private static final DefaultMetricValueFormatter INSTANCE =
                new DefaultMetricValueFormatter();

        /**
         * Returns the singleton instance of the default formatter.
         *
         * @return the shared default formatter instance
         */
        public static DefaultMetricValueFormatter getInstance() {
            return INSTANCE;
        }

        /**
         * Formats a metric value with appropriate number scaling.
         *
         * <p>Output format: {@code name=value unit} (e.g., "avg=1.5k requests/sec"). NaN values are
         * formatted as "n/a".
         *
         * @param metric the metric value to format
         * @return formatted string such as "avg=1.5k requests/sec" or "rate=n/a requests/sec"
         */
        @Override
        public String formatMetric(MetricValue metric) {
            double value = metric.value();

            if (Double.isNaN(value)) {
                return metric.name() + "=n/a " + metric.unit();
            }

            String formatted = formatNumber(value, metric.precision());
            return metric.name() + "=" + formatted + " " + metric.unit();
        }

        /**
         * Formats a number with SI scaling for readability.
         *
         * <p>Applies scale factors based on magnitude: G (billions), M (millions), k (thousands),
         * or no scaling for values under 1000.
         *
         * @param value the number to format
         * @param precision the number of decimal places (0-9)
         * @return scaled number with SI suffix, such as "1.5k", "2.3M", or "1.2G"
         */
        protected String formatNumber(double value, int precision) {
            String format = "%." + precision + "f%s";

            if (value >= 1_000_000_000.0) {
                return String.format(format, value / 1_000_000_000.0, "G");
            } else if (value >= 1_000_000.0) {
                return String.format(format, value / 1_000_000.0, "M");
            } else if (value >= 1_000.0) {
                return String.format(format, value / 1_000.0, "k");
            } else {
                return String.format(format, value, "");
            }
        }
    }

    /**
     * Base implementation for reporters that format metrics as text messages.
     *
     * <p>Uses the Template Method pattern to allow subclasses to customize formatting at various
     * levels: complete message layout, description prefix, metrics separator, or individual metric
     * formatting. By default, builds messages in the format: {@code description: metric1, metric2,
     * ...} and delegates output to {@link #reportMessage(String)}.
     *
     * <p>Subclasses can override specific formatting hooks without reimplementing the entire
     * reporting logic:
     *
     * <ul>
     *   <li>{@link #formatMessage(String, List)} - complete message structure
     *   <li>{@link #formatDescription(String)} - description prefix/suffix
     *   <li>{@link #formatMetrics(List)} - metrics list layout
     *   <li>{@link #getMetricSeparator()} - separator between metrics
     *   <li>{@link #formatSingleMetric(MetricValue)} - individual metric formatting
     * </ul>
     *
     * <p><b>Thread Safety:</b> This base class maintains no mutable state and is thread-safe.
     * Subclasses must ensure their {@link #reportMessage(String)} implementation is also
     * thread-safe if the reporter will be used concurrently.
     */
    abstract static class AbstractReporter implements Reporter {

        @Override
        public void report(String reportDescription, List<MetricValue> metrics) {
            if (reportDescription == null || reportDescription.isEmpty()) {
                throw new IllegalArgumentException("Report description cannot be null or empty");
            }
            if (metrics == null || metrics.isEmpty()) {
                return;
            }

            try {
                String formattedMessage = formatMessage(reportDescription, metrics);
                reportMessage(formattedMessage);
            } catch (Exception e) {
                handleFormattingError(reportDescription, metrics, e);
            }
        }

        /**
         * Handles errors that occur during message formatting or reporting.
         *
         * <p>Default implementation logs the error and re-throws as RuntimeException. Subclasses
         * can override to implement custom error handling (e.g., fallback formatting, error
         * metrics).
         *
         * @param description the report description
         * @param metrics the metrics being reported
         * @param error the exception that occurred
         * @throws RuntimeException wrapping the original error
         */
        protected void handleFormattingError(
                String description, List<MetricValue> metrics, Exception error) {
            String errorMsg =
                    String.format(
                            "Failed to format/report metrics for '%s' with %d metric(s)",
                            description, metrics.size());
            throw new RuntimeException(errorMsg, error);
        }

        /**
         * Formats the complete report message. Subclasses can override for custom message
         * structure.
         *
         * <p>Default implementation combines the formatted description and metrics list.
         *
         * @param description the report description
         * @param metrics the metrics to include in the report
         * @return the complete formatted message
         */
        protected String formatMessage(String description, List<MetricValue> metrics) {
            StringBuilder sb = new StringBuilder();
            sb.append(formatDescription(description));
            sb.append(formatMetrics(metrics));
            return sb.toString();
        }

        /**
         * Formats the description prefix. Override to add timestamps, labels, or styling.
         *
         * <p>Default implementation appends a colon and space: {@code "description: "}
         *
         * @param description the raw description text
         * @return formatted description with any prefix, suffix, or styling
         */
        protected String formatDescription(String description) {
            return description + ": ";
        }

        /**
         * Formats the metrics list. Override to customize layout (e.g., multi-line, tabular).
         *
         * <p>Default implementation joins metrics with the separator from {@link
         * #getMetricSeparator()}.
         *
         * @param metrics the metrics to format
         * @return formatted metrics as a single string
         */
        protected String formatMetrics(List<MetricValue> metrics) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < metrics.size(); i++) {
                if (i > 0) {
                    sb.append(getMetricSeparator());
                }
                sb.append(formatSingleMetric(metrics.get(i)));
            }
            return sb.toString();
        }

        /**
         * Returns the separator between metrics. Override for custom separators (e.g., newlines,
         * tabs).
         *
         * <p>Default: {@code ", "}
         *
         * @return the separator string
         */
        protected String getMetricSeparator() {
            return ", ";
        }

        /**
         * Formats a single metric. Override for custom per-metric handling (e.g., threshold
         * highlighting, conditional formatting).
         *
         * <p>Default implementation delegates to {@link MetricValue#format()}. If the metric is
         * null, returns a placeholder string to prevent reporting failure.
         *
         * @param metric the metric to format
         * @return formatted metric string, or placeholder if metric is null
         */
        protected String formatSingleMetric(MetricValue metric) {
            if (metric == null) {
                return "<null>";
            }
            try {
                return metric.format();
            } catch (Exception e) {
                return metric.name() + "=<error>";
            }
        }

        /**
         * Outputs the formatted message to the destination.
         *
         * <p>Subclasses must implement this to define where messages are sent (logs, console, file,
         * network, etc.).
         *
         * @param message the complete formatted message to output
         */
        abstract void reportMessage(String message);
    }

    /**
     * Reporter implementation that outputs to an SLF4J logger.
     *
     * <p>Messages are logged at INFO level using the configured logger instance.
     *
     * <p><b>Thread Safety:</b> This implementation is thread-safe as SLF4J loggers are inherently
     * thread-safe.
     */
    public static class LogReporter extends AbstractReporter {

        private final Logger logger;

        /**
         * Creates a log-based reporter.
         *
         * @param logger the SLF4J logger for output
         * @throws IllegalArgumentException if logger is null
         */
        public LogReporter(Logger logger) {
            if (logger == null) {
                throw new IllegalArgumentException("Logger cannot be null");
            }
            this.logger = logger;
        }

        @Override
        public void reportMessage(String message) {
            logger.info(message);
        }
    }

    /**
     * Reporter implementation that outputs to standard output.
     *
     * <p>Messages are printed to {@code System.out}.
     *
     * <p><b>Thread Safety:</b> This implementation is thread-safe as {@code System.out} is
     * synchronized. However, messages from concurrent threads may be interleaved.
     */
    public static class ConsoleReporter extends AbstractReporter {

        @Override
        public void reportMessage(String message) {
            System.out.println(message);
        }
    }
}
