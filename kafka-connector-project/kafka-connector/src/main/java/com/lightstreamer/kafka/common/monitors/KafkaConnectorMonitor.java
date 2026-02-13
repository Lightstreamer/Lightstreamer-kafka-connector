
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

import com.lightstreamer.kafka.common.monitors.KafkaConnectorMonitor.DefaultObserver.ObserverID;
import com.lightstreamer.kafka.common.monitors.metrics.Meter;
import com.lightstreamer.kafka.common.monitors.reporting.Reporter;
import com.lightstreamer.kafka.common.monitors.reporting.Reporter.MetricValueFormatter;
import com.lightstreamer.kafka.common.monitors.reporting.Reporters;
import com.lightstreamer.kafka.common.monitors.timeseries.Functions;
import com.lightstreamer.kafka.common.monitors.timeseries.Functions.Function;
import com.lightstreamer.kafka.common.monitors.timeseries.Functions.FunctionResult;
import com.lightstreamer.kafka.common.monitors.timeseries.RangeVector;
import com.lightstreamer.kafka.common.monitors.timeseries.TimeSeries;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Default {@link Monitor} implementation with configurable scrape intervals and circular buffer
 * storage.
 *
 * <p>Operates with two periodic tasks: scraping (samples meter values) and evaluation (computes and
 * reports aggregate functions). Configuration via fluent {@code with*} methods:
 *
 * <pre>{@code
 * new KafkaConnectorMonitor("my-connector")
 *     .withScrapeInterval(Duration.ofSeconds(5))
 *     .withDataPoints(240)
 *     .withLogReporter();
 * }</pre>
 */
public final class KafkaConnectorMonitor implements Monitor {

    private static final Duration DEFAULT_SCRAPE_INTERVAL = Duration.ofSeconds(15);
    private static final int DEFAULT_DATA_POINTS = 120;
    private static final String LOGGER_SUFFIX = "Monitor";

    private final Logger logger;
    private final Duration scrapeInterval;
    private final int dataPoints;
    private final LinkedHashMap<ObserverID, DefaultObserver> observers;
    private final Reporter reporter;
    private final AtomicInteger observerIndex = new AtomicInteger(0);
    private volatile ScheduledExecutorService executor;
    private volatile boolean started = false;

    static record FunctionConfig(Function function, int precision, MetricValueFormatter formatter) {

        public FunctionConfig {
            if (precision < 0 || precision > 9) {
                throw new IllegalArgumentException("precision must be between 0 and 9");
            }
        }
    }

    /**
     * Default implementation that stores enabled functions and delegates scraping to {@link
     * TimeSeries}.
     */
    static class DefaultObserver implements Observer {

        static final Duration DEFAULT_RANGE_INTERVAL = Duration.ofMinutes(1);

        /** Unique identifier for an observer instance. */
        static record ObserverID(int id) {}

        /** Defines evaluation order for aggregate functions. */
        enum FunctionPriority {
            LAST,
            RATE,
            IRATE,
            MIN,
            MAX,
            AVG
        }

        private final KafkaConnectorMonitor monitor;
        private final ObserverID observerID;
        private final Meter meter;
        private final Duration rangeInterval;
        private final EnumMap<FunctionPriority, FunctionConfig> enabledFunctions;
        private final TimeSeries timeSeries;
        private final String reportDescription;

        private DefaultObserver(
                KafkaConnectorMonitor monitor,
                ObserverID observerID,
                Meter meter,
                Duration rangeInterval,
                Map<FunctionPriority, FunctionConfig> functions) {
            this.monitor = monitor;
            this.observerID = observerID;
            this.meter = meter;
            this.rangeInterval = rangeInterval;
            this.enabledFunctions = new EnumMap<>(functions);
            this.timeSeries = new TimeSeries(monitor.dataPoints, meter);
            this.reportDescription = formatDescription(meter, rangeInterval);
        }

        private DefaultObserver(DefaultObserver source) {
            this(
                    source.monitor,
                    source.observerID,
                    source.meter,
                    source.rangeInterval,
                    source.enabledFunctions);
        }

        private static String formatDescription(Meter meter, Duration rangeInterval) {
            long rangeIntervalSeconds = rangeInterval.toSeconds();
            String intervalSpecifier;
            long rangeIntervalValue;
            if (rangeIntervalSeconds >= 60) {
                intervalSpecifier = "%dm";
                rangeIntervalValue = rangeInterval.toMinutes();
            } else {
                intervalSpecifier = "%ds";
                rangeIntervalValue = rangeIntervalSeconds;
            }
            return String.format(
                    "%s [" + intervalSpecifier + "]", meter.name(), rangeIntervalValue);
        }

        @Override
        public DefaultObserver enableLatest(int precision, MetricValueFormatter formatter) {
            enabledFunctions.put(
                    FunctionPriority.LAST,
                    new FunctionConfig(new Functions.Latest(), precision, formatter));
            return addAndGet(new DefaultObserver(this));
        }

        @Override
        public DefaultObserver enableRate(int precision, MetricValueFormatter formatter) {
            enabledFunctions.put(
                    FunctionPriority.RATE,
                    new FunctionConfig(new Functions.Rate(), precision, formatter));
            return addAndGet(new DefaultObserver(this));
        }

        @Override
        public DefaultObserver enableIrate(int precision, MetricValueFormatter formatter) {
            enabledFunctions.put(
                    FunctionPriority.IRATE,
                    new FunctionConfig(new Functions.InstantRate(), precision, formatter));
            return addAndGet(new DefaultObserver(this));
        }

        @Override
        public DefaultObserver enableMin(int precision, MetricValueFormatter formatter) {
            enabledFunctions.put(
                    FunctionPriority.MIN,
                    new FunctionConfig(new Functions.Min(), precision, formatter));
            return addAndGet(new DefaultObserver(this));
        }

        @Override
        public DefaultObserver enableMax(int precision, MetricValueFormatter formatter) {
            enabledFunctions.put(
                    FunctionPriority.MAX,
                    new FunctionConfig(new Functions.Max(), precision, formatter));
            return addAndGet(new DefaultObserver(this));
        }

        @Override
        public DefaultObserver enableAverage(int precision, MetricValueFormatter formatter) {
            enabledFunctions.put(
                    FunctionPriority.AVG,
                    new FunctionConfig(new Functions.Average(), precision, formatter));
            return addAndGet(new DefaultObserver(this));
        }

        @Override
        public DefaultObserver withRangeInterval(Duration interval) {
            if (interval == null || interval.isZero() || interval.isNegative()) {
                throw new IllegalArgumentException("interval must be positive and non-null");
            }
            long maxRangeIntervalMs = (long) monitor.dataPoints * monitor.scrapeInterval.toMillis();
            if (interval.toMillis() > maxRangeIntervalMs) {
                throw new IllegalArgumentException(
                        String.format(
                                "Range interval cannot be greater than %d ms (dataPoints * scrapeInterval)",
                                maxRangeIntervalMs));
            }
            return addAndGet(
                    new DefaultObserver(monitor, observerID, meter, interval, enabledFunctions));
        }

        DefaultObserver addAndGet(DefaultObserver observer) {
            monitor.addObserver(observer);
            return observer;
        }

        void observe() {
            timeSeries.sample();
        }

        void emit(Reporter reporter, long timestamp) {
            final RangeVector vector = timeSeries.selectRange(rangeInterval, timestamp);

            List<Reporter.MetricValue> entries =
                    enabledFunctions.values().stream().map(fp -> getResult(vector, fp)).toList();
            reporter.report(reportDescription, entries);
        }

        /**
         * Converts a function evaluation result into a metric value for reporting.
         *
         * @param vector the time series data range to evaluate
         * @param config the function configuration (function, precision, formatter)
         * @return a metric value containing the result and formatting metadata
         */
        private Reporter.MetricValue getResult(RangeVector vector, FunctionConfig config) {
            Function function = config.function();
            FunctionResult result = function.evaluate(vector);
            return new Reporter.MetricValue(
                    result.name(),
                    result.value(),
                    config.precision(),
                    result.decorateUnit(meter.unit()),
                    config.formatter());
        }
    }

    // Constructors

    private KafkaConnectorMonitor(
            String connectionName,
            String loggerSuffix,
            Duration scrapeInterval,
            int dataPoints,
            LinkedHashMap<ObserverID, DefaultObserver> observers,
            Reporter reporter) {
        this(
                connectionName == null || connectionName.isBlank()
                        ? null
                        : LoggerFactory.getLogger(connectionName + loggerSuffix),
                scrapeInterval,
                dataPoints,
                observers,
                reporter);
    }

    private KafkaConnectorMonitor(
            Logger logger,
            Duration scrapeInterval,
            int dataPoints,
            LinkedHashMap<ObserverID, DefaultObserver> observers,
            Reporter reporter) {
        if (logger == null) {
            throw new IllegalArgumentException("logger cannot be null");
        }
        if (scrapeInterval == null || scrapeInterval.isZero() || scrapeInterval.isNegative()) {
            throw new IllegalArgumentException("scrapeInterval must be positive and non-null");
        }
        if (dataPoints <= 0) {
            throw new IllegalArgumentException("dataPoints must be positive");
        }
        if (reporter == null) {
            throw new IllegalArgumentException("reporter cannot be null");
        }
        if (observers == null) {
            throw new IllegalArgumentException("observers cannot be null");
        }

        this.logger = logger;
        this.scrapeInterval = scrapeInterval;
        this.dataPoints = dataPoints;
        this.reporter = reporter;
        this.observers = observers;
    }

    /**
     * Creates a monitor with default configuration: 15s scrape interval, 120 data points, console
     * reporter.
     *
     * @param connectionName the Kafka connection name for the logger
     */
    public KafkaConnectorMonitor(String connectionName) {
        this(
                connectionName,
                LOGGER_SUFFIX,
                DEFAULT_SCRAPE_INTERVAL,
                DEFAULT_DATA_POINTS,
                new LinkedHashMap<>(),
                new Reporters.ConsoleReporter());
    }

    // Public methods

    /**
     * Returns a new monitor with the specified scrape interval.
     *
     * @param scrapeInterval how frequently to sample meter values
     * @return a new monitor with updated configuration
     */
    public KafkaConnectorMonitor withScrapeInterval(Duration scrapeInterval) {
        return new KafkaConnectorMonitor(
                this.logger, scrapeInterval, this.dataPoints, this.observers, this.reporter);
    }

    /**
     * Returns a new monitor with the specified circular buffer capacity.
     *
     * @param dataPoints maximum data points per observer
     * @return a new monitor with updated configuration
     */
    public KafkaConnectorMonitor withDataPoints(int dataPoints) {
        return new KafkaConnectorMonitor(
                this.logger, this.scrapeInterval, dataPoints, this.observers, this.reporter);
    }

    /**
     * Returns a new monitor with the specified reporter.
     *
     * @param reporter the reporter for monitoring output
     * @return a new monitor with updated configuration
     */
    public KafkaConnectorMonitor withReporter(Reporter reporter) {
        return new KafkaConnectorMonitor(
                this.logger, this.scrapeInterval, this.dataPoints, this.observers, reporter);
    }

    /**
     * Returns a new monitor with log-based reporter using this monitor's logger.
     *
     * @return a new monitor configured for SLF4J logging
     */
    public KafkaConnectorMonitor withLogReporter() {
        return withReporter(Reporters.logReporter(this.logger));
    }

    /**
     * Returns a new monitor with console-based reporter.
     *
     * @return a new monitor configured for stdout output
     */
    public KafkaConnectorMonitor withConsoleReporter() {
        return withReporter(Reporters.consoleReporter());
    }

    /** Returns the logger for this monitor. */
    public Logger logger() {
        return this.logger;
    }

    @Override
    public synchronized Observer observe(Meter meter) {
        if (meter == null) {
            throw new IllegalArgumentException("meter cannot be null");
        }
        if (started) {
            throw new IllegalStateException("Cannot add observers after monitor has started");
        }
        DefaultObserver observer =
                new DefaultObserver(
                        this,
                        new ObserverID(observerIndex.incrementAndGet()),
                        meter,
                        DefaultObserver.DEFAULT_RANGE_INTERVAL,
                        new EnumMap<>(DefaultObserver.FunctionPriority.class));
        addObserver(observer);
        return observer;
    }

    @Override
    public synchronized void start(Duration stepInterval) {
        if (started) {
            return;
        }

        // Validate stepInterval
        if (stepInterval == null || stepInterval.isZero() || stepInterval.isNegative()) {
            throw new IllegalArgumentException("stepInterval must be positive and non-null");
        }

        // Validate relationship between stepInterval and scrapeInterval
        if (stepInterval.toMillis() < scrapeInterval.toMillis()) {
            throw new IllegalArgumentException(
                    String.format(
                            "stepInterval (%d ms) cannot be less than scrapeInterval (%d ms)",
                            stepInterval.toMillis(), scrapeInterval.toMillis()));
        }

        // Log warning if stepInterval is not a multiple of scrapeInterval
        if (stepInterval.toMillis() % scrapeInterval.toMillis() != 0) {
            logger.warn(
                    "stepInterval ({} ms) is not a multiple of scrapeInterval ({} ms). "
                            + "This may lead to unpredictable evaluation timing.",
                    stepInterval.toMillis(),
                    scrapeInterval.toMillis());
        }

        this.started = true;

        logger.info(
                "Starting KafkaConnectorMonitor with scrape interval: {} ms and data points: {}",
                scrapeInterval.toMillis(),
                dataPoints);
        this.executor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r);
                            t.setDaemon(true);
                            t.setName("KafkaConnectorMonitor");
                            return t;
                        });
        this.executor.scheduleAtFixedRate(
                this::scrapeMeters,
                scrapeInterval.toMillis(),
                scrapeInterval.toMillis(),
                TimeUnit.MILLISECONDS);
        this.executor.scheduleAtFixedRate(
                this::evaluate,
                stepInterval.toMillis(),
                stepInterval.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized boolean isRunning() {
        return started;
    }

    @Override
    public synchronized void stop() {
        if (!started) {
            return;
        }

        try {
            logger.info("Stopping KafkaConnectorMonitor");
            this.executor.shutdown();

            // Wait up to 5 seconds for graceful termination
            if (!this.executor.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warn("Graceful shutdown timeout, forcing termination");
                this.executor.shutdownNow();
                this.executor.awaitTermination(2, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted while stopping monitor", e);
            this.executor.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            this.executor = null;
            this.started = false;
        }
    }

    // Private methods

    private void addObserver(DefaultObserver observer) {
        observers.put(observer.observerID, observer);
    }

    private void scrapeMeters() {
        try {
            observers.values().forEach(DefaultObserver::observe);
        } catch (Exception e) {
            logger.error("Error while scraping meters", e);
        }
    }

    private void evaluate() {
        long timestamp = System.currentTimeMillis();
        observers
                .values()
                .forEach(
                        observer -> {
                            try {
                                observer.emit(reporter, timestamp);
                            } catch (Exception e) {
                                logger.error(
                                        "Error while evaluating observer: " + observer.observerID,
                                        e);
                            }
                        });
    }
}
