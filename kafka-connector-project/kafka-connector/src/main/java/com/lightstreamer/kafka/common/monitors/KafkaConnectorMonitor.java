
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

import com.lightstreamer.kafka.common.monitors.Functions.Function;
import com.lightstreamer.kafka.common.monitors.KafkaConnectorMonitor.DefaultObserver.ObserverID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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

    static class DefaultObserver implements Observer {

        static final Duration DEFAULT_RANGE_INTERVAL = Duration.ofMinutes(1);

        static record ObserverID(int id) {}

        enum FunctionPriority {
            LAST,
            RATE,
            IRATE,
            MIN,
            MAX,
            AVG;
        }

        private final KafkaConnectorMonitor monitor;
        private final ObserverID observerID;
        private final TimeSeries timeSeries;
        private final RangeSelector rangeSelector;
        private final EnumMap<FunctionPriority, Function> enabledFunctions;

        private DefaultObserver(
                KafkaConnectorMonitor monitor,
                ObserverID observerID,
                TimeSeries timeSeries,
                Duration rangeInterval,
                Map<FunctionPriority, Function> functions) {
            this.monitor = monitor;
            this.observerID = observerID;
            this.timeSeries = timeSeries;
            this.rangeSelector = new RangeSelector(rangeInterval);
            this.enabledFunctions = new EnumMap<>(functions);
        }

        private DefaultObserver(DefaultObserver source) {
            this(
                    source.monitor,
                    source.observerID,
                    source.timeSeries,
                    source.rangeSelector.interval(),
                    source.enabledFunctions);
        }

        @Override
        public DefaultObserver enableLast() {
            enabledFunctions.put(FunctionPriority.LAST, new Functions.Last());
            return new DefaultObserver(this);
        }

        @Override
        public DefaultObserver enableRate() {
            enabledFunctions.put(FunctionPriority.RATE, new Functions.Rate());
            return new DefaultObserver(this);
        }

        @Override
        public DefaultObserver enableIrate() {
            enabledFunctions.put(FunctionPriority.IRATE, new Functions.InstantRate());
            return new DefaultObserver(this);
        }

        @Override
        public DefaultObserver enableMin() {
            enabledFunctions.put(FunctionPriority.MIN, new Functions.Min());
            return new DefaultObserver(this);
        }

        @Override
        public DefaultObserver enableMax() {
            enabledFunctions.put(FunctionPriority.MAX, new Functions.Max());
            return new DefaultObserver(this);
        }

        @Override
        public DefaultObserver enableAverage() {
            enabledFunctions.put(FunctionPriority.AVG, new Functions.Average());
            return new DefaultObserver(this);
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
            return new DefaultObserver(monitor, observerID, timeSeries, interval, enabledFunctions);
        }

        @Override
        public void observe() {
            timeSeries.scrape();
        }

        void emit(Reporter reporter, long timestamp) {
            RangeVector vector = rangeSelector.eval(timeSeries, timestamp);

            StringBuilder sb = new StringBuilder();
            String format =
                    String.format(
                            "%s [%dm]", timeSeries.name(), rangeSelector.interval().toMinutes());
            sb.append(format).append(" [");
            boolean appendCommand = false;
            for (Function function : enabledFunctions.values()) {
                Functions.Output output = function.evaluate(vector);
                if (appendCommand) {
                    sb.append(", ");
                }
                sb.append(format(output, timeSeries.unit()));
                appendCommand = true;
            }

            sb.append("]");
            reporter.report(sb.toString());
        }

        static String format(Functions.Output output, String unit) {
            String name = output.name();
            double value = output.value().doubleValue();
            String decoratedUnit = output.decorateUnit(unit);
            double scaledValue = value;
            String fmt = "%.2f";
            if (value >= 1_000_000.0) {
                scaledValue = value / 1_000_000.0;
                fmt = "%.1fM";
            } else if (value >= 1000.0) {
                scaledValue = value / 1000.0;
                fmt = "%.1fk";
            }
            return String.format("%s=" + fmt + " %s", name, scaledValue, decoratedUnit);
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

    public KafkaConnectorMonitor(String connectionName) {
        this(
                connectionName,
                LOGGER_SUFFIX,
                DEFAULT_SCRAPE_INTERVAL,
                DEFAULT_DATA_POINTS,
                new LinkedHashMap<>(),
                new Reporters.StdOutReporter());
    }

    // Public methods

    public KafkaConnectorMonitor withScrapeInterval(Duration scrapeInterval) {
        return new KafkaConnectorMonitor(
                this.logger, scrapeInterval, this.dataPoints, this.observers, this.reporter);
    }

    public KafkaConnectorMonitor withDataPoints(int dataPoints) {
        return new KafkaConnectorMonitor(
                this.logger, this.scrapeInterval, dataPoints, this.observers, this.reporter);
    }

    public KafkaConnectorMonitor withReporter(Reporter reporter) {
        return new KafkaConnectorMonitor(
                this.logger, this.scrapeInterval, this.dataPoints, this.observers, reporter);
    }

    public KafkaConnectorMonitor withReporter(Reporters.ReporterProvider reporter) {
        if (reporter == null) {
            throw new IllegalArgumentException("reporter cannot be null");
        }
        return new KafkaConnectorMonitor(
                this.logger,
                this.scrapeInterval,
                this.dataPoints,
                this.observers,
                reporter.get(this));
    }

    @Override
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
                        new TimeSeries(dataPoints, meter),
                        DefaultObserver.DEFAULT_RANGE_INTERVAL,
                        new EnumMap<>(DefaultObserver.FunctionPriority.class));
        observers.put(observer.observerID, observer);
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
