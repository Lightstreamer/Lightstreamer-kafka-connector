
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

import com.lightstreamer.kafka.common.records.RecordBatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public final class GlobalMonitor {

    private final Logger logger;
    private final Duration interval;
    private final int dataPoints;
    private final Map<String, TimeSeries<?>> timeSeries = new HashMap<>();
    private volatile ScheduledExecutorService executor;
    private final Reporter reporter;

    private GlobalMonitor(Logger logger, Duration interval, int dataPoints, Reporter reporter) {
        this.logger = LoggerFactory.getLogger(logger.getName() + "Monitor");
        this.interval = interval;
        this.dataPoints = dataPoints;
        this.reporter = reporter;
    }

    public GlobalMonitor(Logger logger, Duration interval, int dataPoints) {
        this(logger, interval, dataPoints, new StdOutReporter());
    }

    public GlobalMonitor reporter(Reporter reporter) {
        return new GlobalMonitor(this.logger, this.interval, this.dataPoints, reporter);
    }

    public void register(Meter<?> meter) {
        timeSeries.put(meter.name(), new TimeSeries<>(dataPoints, meter));
    }

    public void start() {
        if (this.executor != null) {}
        this.executor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r);
                            t.setDaemon(true);
                            t.setName("GlobalMonitor");
                            return t;
                        });
        this.executor.scheduleAtFixedRate(
                this::scrapeMeters,
                interval.toMillis(),
                interval.toMillis(),
                java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    private void scrapeMeters() {
        timeSeries
                .values()
                .forEach(
                        series -> {
                            try {
                                series.scrape();
                                series.print(reporter);
                            } catch (Exception e) {
                                logger.error("Error scraping meter: " + series.name(), e);
                                e.printStackTrace();
                            }
                        });
    }

    public static void main(String[] args) {
        GlobalMonitor monitor =
                new GlobalMonitor(
                        LoggerFactory.getLogger(GlobalMonitor.class), Duration.ofMillis(1200), 5);
        Meters.Counter processedRecordCounter =
                new Meters.Counter(
                                "Processed record", "Counts the number of processed records", "msg")
                        .enableTotalCount()
                        .enableIrate()
                        .enableRate()
                        .enableMax()
                        .enableMin();
        monitor.register(processedRecordCounter);
        RecordBatch.RecordBatchListener listener =
                batch -> processedRecordCounter.increment(batch.count());

        monitor.start();

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(
                () -> {
                    processedRecordCounter.increment(100);
                },
                0,
                200,
                java.util.concurrent.TimeUnit.MILLISECONDS);
    }
}
