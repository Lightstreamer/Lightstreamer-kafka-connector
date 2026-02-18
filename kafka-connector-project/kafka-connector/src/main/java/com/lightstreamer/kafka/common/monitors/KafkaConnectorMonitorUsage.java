
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
import java.util.concurrent.TimeUnit;

import com.lightstreamer.kafka.common.monitors.metrics.Meters;
import com.lightstreamer.kafka.common.monitors.reporting.Reporter.MetricValue;
import com.lightstreamer.kafka.common.monitors.reporting.Reporter.MetricValueFormatter;

class KafkaConnectorMonitorUsage {

    public static void main(String[] args) {
        KafkaConnectorMonitor globalMonitor =
                new KafkaConnectorMonitor("Testconnection")
                        .withDataPoints(120)
                        .withScrapeInterval(Duration.ofSeconds(1))
                        .withConsoleReporter();

        Meters.Counter processedRecordCounter1 =
                new Meters.Counter(
                        "Processed record 1", "Counts the number of processed records", "msg");

        Meters.Counter processedRecordCounter2 =
                new Meters.Counter(
                        "Processed record 2", "Counts the number of processed records", "msg");

        globalMonitor
                .observe(processedRecordCounter1)
                .enableLatest()
                .enableRate(
                        4,
                        new MetricValueFormatter() {
                            @Override
                            public String formatMetric(MetricValue value) {
                                return String.format("Getting %.2f msg/s", value.value());
                            }
                        })
                .enableIrate()
                .enableMax()
                .withRangeInterval(Duration.ofSeconds(2));
        globalMonitor
                .observe(processedRecordCounter2)
                .enableLatest()
                .enableRate()
                .enableIrate()
                .enableMin()
                .enableMax()
                .enableAverage()
                .withRangeInterval(Duration.ofMinutes(2));

        globalMonitor.start(Duration.ofSeconds(1));

        while (true) {
            try {
                int increment = 500;
                processedRecordCounter1.increment(increment);
                processedRecordCounter2.increment(increment);
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
