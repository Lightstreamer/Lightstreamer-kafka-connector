
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

final class TimeSeries<V extends Number> {

    private static class DataPoint<V extends Number> {

        private final String name;
        private long timestamp;
        private long deltaTime;
        private V value;

        @SuppressWarnings("unchecked")
        DataPoint(String name) {
            this.name = name;
            this.timestamp = System.currentTimeMillis();
            this.value = (V) Integer.valueOf(0); // Initialize with zero
        }

        public String name() {
            return name;
        }

        public void update(V newValue) {
            this.value = newValue;
            long now = System.currentTimeMillis();
            this.deltaTime = now - this.timestamp;
            this.timestamp = now;
        }

        public long timestamp() {
            return timestamp;
        }

        public long deltaTime() {
            return deltaTime;
        }

        public V value() {
            return value;
        }
    }

    private DataPoint<V>[] dataPoints;
    private volatile int dataPointIndex = 0;
    private final Meter<V> meter;

    @SuppressWarnings("unchecked")
    public TimeSeries(int size, Meter<V> meter) {
        this.meter = meter;
        this.dataPoints = new DataPoint[size]; // Ring buffer for last two data points
        for (int i = 0; i < size; i++) {
            dataPoints[i] = new DataPoint<>(meter.name());
        }
    }

    public String name() {
        return meter.name();
    }

    public void scrape() {
        V newValue = meter.scrape();
        dataPoints[dataPointIndex].update(newValue);
        dataPointIndex = (dataPointIndex + 1) % dataPoints.length;
    }

    public V max() {
        V max = dataPoints[0].value();
        for (DataPoint<V> dp : dataPoints) {
            if (dp.value().doubleValue() > max.doubleValue()) {
                max = dp.value();
            }
        }
        return max;
    }

    public V min() {
        V min = dataPoints[0].value();
        for (DataPoint<V> dp : dataPoints) {
            if (dp.value().doubleValue() < min.doubleValue()) {
                min = dp.value();
            }
        }
        return min;
    }

    public void print(Reporter reporter) {
        StringBuilder sb = new StringBuilder();
        sb.append(meter.name()).append(" [");
        boolean appendCommand = false;
        if (meter.isValueEnabled()) {
            sb.append("current=").append(lastValue()).append(" ").append(meter.unit());
            appendCommand = true;
        }

        if (meter.isMaxEnabled()) {
            if (appendCommand) {
                sb.append(", ");
            }
            sb.append("max=").append(max()).append(" ").append(meter.unit());
            appendCommand = true;
        }

        if (meter.isMinEnabled()) {
            if (appendCommand) {
                sb.append(", ");
            }
            sb.append("min=").append(min()).append(" ").append(meter.unit());
            appendCommand = true;
        }

        if (meter.isRateEnabled()) {
            if (appendCommand) {
                sb.append(", ");
            }
            dumpRate(sb);
            appendCommand = true;
        }
        if (meter.isInstantRateEnabled()) {
            if (appendCommand) {
                sb.append(", ");
            }
            dumpInstantRate(sb);
        }
        sb.append("]");
        reporter.appendNewLine(sb.toString());
    }

    private void dumpRate(StringBuilder sb) {
        sb.append(String.format("avg=%.1fk %s/sec", rate(), meter.unit()));
    }

    private void dumpInstantRate(StringBuilder sb) {
        sb.append(String.format("instant=%.1fk %s/sec", instantRate(), meter.unit()));
    }

    public V lastValue() {
        return dataPoints[dataPoints.length - 1].value();
    }

    public double rate() {
        return rateOverLast(dataPoints.length);
    }

    public double rateOverLast(int points) {
        if (points > dataPoints.length) {
            throw new IllegalArgumentException("Requested data points exceed available history");
        }

        double meanDelta = 0.0;
        double meanValue = 0.0;
        for (int i = 0; i < points; i++) {
            meanDelta += dataPoints[i].deltaTime();
            meanValue += dataPoints[i].value().doubleValue();
        }

        meanDelta = meanDelta /= points;
        meanValue = meanValue /= points;

        double t = 0;
        double s = 0;
        for (int i = 0; i < points; i++) {
            t +=
                    (dataPoints[i].deltaTime() - meanDelta)
                            * (dataPoints[i].value().doubleValue() - meanValue);
            double difference = Math.abs(dataPoints[i].deltaTime() - meanDelta);
            s += difference * difference;
        }

        double rate = t / s;
        return rate;
    }

    public double instantRate() {
        DataPoint<V> current = dataPoints[(dataPoints.length - 1)];
        DataPoint<V> previous = dataPoints[(dataPoints.length - 2)];
        if (current == null || previous == null) {
            return 0.0; // Not enough data points to calculate rate
        }
        long timeDelta = current.timestamp() - previous.timestamp();
        if (timeDelta <= 0) {
            return 0.0; // Avoid division by zero or negative time
        }
        double valueDelta = current.value().doubleValue() - previous.value().doubleValue();
        return valueDelta / (timeDelta / 1000.0); // Rate per second
    }

    public DataPoint<V>[] getDataPoints() {
        return dataPoints;
    }
}
