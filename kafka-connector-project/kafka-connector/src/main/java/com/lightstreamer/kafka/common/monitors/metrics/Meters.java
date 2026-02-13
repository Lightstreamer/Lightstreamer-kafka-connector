
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

package com.lightstreamer.kafka.common.monitors.metrics;

import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/**
 * Factory class providing standard implementations of {@link Meter} for monitoring and metrics
 * collection.
 *
 * <p>This class provides two types of meters:
 *
 * <ul>
 *   <li>{@link Counter} - for monotonically increasing values (e.g., request counts, error counts)
 *   <li>{@link Gauge} - for values that can increase or decrease (e.g., memory usage, queue size)
 * </ul>
 */
public class Meters {

    /**
     * Abstract base class for meter implementations. Provides common functionality for storing and
     * accessing meter metadata (name, description, unit).
     */
    abstract static class AbstractMeter implements Meter {

        protected final String name;
        protected final String description;
        protected final String unit;

        /**
         * Constructs a new meter with the specified metadata.
         *
         * @param name the unique name of the meter
         * @param description a human-readable description of what the meter measures
         * @param unit the unit of measurement (e.g., "bytes", "requests", "%")
         * @throws NullPointerException if any parameter is null
         */
        protected AbstractMeter(String name, String description, String unit) {
            this.name = Objects.requireNonNull(name, "name cannot be null");
            this.description = Objects.requireNonNull(description, "description cannot be null");
            this.unit = Objects.requireNonNull(unit, "unit cannot be null");
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String unit() {
            return unit;
        }

        @Override
        public String description() {
            return description;
        }
    }

    /**
     * A counter meter that tracks monotonically increasing values.
     *
     * <p>Counters are suitable for metrics that only increase over time, such as:
     *
     * <ul>
     *   <li>Number of requests processed
     *   <li>Total records processed
     * </ul>
     *
     * <p>This implementation is thread-safe and optimized for high-contention scenarios.
     */
    public static final class Counter extends AbstractMeter {

        private final LongAdder counter = new LongAdder();

        /**
         * Constructs a new counter meter.
         *
         * @param name the unique name of the counter
         * @param description a human-readable description of what the counter measures
         * @param unit the unit of measurement (e.g., "requests", "bytes", "events")
         * @throws NullPointerException if any parameter is null
         */
        public Counter(String name, String description, String unit) {
            super(name, description, unit);
        }

        /**
         * Increments the counter by one.
         *
         * <p>This method is thread-safe.
         */
        public void increment() {
            counter.increment();
        }

        /**
         * Increments the counter by the specified delta.
         *
         * @param delta the amount to add to the counter
         */
        public void increment(long delta) {
            counter.add(delta);
        }

        @Override
        public double collect() {
            return counter.sum();
        }
    }

    /**
     * A gauge meter that tracks values that can increase or decrease over time.
     *
     * <p>Gauges are suitable for metrics such as:
     *
     * <ul>
     *   <li>Memory usage
     *   <li>Queue size
     *   <li>Active connections
     * </ul>
     *
     * <p>The value is obtained on-demand via a {@link Supplier} when {@link #collect()} is called.
     */
    public static final class Gauge extends AbstractMeter {

        private final Supplier<Double> valueSupplier;

        /**
         * Constructs a new gauge meter.
         *
         * @param name the unique name of the gauge
         * @param description a human-readable description of what the gauge measures
         * @param valueSupplier a supplier that provides the current value when queried
         * @param unit the unit of measurement (e.g., "bytes", "connections", "%")
         * @throws NullPointerException if any parameter is null
         */
        public Gauge(String name, String description, Supplier<Double> valueSupplier, String unit) {
            super(name, description, unit);
            this.valueSupplier =
                    Objects.requireNonNull(valueSupplier, "valueSupplier cannot be null");
        }

        @Override
        public double collect() {
            Double value = valueSupplier.get();
            return value != null ? value : Double.NaN;
        }
    }
}
