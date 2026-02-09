
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

import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

public class Meters {

    abstract static class AbstractMeter<V extends Number> implements Meter<V> {

        protected final String name;
        protected final String description;
        protected final String unit;
        protected final boolean enableValue;
        protected final boolean enableMax;
        protected final boolean enableMin;

        protected AbstractMeter(
                String name,
                String description,
                String unit,
                boolean enableValue,
                boolean enableMax,
                boolean enableMin) {
            this.name = name;
            this.description = description;
            this.unit = unit;
            this.enableValue = enableValue;
            this.enableMax = enableMax;
            this.enableMin = enableMin;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String description() {
            return description;
        }

        @Override
        public String unit() {
            return unit;
        }

        @Override
        public boolean isValueEnabled() {
            return enableValue;
        }

        @Override
        public boolean isMaxEnabled() {
            return enableMax;
        }

        @Override
        public boolean isMinEnabled() {
            return enableMin;
        }
    }

    public static final class Counter extends AbstractMeter<Long> {

        private final LongAdder counter = new LongAdder();

        private final boolean enableRate;
        private final boolean enableIrate;

        private Counter(
                String name,
                String description,
                String unit,
                boolean enableTotalCount,
                boolean enableMax,
                boolean enableMin,
                boolean enableRate,
                boolean enableIrate) {
            super(name, description, unit, enableTotalCount, enableMax, enableMin);
            this.enableRate = enableRate;
            this.enableIrate = enableIrate;
        }

        public Counter(String name, String description, String unit) {
            this(name, description, unit, false, false, false, false, false);
        }

        public Counter enableTotalCount() {
            return new Counter(
                    name, description, unit, true, enableMax, enableMin, enableRate, enableIrate);
        }

        public Counter enableRate() {
            return new Counter(
                    name, description, unit, enableValue, enableMax, enableMin, true, enableIrate);
        }

        public Counter enableIrate() {
            return new Counter(
                    name, description, unit, enableValue, enableMax, enableMin, enableRate, true);
        }

        public Counter enableMax() {
            return new Counter(
                    name, description, unit, enableValue, true, enableMin, enableRate, enableIrate);
        }

        public Counter enableMin() {
            return new Counter(
                    name, description, unit, enableValue, enableMax, true, enableRate, enableIrate);
        }

        public void increment(long delta) {
            counter.add(delta);
        }

        @Override
        public boolean isRateEnabled() {
            return enableRate;
        }

        @Override
        public boolean isInstantRateEnabled() {
            return enableIrate;
        }

        @Override
        public Long scrape() {
            return counter.sum();
        }
    }

    public static class Gauge<V extends Number> extends AbstractMeter<V> {

        private final Supplier<V> valueSupplier;

        private Gauge(
                String name,
                String description,
                Supplier<V> valueSupplier,
                String unit,
                boolean enableMax,
                boolean enableMin) {
            super(name, description, unit, true, enableMax, enableMin);
            this.valueSupplier = valueSupplier;
        }

        public Gauge(String name, String description, Supplier<V> valueSupplier, String unit) {
            this(name, description, valueSupplier, unit, false, false);
        }

        public Gauge<V> enableMax() {
            return new Gauge<>(name, description, valueSupplier, unit, true, enableMin);
        }

        public Gauge<V> enableMin() {
            return new Gauge<>(name, description, valueSupplier, unit, enableMax, true);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String description() {
            return description;
        }

        @Override
        public V scrape() {
            return valueSupplier.get();
        }
    }
}
