
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

import com.lightstreamer.kafka.common.monitors.timeseries.RangeVector;
import com.lightstreamer.kafka.common.monitors.timeseries.TimeSeries.DataPoint;

import java.util.function.Supplier;

/**
 * Provides aggregate functions for evaluating time series data from {@link RangeVector}.
 *
 * <p>Functions compute statistical operations such as averages, rates, and extrema over time
 * series data points.
 */
public class Functions {

    /**
     * Result of a function evaluation.
     *
     * @param name the function name
     * @param value the computed value
     * @param unitDecorator suffix to append to the unit (e.g., "/sec" for rates)
     */
    public static record Output(String name, Number value, String unitDecorator) {

        public String format(String unit) {
            String formattedValue =
                    Double.isNaN(value.doubleValue()) ? "n/a" : String.valueOf(value);
            return name + "=" + formattedValue + " " + unit + unitDecorator;
        }

        public String decorateUnit(String unit) {
            return unit + unitDecorator;
        }
    }

    /** Represents a function that evaluates a time series range vector. */
    @FunctionalInterface
    public interface Function {

        /**
         * Evaluates the function over the given range vector.
         *
         * @param vector the time series data to evaluate
         * @return the evaluation result
         */
        Output evaluate(RangeVector vector);

        default String unitDecorator() {
            return "";
        }
    }

    /**
     * Base class for aggregate functions that compute a single value from a range vector.
     *
     * <p>Returns {@code NaN} when the vector is empty.
     */
    abstract static class AggregateFunction implements Function {

        private final String name;
        private final Supplier<String> unitDecoratorSupplier;

        AggregateFunction(String name) {
            this(name, () -> "");
        }

        AggregateFunction(String name, Supplier<String> unitDecorator) {
            this.name = name;
            this.unitDecoratorSupplier = unitDecorator;
        }

        public final String name() {
            return name;
        }

        @Override
        public Output evaluate(RangeVector vector) {
            if (!vector.isEmpty()) {
                return new Output(name(), doEvaluation(vector), unitDecorator());
            }
            return new Output(name(), Double.NaN, unitDecorator());
        }

        /**
         * Performs the actual computation on a non-empty vector.
         *
         * @param vector a non-empty range vector
         * @return the computed value
         */
        protected abstract double doEvaluation(RangeVector vector);

        @Override
        public String unitDecorator() {
            return unitDecoratorSupplier.get();
        }
    }

    /** Returns the most recent value in the time series. */
    public static class Latest extends AggregateFunction {

        public Latest() {
            super("latest");
        }

        @Override
        protected double doEvaluation(RangeVector vector) {
            return vector.last().value();
        }
    }

    /** Computes the arithmetic mean of all values in the time series. */
    public static class Average extends AggregateFunction {

        public Average() {
            super("avg");
        }

        @Override
        protected double doEvaluation(RangeVector vector) {
            return vector.valueStream()
                    .average()
                    .orElseThrow(); // Parent guarantees non-empty vector
        }
    }

    /** Returns the maximum value in the time series. */
    public static class Max extends AggregateFunction {

        public Max() {
            super("max");
        }

        @Override
        protected double doEvaluation(RangeVector vector) {
            return vector.valueStream().max().orElseThrow(); // Parent guarantees non-empty vector
        }
    }

    /** Returns the minimum value in the time series. */
    public static class Min extends AggregateFunction {

        public Min() {
            super("min");
        }

        @Override
        protected double doEvaluation(RangeVector vector) {
            return vector.valueStream().min().orElseThrow(); // Parent guarantees non-empty vector
        }
    }

    /** Returns the sum of all values in the time series. */
    public static class Sum extends AggregateFunction {

        public Sum() {
            super("sum");
        }

        @Override
        protected double doEvaluation(RangeVector vector) {
            return vector.valueStream().sum();
        }
    }

    /** Returns the number of data points in the time series. */
    public static class Count extends AggregateFunction {

        public Count() {
            super("count");
        }

        @Override
        protected double doEvaluation(RangeVector vector) {
            return vector.size();
        }
    }

    /**
     * Computes the average rate of change per second using linear regression.
     *
     * <p>Applies least squares regression across all data points to calculate a smooth rate.
     * Suitable for counter metrics. Returns {@code NaN} if fewer than 2 data points exist.
     */
    public static class Rate extends AggregateFunction {

        public Rate() {
            super("rate", () -> "/sec");
        }

        public Rate(Supplier<String> unitDecorator) {
            super("rate", unitDecorator);
        }

        @Override
        protected double doEvaluation(RangeVector vector) {
            int numOfSamples = vector.size();
            if (numOfSamples < 2) {
                return Double.NaN;
            }

            // Calculate means for linear regression
            double meanTimestamp = 0.0;
            double meanValue = 0.0;
            for (DataPoint dp : vector) {
                meanTimestamp += dp.timeValue();
                meanValue += dp.value();
            }
            meanTimestamp /= numOfSamples;
            meanValue /= numOfSamples;

            // Calculate slope using least squares linear regression: slope = Σ((x - meanX)(y -
            // meanY)) / Σ((x - meanX)²)
            double covariance = 0.0;
            double variance = 0.0;
            for (DataPoint dp : vector) {
                long ts = dp.timeValue();
                double value = dp.value();
                double tsDiff = ts - meanTimestamp;
                covariance += tsDiff * (value - meanValue);
                variance += tsDiff * tsDiff;
            }

            if (variance == 0.0) {
                return Double.NaN; // All timestamps are the same, cannot calculate rate
            }

            // Slope is per millisecond, convert to per second
            return (covariance / variance) * 1000.0;
        }
    }

    /**
     * Computes the instantaneous rate of change per second between the last two data points.
     *
     * <p>More responsive to recent changes than {@link Rate}. Returns {@code NaN} if fewer
     * than 2 data points exist.
     */
    public static class InstantRate extends AggregateFunction {

        public InstantRate() {
            super("irate", () -> "/sec");
        }

        public InstantRate(Supplier<String> unitDecorator) {
            super("irate", unitDecorator);
        }

        @Override
        protected double doEvaluation(RangeVector vector) {
            return vector.forLastTwo(this::calc);
        }

        private double calc(DataPoint last, DataPoint secondLast) {
            long timeDelta = last.timeValue() - secondLast.timeValue();

            if (timeDelta <= 0) {
                return Double.NaN; // Avoid division by zero or negative time
            }
            double valueDelta = last.value() - secondLast.value();
            return valueDelta * 1000.0 / timeDelta; // Rate per second
        }
    }
}
