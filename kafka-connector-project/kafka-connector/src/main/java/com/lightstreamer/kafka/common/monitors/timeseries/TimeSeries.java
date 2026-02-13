
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

package com.lightstreamer.kafka.common.monitors.timeseries;

import com.lightstreamer.kafka.common.monitors.metrics.Collectable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * A thread-safe time series data structure that stores a fixed-capacity window of metric samples.
 *
 * <p>Uses a circular buffer to maintain recent data points efficiently. The buffer grows lazily
 * until reaching capacity, then operates in circular mode by overwriting the oldest entries.
 *
 * <p>This class is designed for low-frequency sampling (typically 1Hz scraping, 0.5Hz snapshots)
 * and uses synchronized blocks for thread safety.
 *
 * <p>All operations are thread-safe and can be called concurrently from multiple threads.
 */
public final class TimeSeries {

    /**
     * An immutable data point representing a metric value at a specific timestamp.
     *
     * @param value the metric value
     * @param timeValue the timestamp in milliseconds since epoch
     */
    public record DataPoint(double value, long timeValue) {

        /**
         * Creates a data point with the given value and the current system time.
         *
         * @param value the metric value
         */
        public DataPoint(double value) {
            this(value, System.currentTimeMillis());
        }

        /** Creates a data point with zero value and the current system time. */
        public DataPoint() {
            this(0);
        }

        /**
         * Creates a new data point with elapsed time relative to a base timestamp.
         *
         * <p>Used to convert absolute timestamps to relative time offsets for time range queries.
         *
         * @param base the base timestamp to subtract from this data point's timestamp
         * @return a new data point with the same value but adjusted timestamp
         */
        DataPoint withElapsedTimeFrom(long base) {
            return new DataPoint(value, timeValue - base);
        }
    }

    private static record RangeInterval(long startInclusive, long endExclusive) {

        static RangeInterval endingAt(long timestamp, Duration duration) {
            return new RangeInterval(timestamp - duration.toMillis(), timestamp);
        }

        boolean contains(long timeValue) {
            return startInclusive <= timeValue && timeValue < endExclusive;
        }
    }

    private final List<DataPoint> dataPoints;
    private final int capacity;
    private final Collectable collectable;
    private int writeIndex = 0;
    private boolean bufferFull = false;

    /**
     * Creates a new time series with the specified capacity.
     *
     * @param capacity the maximum number of data points to store (must be positive)
     * @param collectable the collectable to sample values from
     * @throws IllegalArgumentException if capacity is not positive or collectable is null
     */
    public TimeSeries(int capacity, Collectable collectable) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive");
        }
        if (collectable == null) {
            throw new IllegalArgumentException("Collectable cannot be null");
        }
        this.capacity = capacity;
        this.collectable = collectable;
        this.dataPoints = new ArrayList<>(capacity);
    }

    /**
     * Returns the unit of measurement for the underlying collectable.
     *
     * @return the measurement unit
     */
    public String unit() {
        return collectable.unit();
    }

    /**
     * Returns the current number of data points stored.
     *
     * @return the number of data points (between 0 and capacity)
     */
    public int size() {
        synchronized (dataPoints) {
            return dataPoints.size();
        }
    }

    /**
     * Returns the maximum capacity of this time series.
     *
     * @return the capacity
     */
    public int capacity() {
        return capacity;
    }

    /**
     * Returns whether any data has been collected.
     *
     * @return true if no data points are stored
     */
    public boolean isEmpty() {
        synchronized (dataPoints) {
            return dataPoints.isEmpty();
        }
    }

    /**
     * Returns whether the buffer is full and operating in circular mode.
     *
     * @return true if the buffer has reached capacity
     */
    public boolean isFull() {
        synchronized (dataPoints) {
            return bufferFull;
        }
    }

    /**
     * Samples the collectable and adds the current value to the time series.
     *
     * <p>The buffer grows lazily until reaching capacity, then operates as a circular buffer by
     * overwriting the oldest data point. This method is thread-safe.
     */
    public void sample() {
        double newValue = collectable.collect();
        synchronized (dataPoints) {
            if (!bufferFull) {
                // Still filling the buffer
                dataPoints.add(new DataPoint(newValue));
                if (dataPoints.size() == capacity) {
                    bufferFull = true;
                    writeIndex = 0;
                }
            } else {
                // Circular buffer: write at current index and advance
                dataPoints.set(writeIndex, new DataPoint(newValue));
                writeIndex = (writeIndex + 1) % capacity;
            }
        }
    }

    /**
     * Returns the time range covered by the current data.
     *
     * <p>The returned array contains [oldestTimestamp, newestTimestamp] in milliseconds. Returns
     * {@code null} if no data has been collected yet.
     *
     * @return array with [oldestTimestamp, newestTimestamp], or null if empty
     */
    public long[] timeRange() {
        synchronized (dataPoints) {
            if (dataPoints.isEmpty()) {
                return null;
            }
            if (!bufferFull) {
                return new long[] {
                    dataPoints.get(0).timeValue(), dataPoints.get(dataPoints.size() - 1).timeValue()
                };
            }
            return new long[] {
                dataPoints.get(writeIndex).timeValue(),
                dataPoints.get((writeIndex + capacity - 1) % capacity).timeValue()
            };
        }
    }

    /**
     * Returns an immutable snapshot of all data points in chronological order.
     *
     * <p>If the buffer is not yet full, returns all collected points. If full, returns all points
     * from oldest to newest. The returned list is immutable and safe to iterate without
     * synchronization. This method is thread-safe.
     *
     * @return an immutable list of data points in chronological order
     */
    public List<DataPoint> snapshot() {
        synchronized (dataPoints) {
            if (!bufferFull) {
                // Buffer not full yet, return all collected points in order
                return List.copyOf(dataPoints);
            }
            // Read in chronological order: from writeIndex (oldest) to writeIndex-1 (newest)
            List<DataPoint> result = new ArrayList<>(capacity);
            for (int i = 0; i < capacity; i++) {
                int index = (writeIndex + i) % capacity;
                result.add(dataPoints.get(index));
            }
            return List.copyOf(result);
        }
    }

    /**
     * Selects data points within a time range and normalizes their timestamps.
     *
     * <p>Returns data points that fall within the interval [endTimestamp - interval, endTimestamp).
     * Timestamps are normalized to elapsed time from the first matching point.
     *
     * @param interval the duration of the time range (must be positive)
     * @param endTimestamp the end of the time range in milliseconds since epoch
     * @return a range vector with matching data points, or empty if no matches
     * @throws IllegalArgumentException if interval is null, not positive, or endTimestamp is
     *     negative
     */
    public RangeVector selectRange(Duration interval, long endTimestamp) {
        if (interval == null) {
            throw new IllegalArgumentException("Interval cannot be null");
        }
        if (interval.isNegative() || interval.isZero()) {
            throw new IllegalArgumentException("Interval must be positive");
        }

        if (endTimestamp < 0) {
            throw new IllegalArgumentException("End timestamp cannot be negative");
        }

        RangeInterval range = RangeInterval.endingAt(endTimestamp, interval);
        List<DataPoint> snapshot = snapshot();

        if (snapshot.isEmpty()) {
            return RangeVector.empty();
        }

        List<DataPoint> result = new ArrayList<>();
        long baseTime = 0;
        boolean foundFirst = false;

        for (DataPoint dp : snapshot) {
            if (range.contains(dp.timeValue())) {
                if (!foundFirst) {
                    baseTime = dp.timeValue();
                    foundFirst = true;
                }
                result.add(dp.withElapsedTimeFrom(baseTime));
            }
        }

        if (result.isEmpty()) {
            return RangeVector.empty();
        }

        return RangeVector.of(result);
    }
}
