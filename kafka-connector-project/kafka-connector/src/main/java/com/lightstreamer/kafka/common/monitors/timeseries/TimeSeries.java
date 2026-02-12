
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

import com.lightstreamer.kafka.common.monitors.metrics.Meter;

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

    private final List<DataPoint> dataPoints;
    private final int capacity;
    private final Meter meter;
    private int writeIndex = 0;
    private boolean bufferFull = false;

    /**
     * Creates a new time series with the specified capacity.
     *
     * @param capacity the maximum number of data points to store (must be positive)
     * @param meter the meter to sample values from
     * @throws IllegalArgumentException if capacity is not positive or meter is null
     */
    public TimeSeries(int capacity, Meter meter) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive");
        }
        if (meter == null) {
            throw new IllegalArgumentException("Meter cannot be null");
        }
        this.capacity = capacity;
        this.meter = meter;
        this.dataPoints = new ArrayList<>(capacity);
    }

    /**
     * Returns the name of the underlying meter.
     *
     * @return the meter name
     */
    public String name() {
        return meter.name();
    }

    /**
     * Returns the unit of measurement for the underlying meter.
     *
     * @return the measurement unit
     */
    public String unit() {
        return meter.unit();
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
     * Samples the meter and adds the current value to the time series.
     *
     * <p>The buffer grows lazily until reaching capacity, then operates as a circular buffer by
     * overwriting the oldest data point. This method is thread-safe.
     */
    public void scrape() {
        double newValue = meter.scrape();
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
}
