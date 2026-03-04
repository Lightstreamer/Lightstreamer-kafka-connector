
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

import com.lightstreamer.kafka.common.monitors.timeseries.TimeSeries.DataPoint;

import java.util.Iterator;
import java.util.List;
import java.util.stream.DoubleStream;

/**
 * An immutable collection of time series data points representing a selected time range.
 *
 * <p>Provides convenient query operations for aggregate function evaluation. Instances are created
 * via package-private factory methods and contain normalized timestamp data.
 *
 * <p>This class implements {@link Iterable} to support for-each loops and stream operations.
 */
public class RangeVector implements Iterable<DataPoint> {

    private final List<DataPoint> dataPoints;

    private RangeVector(List<DataPoint> dataPoints) {
        this.dataPoints = dataPoints;
    }

    /**
     * Creates an empty range vector with no data points.
     *
     * @return an empty range vector
     */
    static RangeVector empty() {
        return new RangeVector(List.of());
    }

    /**
     * Creates a range vector from the given list of data points.
     *
     * <p>The list is used directly without defensive copying, so the caller must ensure it is not
     * modified after creation.
     *
     * @param dataPoints the list of data points
     * @return a range vector containing the data points
     */
    static RangeVector of(List<DataPoint> dataPoints) {
        return new RangeVector(dataPoints);
    }

    /**
     * Returns the number of data points in this range vector.
     *
     * @return the number of data points
     */
    public int size() {
        return dataPoints.size();
    }

    /**
     * Returns whether this range vector contains no data points.
     *
     * @return true if empty
     */
    public boolean isEmpty() {
        return dataPoints.isEmpty();
    }

    /**
     * Returns the last data point in this range vector.
     *
     * @return the last data point
     * @throws IllegalStateException if the range vector is empty
     */
    public DataPoint last() {
        if (dataPoints.isEmpty()) {
            throw new IllegalStateException("Cannot get last element from empty RangeVector");
        }
        return dataPoints.get(dataPoints.size() - 1);
    }

    /**
     * Returns the first data point in this range vector.
     *
     * @return the first data point
     * @throws IllegalStateException if the range vector is empty
     */
    public DataPoint first() {
        if (dataPoints.isEmpty()) {
            throw new IllegalStateException("Cannot get first element from empty RangeVector");
        }
        return dataPoints.get(0);
    }

    /**
     * Returns the second-to-last data point in this range vector.
     *
     * @return the second-to-last data point
     * @throws IllegalStateException if the range vector has fewer than 2 elements
     */
    public DataPoint secondLast() {
        if (dataPoints.size() < 2) {
            throw new IllegalStateException(
                    "Cannot get second-to-last element from RangeVector with fewer than 2 elements");
        }
        return dataPoints.get(dataPoints.size() - 2);
    }

    /**
     * Returns a stream of the metric values from all data points.
     *
     * <p>Useful for aggregate operations like sum, average, min, and max.
     *
     * @return a stream of double values
     */
    public DoubleStream valueStream() {
        return dataPoints.stream().mapToDouble(DataPoint::value);
    }

    /**
     * Returns an iterator over the data points in chronological order.
     *
     * @return an iterator over the data points
     */
    @Override
    public Iterator<DataPoint> iterator() {
        return dataPoints.iterator();
    }
}
