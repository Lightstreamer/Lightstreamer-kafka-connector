
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

/**
 * Represents a metric that can be periodically sampled to collect time series data. Implementations
 * must be thread-safe as {@link #collect()} may be called concurrently.
 */
public interface Meter extends Collectable {

    /**
     * Returns the unique name of this meter.
     *
     * @return the meter name
     */
    String name();

    /**
     * Returns a human-readable description of what this meter measures.
     *
     * @return the meter description
     */
    String description();
}
