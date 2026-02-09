
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

public interface Meter<V extends Number> {

    String name();

    String description();

    String unit();

    /**
     * Scrapes the current value of the meter. This method may be called periodically by a {@link
     * GlobalMonitor} to collect data points for time series analysis.
     *
     * @return the current value of the meter
     */
    V scrape();

    boolean isValueEnabled();

    default boolean isRateEnabled() {
        return false;
    }

    default boolean isInstantRateEnabled() {
        return false;
    }

    default boolean isMaxEnabled() {
        return false;
    }

    default boolean isMinEnabled() {
        return false;
    }
}
