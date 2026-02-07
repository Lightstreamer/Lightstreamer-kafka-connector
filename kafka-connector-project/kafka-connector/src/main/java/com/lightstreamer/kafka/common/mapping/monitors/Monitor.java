
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

import java.time.Duration;

/**
 * Base interface for periodic monitoring components.
 *
 * <p>Monitors are invoked periodically to collect and report statistics. Implementations should be
 * lightweight and non-blocking, as {@link #check()} is typically called from the main processing
 * loop.
 *
 * <p><b>Thread Safety:</b> Implementations must be thread-safe as {@link #check()} may be called
 * from multiple threads, though typically from a single consumer thread.
 *
 * @see BaseMonitor
 * @see Monitors
 */
public interface Monitor {

    /**
     * Performs a periodic check and reports statistics if the configured interval has elapsed.
     *
     * <p>This method is designed to be called frequently (e.g., after each batch). Implementations
     * should track time internally and only perform actual reporting when the configured interval
     * has passed.
     */
    void check();

    /**
     * Returns the configured interval between checks.
     *
     * @return the check interval
     */
    Duration getCheckInterval();
}
