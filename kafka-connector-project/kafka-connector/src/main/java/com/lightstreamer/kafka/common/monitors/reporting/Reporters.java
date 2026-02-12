
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

package com.lightstreamer.kafka.common.monitors.reporting;

import org.slf4j.Logger;

/** Factory class providing standard {@link Reporter} implementations. */
public class Reporters {

    /**
     * Creates a reporter that logs messages using the specified logger.
     *
     * @param logger the logger to use for reporting
     * @return a log-based reporter
     */
    public static Reporter logReporter(Logger logger) {
        return new LogReporter(logger);
    }

    /**
     * Creates a reporter that prints messages to standard output.
     *
     * @return a console-based reporter
     */
    public static Reporter consoleReporter() {
        return new ConsoleReporter();
    }

    /** A reporter that sends messages to an SLF4J logger at INFO level. */
    public static class LogReporter implements Reporter {

        private final Logger logger;

        /**
         * Creates a log reporter with the specified logger.
         *
         * @param logger the logger for output
         */
        public LogReporter(Logger logger) {
            this.logger = logger;
        }

        @Override
        public void report(String message) {
            logger.info(message);
        }
    }

    /** A reporter that prints messages to standard output. */
    public static class ConsoleReporter implements Reporter {

        @Override
        public void report(String message) {
            System.out.println(message);
        }
    }
}
