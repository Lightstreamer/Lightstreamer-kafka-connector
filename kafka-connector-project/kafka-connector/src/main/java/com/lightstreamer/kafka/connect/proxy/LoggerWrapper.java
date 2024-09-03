
/*
 * Copyright (C) 2024 Lightstreamer Srl
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

package com.lightstreamer.kafka.connect.proxy;

import org.slf4j.Logger;

final class LoggerWrapper implements com.lightstreamer.log.Logger {

    private final Logger logger;

    LoggerWrapper(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void error(String line) {
        logger.error(line);
    }

    @Override
    public void error(String line, Throwable exception) {
        logger.error(line, exception);
    }

    @Override
    public void warn(String line) {
        logger.warn(line);
    }

    @Override
    public void warn(String line, Throwable exception) {
        logger.warn(line, exception);
    }

    @Override
    public void info(String line) {
        logger.info(line);
    }

    @Override
    public void info(String line, Throwable exception) {
        logger.info(line, exception);
    }

    @Override
    public void debug(String line) {
        logger.debug(line);
    }

    @Override
    public void debug(String line, Throwable exception) {
        logger.debug(line, exception);
    }

    @Override
    public void fatal(String line) {
        error(line);
    }

    @Override
    public void fatal(String line, Throwable exception) {
        error(line, exception);
    }

    @Override
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    @Override
    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    @Override
    public boolean isWarnEnabled() {
        return logger.isWarnEnabled();
    }

    @Override
    public boolean isErrorEnabled() {
        return logger.isErrorEnabled();
    }

    @Override
    public boolean isFatalEnabled() {
        return logger.isErrorEnabled();
    }
}
