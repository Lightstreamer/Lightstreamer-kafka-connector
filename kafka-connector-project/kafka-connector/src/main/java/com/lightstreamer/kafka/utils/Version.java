
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

package com.lightstreamer.kafka.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class Version {
    private static final Logger log = LoggerFactory.getLogger(Version.class);
    private static final String KEY = "version";
    private static final String DEFAULT_VALUE = "unknown";
    private static final String VERSION_PROPERTIES_FILE = "version.properties";

    private final String version;

    private Version(String versionFile) {
        Properties props = new Properties();
        try (InputStream resourceStream =
                Version.class.getClassLoader().getResourceAsStream(versionFile)) {
            props.load(resourceStream);
        } catch (Exception e) {
            log.warn("Error while loading {}}: {}", versionFile, e.getMessage());
        }

        this.version = props.getProperty(KEY, DEFAULT_VALUE).trim();
    }

    String get() {
        return version;
    }

    public static String getVersion(String versionFile) {
        return new Version(versionFile).get();
    }

    public static String getVersion() {
        return new Version(VERSION_PROPERTIES_FILE).get();
    }
}
