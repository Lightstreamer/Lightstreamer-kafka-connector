
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

package com.lightstreamer.kafka.test_utils;

import java.io.InputStream;
import java.util.Properties;
import java.util.regex.Pattern;

public class VersionUtils {
    static Pattern VERSION = Pattern.compile("\\d\\.\\d\\.\\d");

    /** Gets the current version as stored in the resources/version.properties file */
    public static String currentVersion() {
        // Load the current version into the VERSION variable to be used as expected result
        Properties props = new Properties();
        try (InputStream resourceStream =
                VersionUtils.class.getResourceAsStream("/version.properties")) {
            props.load(resourceStream);
        } catch (Exception e) {
            throw new RuntimeException("Unable to find the current version");
        }

        String version = props.getProperty("version");

        if (!(VERSION.matcher(version).matches())) {
            throw new RuntimeException(
                    "Invalid version file: %s is not a valid string version".formatted(version));
        }
        return version;
    }
}
