
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

package com.lightstreamer.kafka.connect.config;

import org.apache.kafka.common.config.ConfigException;

public class ValidatorUtils {

    static void ensureSplittable(String name, String value, String expecteFormatMsg) {
        String[] tokens = value.split(":");
        boolean valid = false;
        if (tokens.length == 2) {
            String emtryKey = tokens[0];
            String entryValue = tokens[1];
            valid = !(emtryKey.isBlank() || entryValue.isBlank());
        }
        if (!valid) {
            throw new ConfigException(
                    String.format(
                            "Invalid value for configuration \"%s\": Each entry must be expressed in the form %s",
                            name, expecteFormatMsg));
        }
    }
}
