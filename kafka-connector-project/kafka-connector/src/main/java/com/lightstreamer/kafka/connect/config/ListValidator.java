
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

import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ListValidator implements Validator {

    @Override
    public final void ensureValid(String name, Object value) throws ConfigException {
        if (value == null) {
            throw new ConfigException(
                    String.format(
                            "Invalid value for configuration \"%s\": Must be non-null", name));
        }

        if (!(value instanceof List)) {
            throw new ConfigException(
                    String.format("Invalid value for configuration \"%s\": Must be a list", name));
        }

        List<?> list = (List<?>) value;
        if (list.isEmpty())
            throw new ConfigException(
                    String.format(
                            "Invalid value for configuration \"%s\": Must be a non-empty list",
                            name));

        Set<String> keys = new HashSet<>();
        for (Object element : list) {
            String key = validateElement(name, element);
            if (keys.contains(key)) {
                throw new ConfigException(
                        String.format(
                                "Invalid value for configuration \"%s\": Duplicate key \"%s\"",
                                name, key));
            }
            keys.add(key);
        }
    }

    private String validateElement(String name, Object element) throws ConfigException {
        if (!(element instanceof String)) {
            throw new ConfigException(
                    String.format(
                            "Invalid value for configuration \"%s\": Must be a list of non-empty strings",
                            name));
        }

        String stringElement = (String) element;
        if (stringElement.isBlank()) {
            throw new ConfigException(
                    String.format(
                            "Invalid value for configuration \"%s\": Must be a list of non-empty strings",
                            name));
        }
        return validateStringElement(name, stringElement);
    }

    protected String validateStringElement(String name, String element) throws ConfigException {
        return element;
    }
}
