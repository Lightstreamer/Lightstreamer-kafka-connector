
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

public class ItemTemplateValidator implements Validator {

    @Override
    public void ensureValid(String name, Object value) {
        if (value == null) {
            return;
        }

        if (!(value instanceof String)) {
            throw new ConfigException(
                    String.format(
                            "Invalid value for configuration \"%s\": Must be a string", name));
        }

        String strValue = (String) value;
        if (strValue.isBlank()) {
            throw new ConfigException(
                    String.format(
                            "Invalid value for configuration \"%s\": Must be a non-empty semicolon-separated list",
                            name));
        }
        String[] templates = strValue.split(";", -1);
        if (templates.length == 0) {
            throw new ConfigException(
                    String.format(
                            "Invalid value for configuration \"%s\": Must be a semicolon-separated list of non-empty strings",
                            name));
        }

        for (String template : templates) {
            validateTemplate(name, template);
        }
    }

    private void validateTemplate(String name, String template) {
        if (template.isBlank()) {
            throw new ConfigException(
                    String.format(
                            "Invalid value for configuration \"%s\": Must be a semicolon-separated list of non-empty strings",
                            name));
        }

        ValidatorUtils.ensureSplittable(name, template, "<template-name:template-value>");
    }
}
