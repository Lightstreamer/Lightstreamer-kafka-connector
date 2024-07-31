
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

import com.lightstreamer.kafka.common.expressions.ExpressionException;
import com.lightstreamer.kafka.common.expressions.Expressions;
import com.lightstreamer.kafka.common.utils.Split;
import com.lightstreamer.kafka.common.utils.Split.Pair;

import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        List<String> templates = Split.bySemicolon(strValue);
        Set<String> keys = new HashSet<>();
        for (String template : templates) {
            String key = validateTemplate(name, template);
            if (keys.contains(key)) {
                throw new ConfigException(
                        String.format(
                                "Invalid value for configuration \"%s\": Duplicate key \"%s\"",
                                name, key));
            }
            keys.add(key);
        }
    }

    private String validateTemplate(String name, String template) {
        if (template.isBlank()) {
            throw new ConfigException(
                    String.format(
                            "Invalid value for configuration \"%s\": Must be a semicolon-separated list of non-empty strings",
                            name));
        }

        // Gets the <template-name>:<template-expression> pair
        Pair pair =
                Split.pair(template)
                        .orElseThrow(
                                () ->
                                        new ConfigException(
                                                String.format(
                                                        "Invalid value for configuration \"%s\": Each entry must be expressed in the form %s",
                                                        name,
                                                        "<template-name:template-expression>")));

        try {
            // Validates <template-expression>
            Expressions.template(pair.value()).toString();

            // Retruns <template-name>
            return pair.key();
        } catch (ExpressionException ee) {
            throw new ConfigException(
                    String.format(
                            "Invalid value for configuration \"%s\": Template expression must be expressed in the form %s",
                            name, "<template-prefix-#{par1=val1,...parN=valN}"));
        }
    }
}
