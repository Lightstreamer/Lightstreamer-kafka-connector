
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

import org.apache.kafka.common.config.ConfigException;

public class RecordMappingValidator extends ListValidator {

    @Override
    public String validateStringElement(String name, String element) {
        // Gets the <field-name>:<field-expression> pair
        Pair pair =
                Split.asPair(element)
                        .orElseThrow(
                                () ->
                                        new ConfigException(
                                                String.format(
                                                        "Invalid value for configuration \"%s\": Each entry must be in the form %s",
                                                        name,
                                                        "[fieldName]:[extractionExpression]")));
        try {
            // Validates <field-expression>
            Expressions.Wrapped(pair.value());
            return pair.key();
        } catch (ExpressionException ee) {
            throw new ConfigException(
                    String.format(
                            "Invalid value for configuration \"%s\": Extraction expression must be in the form %s",
                            name, "#{...}"));
        }
    }

    @Override
    public String toString() {
        return "[fieldName1]:[extractionExpression1],[fieldName2]:[extractionExpression2],...";
    }
}
