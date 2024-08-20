
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

import com.lightstreamer.kafka.common.utils.Split;
import com.lightstreamer.kafka.common.utils.Split.Pair;

import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TopicMappingsValidator implements Validator {

    @Override
    public void ensureValid(String name, Object value) {
        if (value == null) {
            throw new ConfigException(
                    String.format(
                            "Invalid value for configuration \"%s\": Must be non-null", name));
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
        List<String> topicMappings = Split.bySemicolon(strValue);
        Set<String> keys = new HashSet<>();
        for (String template : topicMappings) {
            String key = validateTopicMapping(name, template);
            if (keys.contains(key)) {
                throw new ConfigException(
                        String.format(
                                "Invalid value for configuration \"%s\": Duplicate key \"%s\"",
                                name, key));
            }
            keys.add(key);
        }
    }

    private String validateTopicMapping(String name, String topicMapping) {
        if (topicMapping.isBlank()) {
            throw new ConfigException(
                    String.format(
                            "Invalid value for configuration \"%s\": Must be a semicolon-separated list of non-empty strings",
                            name));
        }

        // Gets the <topic-name>:<item-mapping> pair
        Pair pair =
                Split.asPair(topicMapping)
                        .orElseThrow(
                                () ->
                                        new ConfigException(
                                                String.format(
                                                        "Invalid value for configuration \"%s\": Each entry must be in the form %s",
                                                        name, "[topicName]:[mappingList]")));

        // Validates <item-mapping>
        List<String> emptyEntryes =
                Split.byComma(pair.value()).stream().filter(String::isBlank).toList();
        if (!emptyEntryes.isEmpty()) {
            throw new ConfigException(
                    String.format(
                            "Invalid value for configuration \"%s\": Mapping list must be in the form %s",
                            name,
                            "[item-template.template1|item1],...,[item-template.templateN|itemN]"));
        }

        // Returns <template-name>
        return pair.key();
    }

    public String toString() {
        return "[topicName1]:[mappingList1],[topicName2]:[mappingList2],...";
    }
}
