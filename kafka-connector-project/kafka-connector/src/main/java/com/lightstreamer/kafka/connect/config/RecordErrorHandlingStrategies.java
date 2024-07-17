
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

import com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.RecordErrorHandlingStrategy;

import org.apache.kafka.common.config.ConfigDef.Recommender;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;

import java.util.List;
import java.util.Map;

public class RecordErrorHandlingStrategies {

    public static Validator VALIDATOR = new RecordErrorHandlingStrategyValidator();

    public static Recommender RECOMMENDER = new RecordErrorHandlingStrategyRecommender();

    private RecordErrorHandlingStrategies() {}

    private static class RecordErrorHandlingStrategyValidator implements Validator {

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
                                "Invalid value for configuration \"%s\": Must be a non-empty string",
                                name));
            }

            RecordErrorHandlingStrategy strategy = RecordErrorHandlingStrategy.from(strValue);
            if (strategy == null) {
                throw new ConfigException(
                        String.format(
                                "Invalid value for configuration \"%s\": Must be a valid strategy",
                                name));
            }
        }
    }

    private static class RecordErrorHandlingStrategyRecommender implements Recommender {

        @Override
        public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
            return RecordErrorHandlingStrategy.recommended();
        }

        @Override
        public boolean visible(String name, Map<String, Object> parsedConfig) {
            return true;
        }
    }
}
