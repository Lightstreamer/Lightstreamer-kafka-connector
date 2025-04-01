
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

package com.lightstreamer.kafka.adapters.mapping.selectors;

import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_PATH;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_PATH;

import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.common.config.ConfigException;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.File;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Supplier;

public abstract class AbstractLocalSchemaDeserializer<T> implements Deserializer<T> {

    private String keySchemaFileName;
    private String valueSchemaFileName;

    public void preConfigure(ConnectorConfig config) {
        keySchemaFileName = config.getFile(RECORD_KEY_EVALUATOR_SCHEMA_PATH);
        valueSchemaFileName = config.getFile(RECORD_VALUE_EVALUATOR_SCHEMA_PATH);
    }

    public final File getSchemaFile(boolean isKey) {
        if (isKey) {
            return keySchemaFile().orElseThrow(getException(RECORD_KEY_EVALUATOR_SCHEMA_PATH));
        }
        return valueSchemaFile().orElseThrow(getException(RECORD_VALUE_EVALUATOR_SCHEMA_PATH));
    }

    private Supplier<ConfigException> getException(String messagePrefix) {
        return () -> new ConfigException(messagePrefix + " setting is mandatory");
    }

    private Optional<File> keySchemaFile() {
        return schemaFile(keySchemaFileName);
    }

    private Optional<File> valueSchemaFile() {
        return schemaFile(valueSchemaFileName);
    }

    private Optional<File> schemaFile(String schemaFile) {
        if (schemaFile == null) { // Actually never happens
            return Optional.empty();
        }

        return Optional.of(Path.of(schemaFile).toFile());
    }
}
