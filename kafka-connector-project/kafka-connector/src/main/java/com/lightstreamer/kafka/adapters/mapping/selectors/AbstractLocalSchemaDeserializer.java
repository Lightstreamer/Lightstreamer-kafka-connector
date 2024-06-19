
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

import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.config.ConfigException;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.File;
import java.nio.file.Path;

public abstract class AbstractLocalSchemaDeserializer<T> implements Deserializer<T> {

    protected final File schemaFile;

    protected AbstractLocalSchemaDeserializer(ConnectorConfig config, boolean isKey) {
        String schemaFileKey =
                isKey
                        ? ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_PATH
                        : ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_PATH;
        String schemaFileName = config.getFile(schemaFileKey);
        if (schemaFileName == null) {
            // Neve happens
            throw new ConfigException(schemaFileKey + " setting is mandatory");
        }
        Path path = Path.of(schemaFileName);
        schemaFile = path.toFile();
    }
}
