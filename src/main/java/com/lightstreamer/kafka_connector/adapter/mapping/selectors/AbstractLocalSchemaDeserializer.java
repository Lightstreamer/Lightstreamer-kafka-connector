
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

package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import com.lightstreamer.kafka_connector.adapter.config.ConnectorConfig;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public abstract class AbstractLocalSchemaDeserializer<T> implements Deserializer<T> {

    protected final File schemaFile;

    protected AbstractLocalSchemaDeserializer(ConnectorConfig config, boolean isKey) {
        String schemaFileKey =
                isKey ? ConnectorConfig.KEY_SCHEMA_FILE : ConnectorConfig.VALUE_SCHEMA_FILE;
        String fileSchema = config.getText(schemaFileKey);
        if (fileSchema == null) {
            throw new SerializationException(schemaFileKey + " setting is mandatory");
        }
        Path path =
                Paths.get((String) config.getDirectory(ConnectorConfig.ADAPTER_DIR), fileSchema);
        if (!Files.isRegularFile(path)) {
            throw new SerializationException("File [%s] not found".formatted(path));
        }
        schemaFile = path.toFile();
    }
}
