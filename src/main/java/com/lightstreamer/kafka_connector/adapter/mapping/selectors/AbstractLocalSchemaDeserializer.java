package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import java.io.File;
import java.nio.file.Paths;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import com.lightstreamer.kafka_connector.adapter.config.ConnectorConfig;

public abstract class AbstractLocalSchemaDeserializer<T> implements Deserializer<T> {

    protected final File schemaFile;

    protected AbstractLocalSchemaDeserializer(ConnectorConfig config, boolean isKey) {
        String schemaFileKey = isKey ? ConnectorConfig.KEY_SCHEMA_FILE : ConnectorConfig.VALUE_SCHEMA_FILE;
        String fileSchema = config.getText(schemaFileKey);
        if (fileSchema == null) {
            throw new SerializationException(schemaFileKey + " setting is mandatory");
        }
        schemaFile = Paths.get((String) config.getDirectory(ConnectorConfig.ADAPTER_DIR), fileSchema).toFile();
    }

}
