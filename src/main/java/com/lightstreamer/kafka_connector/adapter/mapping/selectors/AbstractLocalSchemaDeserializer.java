package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import java.io.File;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import com.lightstreamer.kafka_connector.adapter.config.ConnectorConfig;

public abstract class AbstractLocalSchemaDeserializer<T> implements Deserializer<T> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        File schema = getFileSchema(isKey ? ConnectorConfig.KEY_SCHEMA_FILE : ConnectorConfig.VALUE_SCHEMA_FILE,
                configs);
        doConfigure(configs, schema, isKey);
    }

    abstract protected void doConfigure(Map<String, ?> configs, File schema, boolean isKey);

    private File getFileSchema(String setting, Map<String, ?> configs) {
        Object fileSchema = configs.get(setting);
        if (fileSchema == null) {
            throw new SerializationException(setting + " setting is mandatory");
        }
        if (fileSchema instanceof String f) {
            return Paths.get((String) configs.get(ConnectorConfig.ADAPTER_DIR), f).toFile();
        }
        throw new SerializationException("Unable to load schema file " + fileSchema);
    }
}
