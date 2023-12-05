package com.lightstreamer.kafka_connector.adapter.evaluator.selectors;

import java.util.Properties;

public abstract class AbstractSelectorSupplier<S> {

    private static final String KEY_SCHEMA_FILE = "key.schema.file";

    private static final String VALUE_SCHEMA_FILE = "value.schema.file";

    private static final String SCHEMA_REGISTRY_URL = "schema.registry.url";

    protected abstract Class<?> getLocalSchemaDeserializer();

    protected abstract Class<?> getSchemaDeserializer();

    public String deserializer(boolean isKey, Properties props) {
        if (isKey) {
            return getDeserializer(props.get(KEY_SCHEMA_FILE) != null);
        } else {
            return getDeserializer(props.get(VALUE_SCHEMA_FILE) != null);
        }
    }

    private String getDeserializer(boolean isLocal) {
        if (isLocal) {
            return getLocalSchemaDeserializer().getName();
        } else {
            return getSchemaDeserializer().getName();
        }
    }
}
