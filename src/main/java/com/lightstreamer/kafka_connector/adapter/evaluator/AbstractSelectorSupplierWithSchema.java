package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.Map;

public abstract class AbstractSelectorSupplierWithSchema<S> extends AbstractSelectorSupplier<S> {

    private static final String KEY_SCHEMA_FILE = "key.schema.file";

    private static final String VALUE_SCHEMA_FILE = "value.schema.file";

    private static final String SCHEMA_REGISTRY_URL = "schema.registry.url";

    protected void configKeyDeserializer(Map<String, String> conf) {
        props.put(SCHEMA_REGISTRY_URL, conf.get(SCHEMA_REGISTRY_URL));
        props.put(KEY_SCHEMA_FILE, conf.get(KEY_SCHEMA_FILE));
        super.configKeyDeserializer(conf);
    }

    protected void configValueDeserializer(Map<String, String> conf) {
        props.put(SCHEMA_REGISTRY_URL, conf.get(SCHEMA_REGISTRY_URL));
        props.put(VALUE_SCHEMA_FILE, conf.get(VALUE_SCHEMA_FILE));
        super.configKeyDeserializer(conf);
    }

    protected abstract Class<?> getLocalSchemaDeserializer();

    protected abstract Class<?> getSchemaDeserializer();

    @Override
    public String deserializer(boolean isKey) {
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
