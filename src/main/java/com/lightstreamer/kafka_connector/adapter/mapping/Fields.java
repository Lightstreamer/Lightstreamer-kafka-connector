package com.lightstreamer.kafka_connector.adapter.mapping;

import java.util.Map;

import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Selectors;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Selectors.SelectorsSupplier;

public class Fields {

    public interface FieldMappings<K, V> {

        Selectors<K, V> selectors();

    }

    public static <K, V> FieldMappings<K, V> fieldMappingsFrom(Map<String, String> fieldsMapping,
            SelectorsSupplier<K, V> selectorsSupplier) {

        Selectors<K, V> fieldsSelectors = Selectors.from(selectorsSupplier, "fields", fieldsMapping);
        return new DefaultFieldMappings<>(fieldsSelectors);

    }

    static class DefaultFieldMappings<K, V> implements FieldMappings<K, V> {
        private Selectors<K, V> selectors;

        DefaultFieldMappings(Selectors<K, V> selectors) {
            this.selectors = selectors;
        }

        @Override
        public Selectors<K, V> selectors() {
            return selectors;
        }
    }
}
