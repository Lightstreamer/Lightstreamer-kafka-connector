package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

public interface Suppliers<K, V> {

    KeySelectorSupplier<K> keySelectorSupplier();

    ValueSelectorSupplier<V> valueSelectorSupplier();

    static <K, V> Suppliers<K, V> suppliers() {
        return null;

    }
}
