package com.lightstreamer.kafka_connector.adapter.consumers;

public record Pair<K, V>(K first, V second) {

    public static <K, V> Pair<K, V> p(K k, V v) {
        return new Pair<>(k, v);
    }
}
