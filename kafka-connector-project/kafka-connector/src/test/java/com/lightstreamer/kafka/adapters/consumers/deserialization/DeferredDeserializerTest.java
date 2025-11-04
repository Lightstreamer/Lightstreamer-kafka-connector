
/*
 * Copyright (C) 2025 Lightstreamer Srl
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

package com.lightstreamer.kafka.adapters.consumers.deserialization;

import static com.google.common.truth.Truth.assertThat;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class DeferredDeserializerTest {

    private final String testTopic = "test-topic";
    private final byte[] testData = testBytes("test-data");

    // Helper method for consistent charset usage
    private byte[] testBytes(String content) {
        return content.getBytes(StandardCharsets.UTF_8);
    }

    // Test deserializer that counts invocations and captures parameters
    private static class CountingDeserializer implements Deserializer<String> {
        private final AtomicInteger configureCount = new AtomicInteger(0);
        private final AtomicInteger deserializeCount = new AtomicInteger(0);
        private final AtomicInteger closeCount = new AtomicInteger(0);
        private final AtomicReference<Map<String, ?>> lastConfigs = new AtomicReference<>();
        private final AtomicReference<Boolean> lastIsKey = new AtomicReference<>();

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            configureCount.incrementAndGet();
            lastConfigs.set(configs);
            lastIsKey.set(isKey);
        }

        @Override
        public String deserialize(String topic, byte[] data) {
            deserializeCount.incrementAndGet();
            return data != null ? new String(data, StandardCharsets.UTF_8) : null;
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
        }

        int getConfigureCount() {
            return configureCount.get();
        }

        int getDeserializeCount() {
            return deserializeCount.get();
        }

        int getCloseCount() {
            return closeCount.get();
        }

        Map<String, ?> getLastConfigs() {
            return lastConfigs.get();
        }

        Boolean getLastIsKey() {
            return lastIsKey.get();
        }
    }

    private CountingDeserializer countingDeserializer;
    private DeferredDeserializer<String> deferredDeserializer;

    @BeforeEach
    void setUp() {
        countingDeserializer = new CountingDeserializer();
        deferredDeserializer = new DeferredDeserializer<>(countingDeserializer);
    }

    @Test
    @DisplayName("Should delegate configuration and lifecycle to inner deserializer")
    void shouldDelegateToInnerDeserializer() {
        Map<String, Object> configs = Map.of("key", "value");

        // Test delegation of configure and close
        deferredDeserializer.configure(configs, true);
        deferredDeserializer.close();

        assertThat(countingDeserializer.getConfigureCount()).isEqualTo(1);
        assertThat(countingDeserializer.getCloseCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("Should defer deserialization until load() is called")
    void shouldDeferDeserializationUntilLoad() {
        // Create deferred - should not trigger deserialization
        Deferred<String> deferred = deferredDeserializer.deserialize(testTopic, testData);

        assertThat(deferred).isNotNull();
        assertThat(countingDeserializer.getDeserializeCount()).isEqualTo(0);

        // Load should trigger actual deserialization
        String result = deferred.load();

        assertThat(result).isEqualTo("test-data");
        assertThat(countingDeserializer.getDeserializeCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("Should preserve raw bytes and cache multiple loads")
    void shouldPreserveRawBytesAndCacheMultipleLoads() {
        Deferred<String> deferred = deferredDeserializer.deserialize(testTopic, testData);

        // Raw bytes should be preserved
        assertThat(deferred.rawBytes()).isEqualTo(testData);

        // Multiple loads should work and use cached result (only one deserializer call)
        String result1 = deferred.load();
        String result2 = deferred.load();

        assertThat(result1).isEqualTo("test-data");
        assertThat(result2).isEqualTo("test-data");
        assertThat(countingDeserializer.getDeserializeCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("Should handle different configuration combinations")
    void shouldHandleDifferentConfigurationCombinations() {
        // Test key deserializer configuration
        Map<String, Object> keyConfigs =
                Map.of("bootstrap.servers", "localhost:9092", "key.deserializer", "string");
        deferredDeserializer.configure(keyConfigs, true);

        assertThat(countingDeserializer.getLastConfigs()).isEqualTo(keyConfigs);
        assertThat(countingDeserializer.getLastIsKey()).isEqualTo(true);

        // Test value deserializer configuration
        Map<String, Object> valueConfigs = Map.of("schema.registry.url", "http://localhost:8081");
        deferredDeserializer.configure(valueConfigs, false);

        assertThat(countingDeserializer.getLastConfigs()).isEqualTo(valueConfigs);
        assertThat(countingDeserializer.getLastIsKey()).isEqualTo(false);
        assertThat(countingDeserializer.getConfigureCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("Should handle large data correctly")
    void shouldHandleLargeData() {
        // Create a larger test string (1KB)
        String largeContent = "x".repeat(1024);
        byte[] largeData = testBytes(largeContent);

        Deferred<String> deferred = deferredDeserializer.deserialize(testTopic, largeData);

        assertThat(deferred.rawBytes()).isEqualTo(largeData);
        String result = deferred.load();
        assertThat(result).isEqualTo(largeContent);
        assertThat(result.length()).isEqualTo(1024);
    }

    @Test
    @DisplayName("Should work with real StringDeserializer")
    void shouldWorkWithRealStringDeserializer() {
        StringDeserializer stringDeserializer = new StringDeserializer();
        try (DeferredDeserializer<String> realDeserializer =
                new DeferredDeserializer<>(stringDeserializer)) {
            String inputText = "Hello, World!";
            byte[] data = inputText.getBytes();

            Deferred<String> deferred = realDeserializer.deserialize(testTopic, data);

            assertThat(deferred.load()).isEqualTo(inputText);
            assertThat(deferred.rawBytes()).isEqualTo(data);
            assertThat(deferred.isNull()).isFalse();
        }
    }
}
