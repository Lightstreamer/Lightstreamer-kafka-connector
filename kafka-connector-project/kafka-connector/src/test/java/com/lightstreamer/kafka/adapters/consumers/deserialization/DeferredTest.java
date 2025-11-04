
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

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@DisplayName("Deferred Interface and Implementations")
public class DeferredTest {

    private static final String TEST_TOPIC = "test-topic";

    private byte[] testBytes(String content) {
        return content.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("resolved() factory should create EagerDeferred with non-null data")
    void shouldCreateEagerDeferredWithData() {
        String testData = "test-data";

        Deferred<String> deferred = Deferred.resolved(testData);

        assertThat(deferred.load()).isEqualTo(testData);
        assertThat(deferred.isNull()).isFalse();
        assertThat(deferred.rawBytes()).isEmpty();
    }

    @Test
    @DisplayName("resolved() factory should create EagerDeferred with null data")
    void shouldCreateEagerDeferredWithNullData() {
        Deferred<String> deferred = Deferred.resolved(null);

        assertThat(deferred.load()).isNull();
        assertThat(deferred.isNull()).isTrue();
        assertThat(deferred.rawBytes()).isEmpty();
    }

    @Test
    @DisplayName("lazy() factory should create LazyDeferred")
    void shouldCreateLazyDeferred() {
        // Use wrapped StringDeserializer for realistic byte-to-string conversion
        TestDeserializer deserializer = new TestDeserializer();
        byte[] testData = testBytes("input");

        Deferred<String> deferred = Deferred.lazy(deserializer, TEST_TOPIC, testData);

        assertThat(deferred.rawBytes()).isEqualTo(testData);
        assertThat(deferred.isNull()).isFalse();

        // Verify deserializer hasn't been called yet
        assertThat(deserializer.getCallCount()).isEqualTo(0);

        // Now load and verify deserializer was called
        String result = deferred.load();
        assertThat(result).isEqualTo("input"); // StringDeserializer converts bytes to string
        assertThat(deserializer.getCallCount()).isEqualTo(1);
        assertThat(deserializer.getLastTopic()).isEqualTo(TEST_TOPIC);
        assertThat(deserializer.getLastData()).isEqualTo(testData);
    }

    @Test
    @DisplayName("eager() factory should create EagerDeferred by calling deserializer immediately")
    void shouldCreateEagerDeferredByCallingDeserializerImmediately() {
        // Use wrapped StringDeserializer for realistic byte-to-string conversion
        TestDeserializer deserializer = new TestDeserializer();
        byte[] testData = testBytes("input");

        // eager() should call deserializer immediately during creation
        Deferred<String> deferred = Deferred.eager(deserializer, TEST_TOPIC, testData);

        // Verify deserializer was called during creation
        assertThat(deserializer.getCallCount()).isEqualTo(1);
        assertThat(deserializer.getLastTopic()).isEqualTo(TEST_TOPIC);
        assertThat(deserializer.getLastData()).isEqualTo(testData);

        // Subsequent loads should not call deserializer again
        String result1 = deferred.load();
        String result2 = deferred.load();
        assertThat(result1).isEqualTo("input"); // StringDeserializer converts bytes to string
        assertThat(result2).isEqualTo("input");
        assertThat(deserializer.getCallCount()).isEqualTo(1);

        // Should behave like resolved data (empty raw bytes)
        assertThat(deferred.rawBytes()).isEmpty();
    }

    @Test
    @DisplayName("EagerDeferred should return data immediately without deserializer calls")
    void shouldReturnDataImmediatelyWithEagerDeferred() {
        String testData = "immediate-data";

        Deferred<String> deferred = Deferred.resolved(testData);

        // Multiple calls should return same data
        assertThat(deferred.load()).isEqualTo(testData);
        assertThat(deferred.load()).isEqualTo(testData);
        assertThat(deferred.load()).isEqualTo(testData);

        // Always returns empty bytes for resolved data
        assertThat(deferred.rawBytes()).isEmpty();
    }

    @Test
    @DisplayName("LazyDeferred should cache result and call deserializer only once")
    void shouldCacheResultAndCallDeserializerOnlyOnceWithLazyDeferred() {
        // Use wrapped StringDeserializer for realistic byte-to-string conversion
        TestDeserializer deserializer = new TestDeserializer();
        byte[] testData = testBytes("input");

        Deferred<String> deferred = Deferred.lazy(deserializer, TEST_TOPIC, testData);

        // First load - should call deserializer
        String result1 = deferred.load();
        assertThat(result1).isEqualTo("input"); // StringDeserializer converts bytes to string
        assertThat(deserializer.getCallCount()).isEqualTo(1);

        // Second load - should use cached value (no additional deserializer call)
        String result2 = deferred.load();
        assertThat(result2).isEqualTo("input");
        assertThat(deserializer.getCallCount()).isEqualTo(1);

        // Third load - still using cached value
        String result3 = deferred.load();
        assertThat(result3).isEqualTo("input");
        assertThat(deserializer.getCallCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("LazyDeferred should handle null byte array correctly")
    void shouldHandleNullBytesCorrectlyWithLazyDeferred() {
        // Use wrapped StringDeserializer which returns null for null input
        TestDeserializer deserializer = new TestDeserializer();

        Deferred<String> deferred = Deferred.lazy(deserializer, TEST_TOPIC, null);

        assertThat(deferred.isNull()).isTrue();
        assertThat(deferred.rawBytes()).isNull();

        // load() should call deserializer with null data - StringDeserializer returns null for null
        // input
        String result = deferred.load();
        assertThat(result).isNull(); // StringDeserializer returns null for null data
        assertThat(deserializer.getCallCount()).isEqualTo(1);
        assertThat(deserializer.getLastData()).isNull();
    }

    @Test
    @DisplayName("LazyDeferred should handle empty byte array correctly")
    void shouldHandleEmptyBytesCorrectlyWithLazyDeferred() {
        // Use wrapped StringDeserializer which returns empty string for empty data
        TestDeserializer deserializer = new TestDeserializer();
        byte[] emptyData = new byte[0];

        Deferred<String> deferred = Deferred.lazy(deserializer, TEST_TOPIC, emptyData);

        assertThat(deferred.isNull()).isTrue(); // Empty bytes = null data
        assertThat(deferred.rawBytes()).isEqualTo(emptyData);

        String result = deferred.load();
        assertThat(result).isEqualTo(""); // StringDeserializer returns empty string for empty data
        assertThat(deserializer.getCallCount()).isEqualTo(1);
        assertThat(deserializer.getLastData()).isEqualTo(emptyData);
    }

    @Test
    @DisplayName("LazyDeferred should handle deserializer returning null for empty data")
    void shouldHandleDeserializerReturningNullForEmptyData() {
        // Some deserializers return null for empty/invalid data
        Deserializer<String> nullReturningDeserializer =
                new Deserializer<String>() {
                    @Override
                    public String deserialize(String topic, byte[] data) {
                        return (data == null || data.length == 0)
                                ? null
                                : new String(data, StandardCharsets.UTF_8);
                    }
                };

        byte[] emptyData = new byte[0];
        Deferred<String> deferred = Deferred.lazy(nullReturningDeserializer, TEST_TOPIC, emptyData);

        assertThat(deferred.isNull()).isTrue(); // Empty bytes = null data
        assertThat(deferred.rawBytes()).isEqualTo(emptyData);

        String result = deferred.load();
        assertThat(result).isNull(); // Deserializer returns null for empty data
    }

    @Test
    @DisplayName("LazyDeferred should cache null results correctly")
    void shouldCacheNullResultsCorrectlyWithLazyDeferred() {
        // Create a deserializer that always returns null (to test null caching)
        Deserializer<String> nullDeserializer =
                new Deserializer<String>() {
                    @Override
                    public String deserialize(String topic, byte[] data) {
                        return null; // Always return null to test null result caching
                    }
                };
        TestDeserializer deserializer = new TestDeserializer(nullDeserializer);
        byte[] testData = testBytes("input");

        Deferred<String> deferred = Deferred.lazy(deserializer, TEST_TOPIC, testData);

        // First load - deserializer returns null
        String result1 = deferred.load();
        assertThat(result1).isNull();
        assertThat(deserializer.getCallCount()).isEqualTo(1);

        // Second load - should use cached null value (deserializer not called again)
        // This verifies that null results are properly cached using the separate 'cached' flag
        String result2 = deferred.load();
        assertThat(result2).isNull();
        assertThat(deserializer.getCallCount()).isEqualTo(1); // Still 1, not 2 - cached correctly
    }

    @Test
    @DisplayName("LazyDeferred caching should be thread-safe")
    @Timeout(5)
    void shouldBeCacheThreadSafeWithLazyDeferred() {
        // Use wrapped StringDeserializer for realistic byte-to-string conversion
        TestDeserializer deserializer = new TestDeserializer();
        byte[] testData = testBytes("result");

        Deferred<String> deferred = Deferred.lazy(deserializer, TEST_TOPIC, testData);

        // Simulate concurrent access (this test may occasionally pass even with broken
        // implementation)
        // but demonstrates the need for proper synchronization
        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(2);
        java.util.concurrent.atomic.AtomicInteger results =
                new java.util.concurrent.atomic.AtomicInteger();

        Thread t1 =
                new Thread(
                        () -> {
                            try {
                                latch.countDown();
                                latch.await();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            String result = deferred.load();
                            if ("result".equals(result)) results.incrementAndGet();
                        });

        Thread t2 =
                new Thread(
                        () -> {
                            try {
                                latch.countDown();
                                latch.await();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            String result = deferred.load();
                            if ("result".equals(result)) results.incrementAndGet();
                        });

        t1.start();
        t2.start();

        try {
            t1.join();
            t2.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        assertThat(results.get()).isEqualTo(2);
        // With the current thread-safe implementation using double-checked locking,
        // the deserializer should be called exactly once even under concurrent access
        int callCount = deserializer.getCallCount();
        assertThat(callCount).isEqualTo(1);
    }

    @Test
    @DisplayName("LazyDeferred should propagate deserializer exceptions")
    void shouldPropagateDeserializerExceptionsWithLazyDeferred() {
        Deserializer<String> failingDeserializer =
                new Deserializer<String>() {
                    @Override
                    public String deserialize(String topic, byte[] data) {
                        throw new RuntimeException("Deserialization failed");
                    }
                };

        Deferred<String> deferred =
                Deferred.lazy(failingDeserializer, TEST_TOPIC, testBytes("data"));

        RuntimeException exception = assertThrows(RuntimeException.class, deferred::load);
        assertThat(exception.getMessage()).isEqualTo("Deserialization failed");
    }

    @Test
    @DisplayName("LazyDeferred should preserve topic and data parameters correctly")
    void shouldPreserveParametersCorrectlyWithLazyDeferred() {
        // Use wrapped StringDeserializer for realistic byte-to-string conversion
        TestDeserializer deserializer = new TestDeserializer();
        String customTopic = "custom-topic";
        byte[] customData = testBytes("custom-data");

        Deferred<String> deferred = Deferred.lazy(deserializer, customTopic, customData);

        deferred.load();

        assertThat(deserializer.getLastTopic()).isEqualTo(customTopic);
        assertThat(deserializer.getLastData()).isEqualTo(customData);
    }

    @Test
    @DisplayName("Different Deferred instances should be independent")
    void shouldBeIndependentWithDifferentDeferredInstances() {
        // Create custom deserializers that return specific results for this test
        Deserializer<String> deserializer1Impl = (topic, data) -> "result1";
        Deserializer<String> deserializer2Impl = (topic, data) -> "result2";

        TestDeserializer deserializer1 = new TestDeserializer(deserializer1Impl);
        TestDeserializer deserializer2 = new TestDeserializer(deserializer2Impl);

        Deferred<String> eager = Deferred.resolved("eager-data");
        Deferred<String> lazy1 = Deferred.lazy(deserializer1, "topic1", testBytes("data1"));
        Deferred<String> lazy2 = Deferred.lazy(deserializer2, "topic2", testBytes("data2"));

        // Load all
        assertThat(eager.load()).isEqualTo("eager-data");
        assertThat(lazy1.load()).isEqualTo("result1");
        assertThat(lazy2.load()).isEqualTo("result2");

        // Verify independence
        assertThat(deserializer1.getCallCount()).isEqualTo(1);
        assertThat(deserializer2.getCallCount()).isEqualTo(1);
        assertThat(deserializer1.getLastTopic()).isEqualTo("topic1");
        assertThat(deserializer2.getLastTopic()).isEqualTo("topic2");
    }

    @Test
    @DisplayName("isNull() should work correctly for different data types")
    @Timeout(5)
    void shouldWorkCorrectlyForDifferentTypesWithIsNull() {
        // Eager - null
        assertThat(Deferred.resolved((String) null).isNull()).isTrue();
        assertThat(Deferred.resolved((Integer) null).isNull()).isTrue();

        // Eager - non-null
        assertThat(Deferred.resolved("data").isNull()).isFalse();
        assertThat(Deferred.resolved(42).isNull()).isFalse();
        assertThat(Deferred.resolved("").isNull()).isFalse(); // empty string is not null

        // Lazy - null bytes
        // Use wrapped StringDeserializer for realistic behavior
        TestDeserializer deserializer = new TestDeserializer();
        assertThat(Deferred.lazy(deserializer, TEST_TOPIC, null).isNull()).isTrue();

        // Lazy - empty bytes
        assertThat(Deferred.lazy(deserializer, TEST_TOPIC, new byte[0]).isNull()).isTrue();

        // Lazy - non-empty bytes
        assertThat(Deferred.lazy(deserializer, TEST_TOPIC, testBytes("data")).isNull()).isFalse();
    }

    /** Test wrapper that adds call counting to a real deserializer */
    private static class TestDeserializer implements Deserializer<String> {
        private final Deserializer<String> delegate;
        private final AtomicInteger callCount = new AtomicInteger(0);
        private final AtomicReference<String> lastTopic = new AtomicReference<>();
        private final AtomicReference<byte[]> lastData = new AtomicReference<>();

        TestDeserializer(Deserializer<String> delegate) {
            this.delegate = delegate;
        }

        // Convenience constructor for StringDeserializer
        TestDeserializer() {
            this(new org.apache.kafka.common.serialization.StringDeserializer());
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            delegate.configure(configs, isKey);
        }

        @Override
        public String deserialize(String topic, byte[] data) {
            callCount.incrementAndGet();
            lastTopic.set(topic);
            lastData.set(data);
            return delegate.deserialize(topic, data);
        }

        @Override
        public void close() {
            delegate.close();
        }

        int getCallCount() {
            return callCount.get();
        }

        String getLastTopic() {
            return lastTopic.get();
        }

        byte[] getLastData() {
            return lastData.get();
        }
    }
}
