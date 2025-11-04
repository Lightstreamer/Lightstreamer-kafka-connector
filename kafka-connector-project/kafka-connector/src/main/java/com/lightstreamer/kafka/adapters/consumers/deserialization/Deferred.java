
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

import org.apache.kafka.common.serialization.Deserializer;

/**
 * A deferred loading interface that provides lazy evaluation capabilities for deserialized data.
 * <p>
 * This interface allows for delayed deserialization of data, enabling performance optimizations
 * by deferring expensive deserialization operations until the data is actually needed.
 * </p>
 * 
 * @param <T> the type of data that will be loaded/deserialized
 */
public interface Deferred<T> {

    /**
     * Loads and returns the deferred value.
     * This method triggers the actual loading/computation of the value that was
     * deferred until this point.
     *
     * @return the loaded value of type T
     */
    T load();

    /**
     * Checks if the deferred value is {@code null}.
     *
     * @return {@code true} if the deferred value is {@code null}, {@code false} otherwise
     */
    boolean isNull();

    /**
     * Returns the raw byte array representation of the deferred value.
     * <p>
     * This method provides access to the underlying byte data that represents
     * the deferred object in its serialized form. The returned byte array
     * should be treated as immutable to avoid unintended side effects.
     * </p>
     *
     * @return the raw byte array containing the serialized data, or {@code null} if
     *         no data is available
     */
    byte[] rawBytes();

    /**
     * Creates a new {@code Deferred} instance that is already resolved with the provided data.
     * <p>
     * This factory method returns an eagerly evaluated deferred object that immediately
     * contains the specified value, bypassing any asynchronous computation or lazy evaluation.
     * 
     * @param <T> the type of the data contained in the deferred object
     * @param data the value to wrap in the resolved deferred instance
     * @return a {@code Deferred} instance that is already resolved with the given data
     */
    static <T> Deferred<T> resolved(T data) {
        return new EagerDeferred<>(data);
    }

    /**
     * Creates an eager deferred value that immediately deserializes the provided raw bytes
     * using the specified deserializer.
     * 
     * <p>This method performs the deserialization operation immediately and wraps the result
     * in an {@code EagerDeferred} instance. The deserialization happens at the time this
     * method is called, not when the deferred value is accessed.</p>
     * 
     * @param <T> the type of the deserialized object
     * @param deserializer the deserializer to use for converting the raw bytes
     * @param topic the Kafka topic name, used as context for deserialization
     * @param raw the raw byte array to deserialize
     * @return a {@code Deferred} instance containing the eagerly deserialized value
     * @throws SerializationException if deserialization fails
     */
    static <T> Deferred<T> eager(Deserializer<T> deserializer, String topic, byte[] raw) {
        return new EagerDeferred<>(deserializer.deserialize(topic, raw));
    }

    /**
     * Creates a deferred deserialization instance that performs lazy evaluation.
     * The actual deserialization is postponed until the value is explicitly requested,
     * allowing for efficient memory usage and processing optimization.
     *
     * @param <T> the type of object to be deserialized
     * @param deserializer the deserializer to use for converting the raw bytes
     * @param topic the Kafka topic name associated with the data
     * @param raw the raw byte array containing the serialized data
     * @return a {@code LazyDeferred} instance that will deserialize the data on demand
     */
    static <T> Deferred<T> lazy(Deserializer<T> deserializer, String topic, byte[] raw) {
        return new LazyDeferred<>(deserializer, topic, raw);
    }
}

class EagerDeferred<T> implements Deferred<T> {

    private static final byte[] EMPTY_BYTES = new byte[0];

    private final T data;

    EagerDeferred(T data) {
        this.data = data;
    }

    @Override
    public T load() {
        return data;
    }

    @Override
    public boolean isNull() {
        return data == null;
    }

    @Override
    public byte[] rawBytes() {
        return EMPTY_BYTES;
    }
}

class LazyDeferred<T> implements Deferred<T> {

    private final Deserializer<T> deserializer;
    private final String topic;
    private final byte[] rawBytes;
    private volatile boolean cached = false;
    private volatile T cachedValue;

    LazyDeferred(Deserializer<T> deserializer, String topic, byte[] rawBytes) {
        this.deserializer = deserializer;
        this.topic = topic;
        this.rawBytes = rawBytes;
    }

    @Override
    public boolean isNull() {
        return rawBytes == null || rawBytes.length == 0;
    }

    @Override
    public T load() {
        if (!cached) {
            synchronized (this) {
                if (!cached) {
                    cachedValue = deserializer.deserialize(topic, rawBytes);
                    cached = true;
                }
            }
        }
        return cachedValue;
    }

    @Override
    public byte[] rawBytes() {
        return rawBytes;
    }
}
