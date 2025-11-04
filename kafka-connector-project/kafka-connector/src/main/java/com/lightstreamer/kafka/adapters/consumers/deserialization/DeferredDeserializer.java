
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

import java.util.Map;

/**
 * A wrapper deserializer that defers the actual deserialization process until the value is needed.
 * 
 * <p>This deserializer wraps another deserializer and returns a {@link Deferred} object instead of
 * performing immediate deserialization. The actual deserialization is postponed until the deferred
 * value is accessed, which can help optimize performance by avoiding unnecessary deserialization
 * operations for data that might not be used.</p>
 * 
 * @param <T> the type of object that will be deserialized by the inner deserializer
 * 
 * @see Deserializer
 * @see Deferred
 */
public class DeferredDeserializer<T> implements Deserializer<Deferred<T>> {

    private final Deserializer<T> innerDeserializer;

    public DeferredDeserializer(Deserializer<T> innerDeserializer) {
        this.innerDeserializer = innerDeserializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        innerDeserializer.configure(configs, isKey);
    }

    @Override
    public Deferred<T> deserialize(String topic, byte[] data) {
        return Deferred.lazy(innerDeserializer, topic, data);
    }

    @Override
    public void close() {
        innerDeserializer.close();
    }
}
