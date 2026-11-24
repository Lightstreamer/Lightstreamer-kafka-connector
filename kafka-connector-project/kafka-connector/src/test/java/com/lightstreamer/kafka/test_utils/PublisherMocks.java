
/*
 * Copyright (C) 2026 Lightstreamer Srl
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

package com.lightstreamer.kafka.test_utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka.adapters.producers.ProducerFactory;
import com.lightstreamer.kafka.adapters.pub.messages.ClientMessage;
import com.lightstreamer.kafka.adapters.pub.messages.ClientMessageInfo;
import com.lightstreamer.kafka.adapters.pub.messages.MessageHook;
import com.lightstreamer.kafka.adapters.pub.messages.PublishingException;
import com.lightstreamer.kafka.adapters.pub.messages.converters.PayloadConverter;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.concurrent.Future;
import java.util.function.Supplier;

public class PublisherMocks {

    public static class MockMessageHook implements MessageHook {

        Supplier<PayloadConverter> keyConverterSupplier;
        Supplier<PayloadConverter> valueConverterSupplier;

        PublishingException publishingException;

        public MockMessageHook(
                Supplier<PayloadConverter> keyConverterSupplier,
                Supplier<PayloadConverter> valueConverterSupplier) {
            this.keyConverterSupplier = keyConverterSupplier;
            this.valueConverterSupplier = valueConverterSupplier;
        }

        public MockMessageHook() {
            this(null, null);
        }

        public void setPublishingException(PublishingException publishingException) {
            this.publishingException = publishingException;
        }

        @Override
        public ClientMessage onMessagePublishing(ClientMessageInfo messageInfo)
                throws PublishingException {
            if (publishingException != null) {
                throw publishingException;
            }
            return messageInfo.message();
        }

        @Override
        public PayloadConverter createKeyConverter() {
            if (keyConverterSupplier != null) {
                return keyConverterSupplier.get();
            }
            return null;
        }

        @Override
        public PayloadConverter createValueConverter() {
            if (valueConverterSupplier != null) {
                return valueConverterSupplier.get();
            }
            return null;
        }
    }

    public static class MockProducerFactory implements ProducerFactory {

        final RuntimeException sendException;
        final ProducerListener listener;

        public MockProducerFactory(RuntimeException sendException, ProducerListener listener) {
            this.sendException = sendException;
            this.listener = listener;
        }

        public MockProducerFactory(RuntimeException sendException) {
            this(sendException, null);
        }

        public MockProducerFactory(ProducerListener listener) {
            this(null, listener);
        }

        public MockProducerFactory() {
            this(null, null);
        }

        @Override
        public MockProducer<JsonNode, JsonNode> createProducer(
                Map<String, Object> configs,
                Serializer<JsonNode> keySerializer,
                Serializer<JsonNode> valueSerializer) {
            MockProducer<JsonNode, JsonNode> mockProducer =
                    new MockProducer<>(keySerializer, valueSerializer);
            mockProducer.sendException = sendException;
            mockProducer.listener = listener;
            return mockProducer;
        }
    }

    public static interface ProducerListener {

        void onSent(MockProducer<?, ?> producer, ProducerRecord<?, ?> record);
    }

    public static class MockProducer<K, V>
            extends org.apache.kafka.clients.producer.MockProducer<K, V> {

        ProducerListener listener;

        public MockProducer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
            super(false, null, keySerializer, valueSerializer);
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
            Future<RecordMetadata> future = super.send(record, callback);
            if (listener != null) {
                listener.onSent(this, record);
                return future;
            }
            this.completeNext();
            return future;
        }
    }
}
