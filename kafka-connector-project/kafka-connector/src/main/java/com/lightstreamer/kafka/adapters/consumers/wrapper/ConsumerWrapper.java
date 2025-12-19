
/*
 * Copyright (C) 2024 Lightstreamer Srl
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

package com.lightstreamer.kafka.adapters.consumers.wrapper;

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.kafka.adapters.commons.MetadataListener;
import com.lightstreamer.kafka.adapters.consumers.ConsumerTrigger.ConsumerTriggerConfig;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

public interface ConsumerWrapper<K, V> extends Runnable {

    void consumeRecords(ConsumerRecords<K, V> records);

    default void close() {}

    interface AdminInterface extends AutoCloseable {

        static AdminInterface newAdmin(Properties properties) {
            return new DefaultAdminInterface(properties);
        }

        Set<String> listTopics(ListTopicsOptions options) throws Exception;

        static class DefaultAdminInterface implements AdminInterface {

            private final Admin admin;

            DefaultAdminInterface(Properties properties) {
                admin = Admin.create(properties);
            }

            @Override
            public Set<String> listTopics(ListTopicsOptions options) throws Exception {
                return admin.listTopics(options).names().get();
            }

            @Override
            public void close() throws Exception {
                admin.close();
            }
        }
    }

    static <K, V> ConsumerWrapper<K, V> create(
            ConsumerTriggerConfig<K, V> config,
            ItemEventListener eventListener,
            MetadataListener metadataListener,
            SubscribedItems subscribedItems) {
        return create(
                config,
                eventListener,
                metadataListener,
                subscribedItems,
                () ->
                        new KafkaConsumer<>(
                                config.consumerProperties(),
                                config.suppliers().keySelectorSupplier().deserializer(),
                                config.suppliers().valueSelectorSupplier().deserializer()),
                AdminInterface::newAdmin);
    }

    static <K, V> ConsumerWrapper<K, V> create(
            ConsumerTriggerConfig<K, V> config,
            ItemEventListener eventListener,
            MetadataListener metadataListener,
            SubscribedItems subscribedItems,
            Supplier<Consumer<K, V>> consumerSupplier) {
        return create(
                config,
                eventListener,
                metadataListener,
                subscribedItems,
                consumerSupplier,
                AdminInterface::newAdmin);
    }

    static <K, V> ConsumerWrapper<K, V> create(
            ConsumerTriggerConfig<K, V> config,
            ItemEventListener eventListener,
            MetadataListener metadataListener,
            SubscribedItems subscribedItems,
            Supplier<Consumer<K, V>> consumerSupplier,
            Function<Properties, AdminInterface> adminFactory) {
        return new ConsumerWrapperImpl<>(
                config,
                eventListener,
                metadataListener,
                subscribedItems,
                consumerSupplier,
                adminFactory);
    }
}
