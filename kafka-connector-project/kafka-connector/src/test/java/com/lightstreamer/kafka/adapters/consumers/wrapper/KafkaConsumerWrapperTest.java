
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

package com.lightstreamer.kafka.adapters.consumers.wrapper;

import static com.google.common.truth.Truth.assertThat;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandModeStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapper.Concurrency;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapper.Config;
import com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.test_utils.ItemTemplatesUtils;
import com.lightstreamer.kafka.test_utils.Mocks;

import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public class KafkaConsumerWrapperTest {

    private final Mocks.MockMetadataListener metadataListener = new Mocks.MockMetadataListener();

    private KafkaConsumerWrapper mkConsumer(boolean exceptionOnConnection) {
        // Create a trivial consumer configuration
        Config<String, String> config =
                new Config<>(
                        "TestConnection",
                        new Properties(),
                        ItemTemplatesUtils.itemTemplates(
                                "aTopic", "anItemTemplate,anotherItemTemplate"),
                        ItemTemplatesUtils.fieldsExtractor(),
                        OthersSelectorSuppliers.String().deserializers(),
                        RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE,
                        CommandModeStrategy.NONE,
                        new Concurrency(RecordConsumeWithOrderStrategy.ORDER_BY_PARTITION, 1));

        return KafkaConsumerWrapper.create(
                config,
                metadataListener,
                new Mocks.MockItemEventListener(),
                Mocks.MockConsumer.supplier(exceptionOnConnection));
    }

    @Test
    public void shouldStartConsuming() {
        KafkaConsumerWrapper consumer = mkConsumer(false);

        CompletableFuture<Void> consuming1 = consumer.startConsuming(SubscribedItems.nop());
        assertThat(consuming1.isCompletedExceptionally()).isFalse();

        // Invoking startConsuming again should be idempotent
        CompletableFuture<Void> consuming2 = consumer.startConsuming(SubscribedItems.nop());
        assertThat(consuming1).isSameInstanceAs(consuming2);
        assertThat(consumer.isConsuming()).isTrue();

        consuming1.join();
    }

    @Test
    public void shouldStartAndStopConsuming() {
        KafkaConsumerWrapper consumerWrapper = mkConsumer(false);

        CompletableFuture<Void> consuming = consumerWrapper.startConsuming(SubscribedItems.nop());
        assertThat(consuming.isCompletedExceptionally()).isFalse();

        consumerWrapper.stopConsuming();
        assertThat(consumerWrapper.isConsuming()).isFalse();

        // Invoking stopConsuming again should be idempotent
        consumerWrapper.stopConsuming();
        assertThat(consumerWrapper.isConsuming()).isFalse();
    }

    @Test
    public void shouldFailStartDueToKafkaException() {
        KafkaConsumerWrapper consumerWrapper = mkConsumer(true);

        CompletableFuture<Void> consuming = consumerWrapper.startConsuming(SubscribedItems.nop());
        assertThat(consumerWrapper.isConsuming()).isFalse();
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
        Exception ke = assertThrows(CompletionException.class, consuming::join);

        assertThat(ke).hasMessageThat().contains("Simulated Exception");
        assertThat(ke).hasCauseThat().isInstanceOf(KafkaException.class);
    }

    @Test
    public void shouldHaveConfig() {
        KafkaConsumerWrapper consumerWrapper = mkConsumer(true);
        Config<?, ?> config = consumerWrapper.config();
        assertThat(config).isNotNull();
        assertThat(config.connectionName()).isEqualTo("TestConnection");
    }
}
