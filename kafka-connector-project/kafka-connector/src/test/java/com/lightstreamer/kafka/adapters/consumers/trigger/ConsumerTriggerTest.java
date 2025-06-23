
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

package com.lightstreamer.kafka.adapters.consumers.trigger;

import static com.google.common.truth.Truth.assertThat;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandModeStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.consumers.trigger.ConsumerTrigger.Concurrency;
import com.lightstreamer.kafka.adapters.consumers.trigger.ConsumerTrigger.ConsumerTriggerConfig;
import com.lightstreamer.kafka.adapters.consumers.wrapper.ConsumerWrapper;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.test_utils.ItemTemplatesUtils;
import com.lightstreamer.kafka.test_utils.Mocks;
import com.lightstreamer.kafka.test_utils.Mocks.MockConsumerWrapper;

import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

public class ConsumerTriggerTest {

    private final Mocks.MockMetadataListener metadataListener = new Mocks.MockMetadataListener();
    private MockConsumerWrapper<String, String> kafkaConsumer;

    private ConsumerTrigger mkConsumerTrigger(boolean throwExceptionWhileConnectingToKafka) {
        // Create a trivial consumer trigger configuration
        ConsumerTriggerConfig<String, String> config =
                new ConsumerTriggerConfig<>(
                        "TestConnection",
                        new Properties(),
                        ItemTemplatesUtils.itemTemplates(
                                "aTopic", "anItemTemplate,anotherItemTemplate"),
                        ItemTemplatesUtils.fieldsExtractor(),
                        null,
                        RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE,
                        CommandModeStrategy.NONE,
                        new Concurrency(RecordConsumeWithOrderStrategy.ORDER_BY_PARTITION, 1));

        Function<SubscribedItems, ConsumerWrapper<String, String>> consumerWrapper =
                items -> {
                    if (throwExceptionWhileConnectingToKafka) {
                        throw new KafkaException("Simulated Exception");
                    }
                    return kafkaConsumer;
                };
        return new ConsumerTriggerImpl<>(config, metadataListener, consumerWrapper);
    }

    @BeforeEach
    public void beforeEach() {
        this.kafkaConsumer = new Mocks.MockConsumerWrapper<>();
    }

    @Test
    public void shouldStartConsuming() {
        ConsumerTrigger consumerTrigger = mkConsumerTrigger(false);

        CompletableFuture<Void> consuming1 = consumerTrigger.startConsuming(SubscribedItems.nop());
        assertThat(consuming1.isCompletedExceptionally()).isFalse();

        CompletableFuture<Void> consuming2 = consumerTrigger.startConsuming(SubscribedItems.nop());
        assertThat(consuming1).isSameInstanceAs(consuming2);

        consuming1.join();
        assertThat(kafkaConsumer.hasRan()).isTrue();
        assertThat(kafkaConsumer.isClosed()).isFalse();
    }

    @Test
    public void shouldStartAndStopConsuming() {
        ConsumerTrigger consumerTrigger = mkConsumerTrigger(false);

        CompletableFuture<Void> consuming1 = consumerTrigger.startConsuming(SubscribedItems.nop());
        assertThat(consuming1.isCompletedExceptionally()).isFalse();

        consumerTrigger.stopConsuming();
        assertThat(kafkaConsumer.isClosed()).isTrue();
    }

    @Test
    public void shouldFailStartDueToKafkaException() {
        ConsumerTrigger consumerTrigger = mkConsumerTrigger(true);

        CompletableFuture<Void> consuming1 = consumerTrigger.startConsuming(SubscribedItems.nop());
        assertThat(metadataListener.forcedUnsubscription()).isTrue();
        Exception ke = assertThrows(CompletionException.class, consuming1::join);

        assertThat(ke).hasMessageThat().contains("Simulated Exception");
        assertThat(ke).hasCauseThat().isInstanceOf(KafkaException.class);

        assertThat(kafkaConsumer.hasRan()).isFalse();
        assertThat(kafkaConsumer.isClosed()).isFalse();
    }

    @Test
    public void shouldHaveConfig() {
        ConsumerTrigger consumerTrigger = mkConsumerTrigger(true);
        ConsumerTriggerConfig<?, ?> config = consumerTrigger.config();
        assertThat(config).isNotNull();
        assertThat(config.connectionName()).isEqualTo("TestConnection");
    }
}
