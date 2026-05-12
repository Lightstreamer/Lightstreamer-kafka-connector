
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

package com.lightstreamer.kafka.adapters.consumers;

import static com.google.common.truth.Truth.assertThat;

import static org.junit.Assert.assertThrows;

import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandModeStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.consumers.ConsumerSettings.ConnectionSpec;
import com.lightstreamer.kafka.adapters.consumers.ConsumerSettings.ConnectionSpec.Concurrency;
import com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.test_utils.ItemTemplatesUtils;
import com.lightstreamer.kafka.test_utils.Mocks;
import com.lightstreamer.kafka.test_utils.Mocks.MockConsumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

public class SubscriptionsHandlerTest {

    private ConnectionSpec<String, String> connectionSpec;
    private Mocks.MockMetadataListener metadataListener = new Mocks.MockMetadataListener();

    @BeforeEach
    public void before() {
        this.connectionSpec =
                new ConnectionSpec<>(
                        "TestConnection",
                        new Properties(),
                        ItemTemplatesUtils.itemTemplates(
                                "aTopic", "anItemTemplate,anotherItemTemplate"),
                        ItemTemplatesUtils.fieldsExtractor(),
                        new KafkaRecord.DeserializerPair<>(
                                OthersSelectorSuppliers.String()
                                        .keySelectorSupplier()
                                        .deserializer(),
                                OthersSelectorSuppliers.String()
                                        .valueSelectorSupplier()
                                        .deserializer()),
                        RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE,
                        CommandModeStrategy.NONE,
                        new Concurrency(RecordConsumeWithOrderStrategy.ORDER_BY_PARTITION, 1));
    }

    @Test
    public void shouldNotBuildSubscriptionsHandler() {
        IllegalStateException ise =
                assertThrows(
                        IllegalStateException.class,
                        () -> SubscriptionsHandler.<String, String>builder().build());
        assertThat(ise).hasMessageThat().isEqualTo("ConnectionSpec not set");

        ise =
                assertThrows(
                        IllegalStateException.class,
                        () -> {
                            SubscriptionsHandler.<String, String>builder()
                                    .withConnectionSpec(connectionSpec)
                                    .build();
                        });
        assertThat(ise).hasMessageThat().isEqualTo("MetadataListener not set");

        ise =
                assertThrows(
                        IllegalStateException.class,
                        () -> {
                            SubscriptionsHandler.<String, String>builder()
                                    .withConnectionSpec(connectionSpec)
                                    .withMetadataListener(metadataListener)
                                    .build();
                        });
        assertThat(ise).hasMessageThat().isEqualTo("ConsumerFactory not set");
    }

    @Test
    public void shouldBuildOnDemandSubscriptionsHandlerWhenSnapshotModeDisabled() {
        SubscriptionsHandler<String, String> subscriptionsHandler =
                SubscriptionsHandler.<String, String>builder()
                        .withConnectionSpec(connectionSpec)
                        .withMetadataListener(metadataListener)
                        .withConsumerFactory(MockConsumer.factory())
                        .build();
        assertThat(subscriptionsHandler)
                .isInstanceOf(SubscriptionsHandler.OnDemandSubscriptionsHandler.class);
        assertThat(subscriptionsHandler.isConsuming()).isFalse();
    }

    @Test
    public void shouldBuildForceableSubscriptionsHandlerWhenSnapshotModeEnabled() {
        SubscriptionsHandler<String, String> subscriptionsHandler =
                SubscriptionsHandler.<String, String>builder()
                        .withConnectionSpec(connectionSpec)
                        .withMetadataListener(metadataListener)
                        .withConsumerFactory(MockConsumer.factory())
                        .withItemSnapshotEnabled(true)
                        .build();
        assertThat(subscriptionsHandler)
                .isInstanceOf(SubscriptionsHandler.ForceableSubscriptionsHandler.class);
    }
}
