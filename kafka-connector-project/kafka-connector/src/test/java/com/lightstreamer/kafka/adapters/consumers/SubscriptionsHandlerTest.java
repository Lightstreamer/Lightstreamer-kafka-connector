
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

import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandModeStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.consumers.SubscriptionsHandler.AtStartupSubscriptionsHandler;
import com.lightstreamer.kafka.adapters.consumers.SubscriptionsHandler.DefaultSubscriptionsHandler;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapperConfig.Concurrency;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapperConfig.Config;
import com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers;
import com.lightstreamer.kafka.test_utils.ItemTemplatesUtils;
import com.lightstreamer.kafka.test_utils.Mocks;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Properties;

public class SubscriptionsHandlerTest {

    private Config<String, String> config;
    private Mocks.MockMetadataListener metadataListener = new Mocks.MockMetadataListener();

    @BeforeEach
    public void before() {
        this.config =
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
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldCreateDefaultSubscriptionsHandler(boolean allowImplicitItems) {
        SubscriptionsHandler<?, ?> subscriptionsHandler =
                SubscriptionsHandler.builder()
                        .withConsumerConfig(config)
                        .withMetadataListener(metadataListener)
                        .atStartup(false, allowImplicitItems)
                        .build();
        assertThat(subscriptionsHandler).isInstanceOf(DefaultSubscriptionsHandler.class);
        assertThat(subscriptionsHandler.isConsuming()).isFalse();
        // Verify that the allowImplicitItems flag is irrelevant for DefaultSubscriptionsHandler
        assertThat(
                        ((DefaultSubscriptionsHandler<?, ?>) subscriptionsHandler)
                                .getSubscribedItems()
                                .acceptSubscriptions())
                .isEqualTo(true);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldCreateAtSubscriptionsHandler(boolean allowImplicitItems) {
        SubscriptionsHandler<?, ?> subscriptionsHandler =
                SubscriptionsHandler.builder()
                        .withConsumerConfig(config)
                        .withMetadataListener(metadataListener)
                        .atStartup(true, allowImplicitItems)
                        .build();
        assertThat(subscriptionsHandler).isInstanceOf(AtStartupSubscriptionsHandler.class);
        assertThat(subscriptionsHandler.isConsuming()).isFalse();
        AtStartupSubscriptionsHandler<?, ?> atStartupHandler =
                (AtStartupSubscriptionsHandler<?, ?>) subscriptionsHandler;
        assertThat(atStartupHandler.getSubscribedItems().acceptSubscriptions())
                .isEqualTo(!allowImplicitItems);
    }
}
