
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
import com.lightstreamer.kafka.adapters.consumers.SubscriptionsHandlerSupport.DefaultSubscriptionsHandler;
import com.lightstreamer.kafka.adapters.consumers.SubscriptionsHandlerSupport.NOPSubscriptionsHandler;
import com.lightstreamer.kafka.adapters.consumers.trigger.ConsumerTrigger.Concurrency;
import com.lightstreamer.kafka.adapters.consumers.trigger.ConsumerTrigger.ConsumerTriggerConfig;
import com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers;
import com.lightstreamer.kafka.test_utils.ItemTemplatesUtils;
import com.lightstreamer.kafka.test_utils.Mocks;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

public class SubscriptionsHandlerTest {

    private ConsumerTriggerConfig<String, String> config;
    private Mocks.MockMetadataListener metadataListener = new Mocks.MockMetadataListener();
    private Mocks.MockItemEventListener eventListener = new Mocks.MockItemEventListener();

    @BeforeEach
    public void before() {
        this.config =
                new ConsumerTriggerConfig<>(
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

    @Test
    public void shouldCreateDefaultSubscriptionsHandler() {
        SubscriptionsHandler subscriptionsHandler =
                SubscriptionsHandler.create(config, metadataListener, eventListener, false);
        assertThat(subscriptionsHandler).isInstanceOf(DefaultSubscriptionsHandler.class);
        assertThat(subscriptionsHandler.isConsuming()).isFalse();
    }

    @Test
    public void shouldCreateNopSubscriptionsHandler() {
        SubscriptionsHandler subscriptionsHandler =
                SubscriptionsHandler.create(
                        config, metadataListener, items -> new Mocks.MockConsumerWrapper<>(), true);
        assertThat(subscriptionsHandler).isInstanceOf(NOPSubscriptionsHandler.class);
        assertThat(subscriptionsHandler.isConsuming()).isTrue();
    }
}
