
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

package com.lightstreamer.kafka.adapters.consumers;

import static com.google.common.truth.Truth.assertThat;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.interfaces.data.SubscriptionException;
import com.lightstreamer.kafka.adapters.ConnectorConfigurator.ConsumerLoopConfig;
import com.lightstreamer.kafka.adapters.commons.MetadataListener;
import com.lightstreamer.kafka.adapters.config.InfoItem;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.common.config.TopicConfigurations;
import com.lightstreamer.kafka.common.config.TopicConfigurations.ItemTemplateConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations.TopicMappingConfig;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.Item;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.ValuesExtractor;
import com.lightstreamer.kafka.test_utils.TestSelectorSuppliers;

import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

class TestMetadataListener implements MetadataListener {

    @Override
    public void forceUnsubscription(String item) {}

    @Override
    public void forceUnsubscriptionAll() {}
}

class TestConsumerLoopTest extends AbstractConsumerLoop<String, String> {

    TestConsumerLoopTest(ConsumerLoopConfig<String, String> config) {
        super(config);
    }

    @Override
    void stopConsuming() {}

    @Override
    public void subscribeInfoItem(InfoItem itemHandle) {}

    @Override
    public void unsubscribeInfoItem() {}

    @Override
    void startConsuming() throws SubscriptionException {}
}

class TestLoopConfig implements ConsumerLoopConfig<String, String> {

    private final TopicConfigurations topicsConfig;

    TestLoopConfig(TopicConfigurations topicsConfig) {
        this.topicsConfig = topicsConfig;
    }

    @Override
    public Properties consumerProperties() {
        return new Properties();
    }

    @Override
    public ValuesExtractor<String, String> fieldsExtractor() {
        throw new UnsupportedOperationException("Unimplemented method 'fieldMappings'");
    }

    @Override
    public ItemTemplates<String, String> itemTemplates() {
        try {
            return Items.from(topicsConfig, TestSelectorSuppliers.string());
        } catch (ExtractionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Deserializer<String> keyDeserializer() {
        throw new UnsupportedOperationException("Unimplemented method 'keyDeserializer'");
    }

    @Override
    public Deserializer<String> valueDeserializer() {
        throw new UnsupportedOperationException("Unimplemented method 'valueDeserializer'");
    }

    @Override
    public RecordErrorHandlingStrategy recordErrorHandlingStrategy() {
        return RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE;
    }

    @Override
    public String connectionName() {
        return "TestConnection";
    }
}

public class ConsumerLoopTest {

    private static TestConsumerLoopTest consumerLoopTest() {
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(
                        ItemTemplateConfigs.empty(),
                        List.of(TopicMappingConfig.from("aTopic", "anItemTemplate")));
        ConsumerLoopConfig<String, String> c = new TestLoopConfig(topicsConfig);

        TestConsumerLoopTest consumerLoopTest = new TestConsumerLoopTest(c);
        return consumerLoopTest;
    }

    @Test
    public void shouldSubscribe() throws SubscriptionException {
        TestConsumerLoopTest consumerLoopTest = consumerLoopTest();
        assertThat(consumerLoopTest.getItemsCounter()).isEqualTo(0);
        Object itemHandle = new Object();
        Item item = consumerLoopTest.subscribe("anItemTemplate", itemHandle);
        assertThat(item).isEqualTo(Items.susbcribedFrom("anItemTemplate", itemHandle));
        assertThat(consumerLoopTest.getItemsCounter()).isEqualTo(1);
    }

    @Test
    public void shouldNotSubscribe() {
        TestConsumerLoopTest consumerLoopTest = consumerLoopTest();
        Object itemHandle = new Object();
        assertThrows(
                SubscriptionException.class,
                () -> consumerLoopTest.subscribe("unregisteredTemplate", itemHandle));
    }

    @Test
    public void shouldUnsubscribe() throws SubscriptionException {
        TestConsumerLoopTest consumerLoopTest = consumerLoopTest();
        Object itemHandle = new Object();
        Item item = consumerLoopTest.subscribe("anItemTemplate", itemHandle);
        Item removed = consumerLoopTest.unsubscribe("anItemTemplate");
        assertThat(removed).isSameInstanceAs(item);
        assertThat(consumerLoopTest.getItemsCounter()).isEqualTo(0);
    }

    @Test
    public void shouldNotUnsubscribe() {
        TestConsumerLoopTest consumerLoopTest = consumerLoopTest();
        assertThrows(
                SubscriptionException.class, () -> consumerLoopTest.unsubscribe("anItemTemplate"));
    }
}
