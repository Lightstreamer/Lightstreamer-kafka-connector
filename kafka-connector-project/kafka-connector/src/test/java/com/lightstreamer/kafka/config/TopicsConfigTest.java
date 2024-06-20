
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

package com.lightstreamer.kafka.config;

import static com.google.common.truth.Truth.assertThat;

import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.test_utils.ConnectorConfigProvider;
import com.lightstreamer.kafka.config.TopicsConfig.ItemReference;
import com.lightstreamer.kafka.config.TopicsConfig.TopicConfiguration;
import com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopicsConfigTest {

    @Test
    void shouldConfigOneToOneTemplate() {
        Map<String, String> updatedConfigs = new HashMap<>();
        updatedConfigs.put("map.topic.to", "item-template.template1");
        updatedConfigs.put("item-template.template1", "item");
        ConnectorConfig cgg = ConnectorConfigProvider.minimalWith(updatedConfigs);

        TopicsConfig topicConfig =
                TopicsConfig.of(
                        cgg.getValues(ConnectorConfig.ITEM_TEMPLATE, true),
                        cgg.getValues(ConnectorConfig.TOPIC_MAPPING, true));

        List<TopicConfiguration> configurations = topicConfig.configurations();
        assertThat(configurations).hasSize(1);

        TopicConfiguration topicConfiguration = configurations.get(0);
        assertThat(topicConfiguration.topic()).isEqualTo("topic");

        ItemReference itemReference = topicConfiguration.itemReference();
        assertThat(itemReference.isTemplate()).isTrue();
        assertThat(itemReference.templateKey()).isEqualTo("template1");
        assertThat(itemReference.templateValue()).isEqualTo("item");
        assertThat(itemReference.itemName()).isNull();
    }

    @Test
    void shouldConfigOneToOneTemplate2() {
        Map<String, String> updatedConfigs = new HashMap<>();
        updatedConfigs.put("topic.mappings", "topic:item-template.template1");
        updatedConfigs.put("item.templates", "template1:item");
        LightstreamerConnectorConfig config = new LightstreamerConnectorConfig(updatedConfigs);

        TopicsConfig topicConfig =
                TopicsConfig.of(config.getItemTemplates(), config.getTopicMappings());

        List<TopicConfiguration> configurations = topicConfig.configurations();
        assertThat(configurations).hasSize(1);

        TopicConfiguration topicConfiguration = configurations.get(0);
        assertThat(topicConfiguration.topic()).isEqualTo("topic");

        ItemReference itemReference = topicConfiguration.itemReference();
        assertThat(itemReference.isTemplate()).isTrue();
        assertThat(itemReference.templateKey()).isEqualTo("template1");
        assertThat(itemReference.templateValue()).isEqualTo("item");
        assertThat(itemReference.itemName()).isNull();
    }

    @Test
    void shouldConfigOneToOneItem() {
        Map<String, String> updatedConfigs = new HashMap<>();
        updatedConfigs.put("map.topic.to", "simple-item");
        ConnectorConfig cgg = ConnectorConfigProvider.minimalWith(updatedConfigs);
        TopicsConfig topicConfig =
                TopicsConfig.of(
                        cgg.getValues(ConnectorConfig.ITEM_TEMPLATE, true),
                        cgg.getValues(ConnectorConfig.TOPIC_MAPPING, true));

        List<TopicConfiguration> configurations = topicConfig.configurations();
        assertThat(configurations).hasSize(1);

        TopicConfiguration topicConfiguration = configurations.get(0);
        assertThat(topicConfiguration.topic()).isEqualTo("topic");

        ItemReference itemReference = topicConfiguration.itemReference();
        assertThat(itemReference.isTemplate()).isFalse();
        assertThat(itemReference.itemName()).isEqualTo("simple-item");
        assertThat(itemReference.templateKey()).isNull();
        assertThat(itemReference.templateValue()).isNull();
    }

    @Test
    void shouldConfigOneToManyTemplates() {
        Map<String, String> udpatedConfigs = new HashMap<>();
        udpatedConfigs.put("map.topic.to", "item-template.template1,item-template.template2");
        udpatedConfigs.put("item-template.template1", "item1");
        udpatedConfigs.put("item-template.template2", "item2");
        ConnectorConfig cgg = ConnectorConfigProvider.minimalWith(udpatedConfigs);
        TopicsConfig topicConfig =
                TopicsConfig.of(
                        cgg.getValues(ConnectorConfig.ITEM_TEMPLATE, true),
                        cgg.getValues(ConnectorConfig.TOPIC_MAPPING, true));

        List<TopicConfiguration> configurations = topicConfig.configurations();
        assertThat(configurations).hasSize(2);

        TopicConfiguration topicConfiguration1 = configurations.get(0);
        assertThat(topicConfiguration1.topic()).isEqualTo("topic");

        ItemReference itemReference1 = topicConfiguration1.itemReference();
        assertThat(itemReference1.isTemplate()).isTrue();
        assertThat(itemReference1.templateKey()).isEqualTo("template1");
        assertThat(itemReference1.templateValue()).isEqualTo("item1");
        assertThat(itemReference1.itemName()).isNull();

        TopicConfiguration topicConfiguration2 = configurations.get(1);
        assertThat(topicConfiguration2.topic()).isEqualTo("topic");

        ItemReference itemReference2 = topicConfiguration2.itemReference();
        assertThat(itemReference2.isTemplate()).isTrue();
        assertThat(itemReference2.templateKey()).isEqualTo("template2");
        assertThat(itemReference2.templateValue()).isEqualTo("item2");
        assertThat(itemReference2.itemName()).isNull();
    }

    @Test
    void shouldConfigOneToManyItems() {
        Map<String, String> udpatedConfigs = new HashMap<>();
        udpatedConfigs.put("map.topic.to", "item1,item2");
        ConnectorConfig cgg = ConnectorConfigProvider.minimalWith(udpatedConfigs);
        TopicsConfig topicConfig =
                TopicsConfig.of(
                        cgg.getValues(ConnectorConfig.ITEM_TEMPLATE, false),
                        cgg.getValues(ConnectorConfig.TOPIC_MAPPING, true));

        List<TopicConfiguration> configurations = topicConfig.configurations();
        assertThat(configurations).hasSize(2);

        TopicConfiguration topicConfiguration1 = configurations.get(0);
        assertThat(topicConfiguration1.topic()).isEqualTo("topic");

        ItemReference itemReference1 = topicConfiguration1.itemReference();
        assertThat(itemReference1.isTemplate()).isFalse();
        assertThat(itemReference1.itemName()).isEqualTo("item1");
        assertThat(itemReference1.templateKey()).isNull();
        assertThat(itemReference1.templateValue()).isNull();

        TopicConfiguration topicConfiguration2 = configurations.get(1);
        assertThat(topicConfiguration2.topic()).isEqualTo("topic");

        ItemReference itemReference2 = topicConfiguration2.itemReference();
        assertThat(itemReference2.isTemplate()).isFalse();
        assertThat(itemReference2.itemName()).isEqualTo("item2");
        assertThat(itemReference2.templateKey()).isNull();
        assertThat(itemReference2.templateValue()).isNull();
    }

    @Test
    void shouldConfigOneToManyIndenticalTemplates() {
        Map<String, String> udpatedConfigs = new HashMap<>();
        udpatedConfigs.put("item-template.template1", "item1");
        udpatedConfigs.put("map.topic.to", "item-template.template1,item-template.template1");
        ConnectorConfig cgg = ConnectorConfigProvider.minimalWith(udpatedConfigs);
        TopicsConfig topicConfig =
                TopicsConfig.of(
                        cgg.getValues(ConnectorConfig.ITEM_TEMPLATE, true),
                        cgg.getValues(ConnectorConfig.TOPIC_MAPPING, true));

        List<TopicConfiguration> configurations = topicConfig.configurations();
        assertThat(configurations).hasSize(1);

        TopicConfiguration topicConfiguration1 = configurations.get(0);
        assertThat(topicConfiguration1.topic()).isEqualTo("topic");

        ItemReference itemReference = topicConfiguration1.itemReference();
        assertThat(itemReference.isTemplate()).isTrue();
        assertThat(itemReference.templateKey()).isEqualTo("template1");
        assertThat(itemReference.templateValue()).isEqualTo("item1");
        assertThat(itemReference.itemName()).isNull();
    }

    @Test
    void shouldConfigOneToManyIndenticalItems() {
        Map<String, String> udpatedConfigs = new HashMap<>();
        udpatedConfigs.put("map.topic.to", "item1,item1,item2");
        ConnectorConfig cgg = ConnectorConfigProvider.minimalWith(udpatedConfigs);
        TopicsConfig topicConfig =
                TopicsConfig.of(
                        cgg.getValues(ConnectorConfig.ITEM_TEMPLATE, false),
                        cgg.getValues(ConnectorConfig.TOPIC_MAPPING, true));

        List<TopicConfiguration> configurations = topicConfig.configurations();
        assertThat(configurations).hasSize(2);

        TopicConfiguration topicConfiguration1 = configurations.get(0);
        assertThat(topicConfiguration1.topic()).isEqualTo("topic");

        ItemReference itemReference1 = topicConfiguration1.itemReference();
        assertThat(itemReference1.isTemplate()).isFalse();
        assertThat(itemReference1.itemName()).isEqualTo("item1");
        assertThat(itemReference1.templateKey()).isNull();
        assertThat(itemReference1.templateValue()).isNull();

        TopicConfiguration topicConfiguration2 = configurations.get(1);
        assertThat(topicConfiguration2.topic()).isEqualTo("topic");

        ItemReference itemReference2 = topicConfiguration2.itemReference();
        assertThat(itemReference2.isTemplate()).isFalse();
        assertThat(itemReference2.itemName()).isEqualTo("item2");
        assertThat(itemReference2.templateKey()).isNull();
        assertThat(itemReference2.templateValue()).isNull();
    }

    @Test
    void shouldConfigManyToOneTemplate() {
        Map<String, String> updatedConfigs = new HashMap<>();
        updatedConfigs.put("item-template.template1", "item1");
        updatedConfigs.put("map.topic.to", "item-template.template1");
        updatedConfigs.put("map.topic2.to", "item-template.template1");
        ConnectorConfig cgg = ConnectorConfigProvider.minimalWith(updatedConfigs);
        TopicsConfig topicConfig =
                TopicsConfig.of(
                        cgg.getValues(ConnectorConfig.ITEM_TEMPLATE, true),
                        cgg.getValues(ConnectorConfig.TOPIC_MAPPING, true));

        List<TopicConfiguration> configurations = topicConfig.configurations();
        assertThat(configurations).hasSize(2);

        TopicConfiguration topicConfiguration1 = configurations.get(0);
        assertThat(topicConfiguration1.topic()).isEqualTo("topic2");

        TopicConfiguration topicConfiguration2 = configurations.get(1);
        assertThat(topicConfiguration2.topic()).isEqualTo("topic");

        assertThat(topicConfiguration1.itemReference())
                .isEqualTo(topicConfiguration2.itemReference());
    }

    @Test
    void shouldConfigManyToOneItem() {
        Map<String, String> updatedConfigs = new HashMap<>();
        updatedConfigs.put("map.topic.to", "item");
        updatedConfigs.put("map.topic2.to", "item");
        ConnectorConfig cgg = ConnectorConfigProvider.minimalWith(updatedConfigs);
        TopicsConfig topicConfig =
                TopicsConfig.of(
                        cgg.getValues(ConnectorConfig.ITEM_TEMPLATE, true),
                        cgg.getValues(ConnectorConfig.TOPIC_MAPPING, true));

        List<TopicConfiguration> configurations = topicConfig.configurations();
        assertThat(configurations).hasSize(2);

        TopicConfiguration topicConfiguration1 = configurations.get(0);
        assertThat(topicConfiguration1.topic()).isEqualTo("topic2");

        TopicConfiguration topicConfiguration2 = configurations.get(1);
        assertThat(topicConfiguration2.topic()).isEqualTo("topic");

        ItemReference itemReference = topicConfiguration1.itemReference();
        assertThat(itemReference.isTemplate()).isFalse();
        assertThat(itemReference.itemName()).isEqualTo("item");
        assertThat(itemReference.templateKey()).isNull();
        assertThat(itemReference.templateValue()).isNull();
        assertThat(itemReference).isEqualTo(topicConfiguration2.itemReference());
    }
}

