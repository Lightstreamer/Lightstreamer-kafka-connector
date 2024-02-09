
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

package com.lightstreamer.kafka_connector.adapters.config;

import static com.google.common.truth.Truth.assertThat;

import com.lightstreamer.kafka_connector.adapters.config.TopicsConfig.TopicConfiguration;
import com.lightstreamer.kafka_connector.adapters.test_utils.ConnectorConfigProvider;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopicsConfigTest {

    @Test
    void shouldConfigOneToOne() {
        ConnectorConfig cgg = ConnectorConfigProvider.minimal();
        TopicsConfig topicConfig = TopicsConfig.of(cgg);

        List<TopicConfiguration> configurations = topicConfig.configurations();
        assertThat(configurations).hasSize(1);

        TopicConfiguration topicConfiguration = configurations.get(0);
        assertThat(topicConfiguration.itemTemplateKey()).isEqualTo("item-template.template1");
        assertThat(topicConfiguration.itemTemplateValue()).isEqualTo("item");
        assertThat(topicConfiguration.topic()).isEqualTo("topic1");
    }

    @Test
    void shouldConfigOneToMany() {
        Map<String, String> udpateConfig = new HashMap<>();
        udpateConfig.put("map.topic1.to", "item-template.template1,item-template.template2");
        udpateConfig.put("item-template.template2", "item2");
        ConnectorConfig cgg = ConnectorConfigProvider.minimalWith(udpateConfig);
        TopicsConfig topicConfig = TopicsConfig.of(cgg);

        List<TopicConfiguration> configurations = topicConfig.configurations();
        assertThat(configurations).hasSize(2);

        TopicConfiguration topicConfiguration1 = configurations.get(0);
        assertThat(topicConfiguration1.itemTemplateKey()).isEqualTo("item-template.template1");
        assertThat(topicConfiguration1.itemTemplateValue()).isEqualTo("item");
        assertThat(topicConfiguration1.topic()).isEqualTo("topic1");

        TopicConfiguration topicConfiguration2 = configurations.get(1);
        assertThat(topicConfiguration2.itemTemplateKey()).isEqualTo("item-template.template2");
        assertThat(topicConfiguration2.itemTemplateValue()).isEqualTo("item2");
        assertThat(topicConfiguration2.topic()).isEqualTo("topic1");
    }

    @Test
    void shouldConfigOneToManyIndentical() {
        Map<String, String> udpateConfig = new HashMap<>();
        udpateConfig.put("map.topic1.to", "item-template.template1,item-template.template1");
        ConnectorConfig cgg = ConnectorConfigProvider.minimalWith(udpateConfig);
        TopicsConfig topicConfig = TopicsConfig.of(cgg);

        List<TopicConfiguration> configurations = topicConfig.configurations();
        assertThat(configurations).hasSize(1);

        TopicConfiguration topicConfiguration1 = configurations.get(0);
        assertThat(topicConfiguration1.itemTemplateKey()).isEqualTo("item-template.template1");
        assertThat(topicConfiguration1.itemTemplateValue()).isEqualTo("item");
        assertThat(topicConfiguration1.topic()).isEqualTo("topic1");
    }

    @Test
    void shouldConfigManyToOne() {
        Map<String, String> udpateConfig = new HashMap<>();
        udpateConfig.put("map.topic2.to", "item-template.template1");
        ConnectorConfig cgg = ConnectorConfigProvider.minimalWith(udpateConfig);
        TopicsConfig topicConfig = TopicsConfig.of(cgg);

        List<TopicConfiguration> configurations = topicConfig.configurations();
        assertThat(configurations).hasSize(2);

        TopicConfiguration topicConfiguration1 = configurations.get(0);
        assertThat(topicConfiguration1.itemTemplateKey()).isEqualTo("item-template.template1");
        assertThat(topicConfiguration1.itemTemplateValue()).isEqualTo("item");
        assertThat(topicConfiguration1.topic()).isEqualTo("topic1");

        TopicConfiguration topicConfiguration2 = configurations.get(1);
        assertThat(topicConfiguration2.itemTemplateKey()).isEqualTo("item-template.template1");
        assertThat(topicConfiguration2.itemTemplateValue()).isEqualTo("item");
        assertThat(topicConfiguration2.topic()).isEqualTo("topic2");
    }
}
