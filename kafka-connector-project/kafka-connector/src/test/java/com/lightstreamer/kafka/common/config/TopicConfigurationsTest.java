
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

package com.lightstreamer.kafka.common.config;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.WrappedNoWildcardCheck;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.kafka.common.config.TopicConfigurations.ItemTemplateConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations.TopicConfiguration;
import com.lightstreamer.kafka.common.config.TopicConfigurations.TopicMappingConfig;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.TemplateExpression;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class TopicConfigurationsTest {

    @Test
    public void shouldConfigWithRegexDisabledByDefault() {
        TopicConfigurations topicConfig =
                TopicConfigurations.of(ItemTemplateConfigs.empty(), Collections.emptyList());
        assertThat(topicConfig.isRegexEnabled()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldConfigRegexEnablement(boolean regex) {
        TopicConfigurations topicConfig =
                TopicConfigurations.of(ItemTemplateConfigs.empty(), Collections.emptyList(), regex);
        assertThat(topicConfig.isRegexEnabled()).isEqualTo(regex);
    }

    static Stream<Arguments> partitions() {
        return Stream.of(
                Arguments.of("", Set.of()),
                Arguments.of(null, Set.of()),
                Arguments.of("0-4,6-8", Set.of(0, 1, 2, 3, 4, 6, 7, 8)));
    }

    @ParameterizedTest
    @MethodSource("partitions")
    public void shouldConfigOneToOneTemplate(String partitions, Set<Integer> expectedPartitions) {
        var templateConfigs =
                ItemTemplateConfigs.from(Map.of("template1", "template1-#{a=PARTITION}"));
        var topicMappingConfigs =
                List.of(
                        TopicMappingConfig.fromDelimitedMappings(
                                "topic", "item-template.template1", partitions));
        TopicConfigurations topicConfig =
                TopicConfigurations.of(templateConfigs, topicMappingConfigs);

        Set<TopicConfiguration> configurations = topicConfig.configurations();
        assertThat(configurations).hasSize(1);

        TopicConfiguration topicConfiguration = configurations.iterator().next();
        assertThat(topicConfiguration.topic()).isEqualTo("topic");
        assertThat(topicConfiguration.partitions()).isEqualTo(expectedPartitions);

        List<TemplateExpression> itemReferences = topicConfiguration.itemReferences();
        assertThat(itemReferences).hasSize(1);

        TemplateExpression itemReference = itemReferences.get(0);
        assertThat(itemReference.prefix()).isEqualTo("template1");
        assertThat(itemReference.params())
                .containsExactly("a", WrappedNoWildcardCheck("#{PARTITION}"));
    }

    @Test
    public void shouldConfigOneToOneItem() {
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(
                        ItemTemplateConfigs.empty(),
                        List.of(
                                TopicMappingConfig.fromDelimitedMappings(
                                        "topic", "simple-item", "")));

        Set<TopicConfiguration> configurations = topicsConfig.configurations();
        assertThat(configurations).hasSize(1);

        TopicConfiguration topicConfiguration = configurations.iterator().next();
        assertThat(topicConfiguration.topic()).isEqualTo("topic");
        assertThat(topicConfiguration.partitions()).isEmpty();

        List<TemplateExpression> itemReferences = topicConfiguration.itemReferences();
        assertThat(itemReferences).hasSize(1);

        TemplateExpression itemReference = itemReferences.get(0);
        assertThat(itemReference.prefix()).isEqualTo("simple-item");
        assertThat(itemReference.params()).isEmpty();
    }

    @Test
    public void shouldConfigOneToManyTemplates() {
        var templateConfigs =
                ItemTemplateConfigs.from(
                        Map.of(
                                "template1",
                                "template1-#{a=VALUE}",
                                "template2",
                                "template2-#{c=OFFSET}"));
        List<TopicMappingConfig> topicMappingConfigs =
                List.of(
                        TopicMappingConfig.fromDelimitedMappings(
                                "topic", "item-template.template1,item-template.template2", ""));
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(templateConfigs, topicMappingConfigs);

        Set<TopicConfiguration> configurations = topicsConfig.configurations();
        assertThat(configurations).hasSize(1);

        Iterator<TopicConfiguration> iterator = configurations.iterator();

        TopicConfiguration topicConfiguration1 = iterator.next();
        assertThat(topicConfiguration1.topic()).isEqualTo("topic");
        assertThat(topicConfiguration1.partitions()).isEmpty();

        List<TemplateExpression> itemReferences = topicConfiguration1.itemReferences();
        assertThat(itemReferences).hasSize(2);

        TemplateExpression itemReference1 = itemReferences.get(0);
        assertThat(itemReference1.prefix()).isEqualTo("template1");
        assertThat(itemReference1.params())
                .containsExactly("a", WrappedNoWildcardCheck("#{VALUE}"));

        TemplateExpression itemReference2 = itemReferences.get(1);
        assertThat(itemReference2.prefix()).isEqualTo("template2");
        assertThat(itemReference2.params())
                .containsExactly("c", WrappedNoWildcardCheck("#{OFFSET}"));
    }

    @Test
    public void shouldConfigOneToManyItems() {
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(
                        ItemTemplateConfigs.empty(),
                        List.of(
                                TopicMappingConfig.fromDelimitedMappings(
                                        "topic", "item1,item2", "")));

        Set<TopicConfiguration> configurations = topicsConfig.configurations();
        assertThat(configurations).hasSize(1);

        Iterator<TopicConfiguration> iterator = configurations.iterator();

        TopicConfiguration topicConfiguration1 = iterator.next();
        assertThat(topicConfiguration1.topic()).isEqualTo("topic");
        assertThat(topicConfiguration1.partitions()).isEmpty();

        List<TemplateExpression> itemReferences = topicConfiguration1.itemReferences();
        assertThat(itemReferences).hasSize(2);

        TemplateExpression itemReference1 = itemReferences.get(0);
        assertThat(itemReference1.prefix()).isEqualTo("item1");
        assertThat(itemReference1.params()).isEmpty();

        TemplateExpression itemReference2 = itemReferences.get(1);
        assertThat(itemReference2.prefix()).isEqualTo("item2");
        assertThat(itemReference2.params()).isEmpty();
    }

    @Test
    public void shouldConfigOneToManyIdenticalTemplates() {
        var templateConfigs = ItemTemplateConfigs.from(Map.of("template1", "template1-#{a=KEY}"));
        var topicMappingConfigs =
                List.of(
                        TopicMappingConfig.fromDelimitedMappings(
                                "topic", "item-template.template1,item-template.template1", ""));
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(templateConfigs, topicMappingConfigs);

        Set<TopicConfiguration> configurations = topicsConfig.configurations();
        assertThat(configurations).hasSize(1);

        TopicConfiguration topicConfiguration1 = configurations.iterator().next();
        assertThat(topicConfiguration1.topic()).isEqualTo("topic");
        assertThat(topicConfiguration1.partitions()).isEmpty();

        List<TemplateExpression> itemReferences = topicConfiguration1.itemReferences();
        assertThat(itemReferences).hasSize(1);

        TemplateExpression itemReference = itemReferences.get(0);
        assertThat(itemReference.prefix()).isEqualTo("template1");
        assertThat(itemReference.params()).containsExactly("a", WrappedNoWildcardCheck("#{KEY}"));
    }

    @Test
    public void shouldConfigOneToManyIdenticalItems() {
        var topicMappingConfigs =
                List.of(TopicMappingConfig.fromDelimitedMappings("topic", "item1,item1,item2", ""));
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(ItemTemplateConfigs.empty(), topicMappingConfigs);

        Set<TopicConfiguration> configurations = topicsConfig.configurations();
        assertThat(configurations).hasSize(1);

        TopicConfiguration topicConfiguration1 = configurations.iterator().next();
        assertThat(topicConfiguration1.topic()).isEqualTo("topic");
        assertThat(topicConfiguration1.partitions()).isEmpty();

        List<TemplateExpression> itemReferences = topicConfiguration1.itemReferences();
        assertThat(itemReferences).hasSize(2);

        TemplateExpression itemReference1 = itemReferences.get(0);
        assertThat(itemReference1.prefix()).isEqualTo("item1");
        assertThat(itemReference1.params()).isEmpty();

        TemplateExpression itemReference2 = itemReferences.get(1);
        assertThat(itemReference2.prefix()).isEqualTo("item2");
        assertThat(itemReference2.params()).isEmpty();
    }

    @Test
    public void shouldConfigManyToOneTemplate() {
        var templateConfigs =
                ItemTemplateConfigs.from(Map.of("template1", "template-#{name=VALUE}"));

        Map<String, String> mappings = new LinkedHashMap<>(); // Ensures order for later lookup
        mappings.put("topic", "item-template.template1");
        mappings.put("topic2", "item-template.template1");

        var topicMappingConfigs = TopicMappingConfig.from(mappings);

        TopicConfigurations topicsConfig =
                TopicConfigurations.of(templateConfigs, topicMappingConfigs);

        Set<TopicConfiguration> configurations = topicsConfig.configurations();
        assertThat(configurations).hasSize(2);

        Iterator<TopicConfiguration> iterator = configurations.iterator();

        TopicConfiguration topicConfiguration1 = iterator.next();
        assertThat(topicConfiguration1.topic()).isEqualTo("topic");
        assertThat(topicConfiguration1.partitions()).isEmpty();

        TopicConfiguration topicConfiguration2 = iterator.next();
        assertThat(topicConfiguration2.topic()).isEqualTo("topic2");
        assertThat(topicConfiguration2.partitions()).isEmpty();

        assertThat(topicConfiguration1.itemReferences())
                .isEqualTo(topicConfiguration2.itemReferences());
    }

    @Test
    public void shouldConfigManyToOneItem() {
        Map<String, String> mappings = new LinkedHashMap<>(); // Ensures order for later lookup
        mappings.put("topic", "item");
        mappings.put("topic2", "item");

        TopicConfigurations topicsConfig =
                TopicConfigurations.of(
                        ItemTemplateConfigs.empty(), TopicMappingConfig.from(mappings));

        Set<TopicConfiguration> configurations = topicsConfig.configurations();
        assertThat(configurations).hasSize(2);

        Iterator<TopicConfiguration> iterator = configurations.iterator();

        TopicConfiguration topicConfiguration1 = iterator.next();
        assertThat(topicConfiguration1.topic()).isEqualTo("topic");
        assertThat(topicConfiguration1.partitions()).isEmpty();

        TopicConfiguration topicConfiguration2 = iterator.next();
        assertThat(topicConfiguration2.topic()).isEqualTo("topic2");
        assertThat(topicConfiguration2.partitions()).isEmpty();

        List<TemplateExpression> itemReference = topicConfiguration1.itemReferences();
        assertThat(itemReference).isEqualTo(topicConfiguration2.itemReferences());
    }

    @Test
    public void shouldNotConfigDueToMissingTemplate() {
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                TopicConfigurations.of(
                                        ItemTemplateConfigs.empty(),
                                        TopicMappingConfig.from(
                                                Map.of(
                                                        "topic",
                                                        "item-template.missing-template"))));
        assertThat(ce).hasMessageThat().isEqualTo("No item template [missing-template] found");
    }

    @Test
    public void shouldNotConfigDueToInvalidTemplate() {
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                TopicConfigurations.of(
                                        ItemTemplateConfigs.empty(),
                                        TopicMappingConfig.from(
                                                Map.of("topic", "item-template."))));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Item template reference must be a non-empty string");
    }
}
