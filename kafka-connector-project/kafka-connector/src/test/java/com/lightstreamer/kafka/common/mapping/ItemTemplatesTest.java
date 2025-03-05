
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

package com.lightstreamer.kafka.common.mapping;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static com.lightstreamer.kafka.common.expressions.Expressions.Expression;
import static com.lightstreamer.kafka.common.mapping.selectors.DataExtractor.extractor;
import static com.lightstreamer.kafka.test_utils.TestSelectorSuppliers.JsonValue;
import static com.lightstreamer.kafka.test_utils.TestSelectorSuppliers.Object;

import static java.util.Collections.emptySet;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka.common.config.TopicConfigurations;
import com.lightstreamer.kafka.common.config.TopicConfigurations.ItemTemplateConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations.TopicMappingConfig;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.selectors.DataExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KeyValueSelectorSuppliers;
import com.lightstreamer.kafka.common.mapping.selectors.Schema;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ItemTemplatesTest {

    private static final String TEST_TOPIC_1 = "topic";

    private static final String TEST_TOPIC_2 = "anotherTopic";

    /*
     * Prefix each input element with "item-template." and concatenate all of them, separated by a comma character
     */
    static String getFullTemplateNames(String... template) {
        return Stream.of(template).map(t -> "item-template." + t).collect(Collectors.joining(","));
    }

    @Test
    public void shouldCreateWihtCommonTemplateDefinition() throws ExtractionException {
        // Define three template names
        String template1 = "template1";
        String template2 = "template2";
        String template3 = "template3";

        // TEST_TOPIC_1 mapped to template "template1" and "template2"
        TopicMappingConfig testTopic1Mapping =
                TopicMappingConfig.fromDelimitedMappings(
                        TEST_TOPIC_1, getFullTemplateNames(template1, template2));

        // TEST_TOPIC_2 mapped to template "template1","template2", and "template3"
        TopicMappingConfig testTopic2Mapping =
                TopicMappingConfig.fromDelimitedMappings(
                        TEST_TOPIC_2, getFullTemplateNames(template1, template2, template3));

        // Create the three templates, sharing the same template definition
        String templateDefinition = "stock-#{index=KEY.attrib}";
        ItemTemplateConfigs templateConfigs =
                ItemTemplateConfigs.from(
                        Map.of(
                                template1,
                                templateDefinition,
                                template2,
                                templateDefinition,
                                template3,
                                templateDefinition));
        ItemTemplates<Object, Object> templates =
                Items.templatesFrom(
                        TopicConfigurations.of(
                                templateConfigs, List.of(testTopic1Mapping, testTopic2Mapping)),
                        Object());
        assertWithMessage("Templates object contains the expected topics")
                .that(templates.topics())
                .containsExactly(TEST_TOPIC_1, TEST_TOPIC_2);

        assertWithMessage("Templates object has not regex enabled by default")
                .that(templates.isRegexEnabled())
                .isFalse();

        Map<String, Set<DataExtractor<Object, Object>>> extractors = templates.groupExtractors();
        assertThat(extractors).hasSize(2);
        assertWithMessage("Only one extractor associated with TEST_TOPIC_1")
                .that(extractors.get(TEST_TOPIC_1))
                .hasSize(1);
        assertWithMessage("The extractor associated with TEST_TOPIC_1 is as expected")
                .that(extractors.get(TEST_TOPIC_1))
                .containsExactly(
                        extractor(
                                Object(),
                                "stock",
                                Map.of("index", Expression("KEY.attrib")),
                                false,
                                false));

        assertWithMessage("Only one extractor associated with TEST_TOPIC_2")
                .that(extractors.get(TEST_TOPIC_2))
                .hasSize(1);
        assertWithMessage("The extractor associated with TEST_TOPIC_2 is as expected")
                .that(extractors.get(TEST_TOPIC_2))
                .containsExactly(
                        extractor(
                                Object(),
                                "stock",
                                Map.of("index", Expression("KEY.attrib")),
                                false,
                                false));
        assertThat(templates.getExtractorSchemasByTopicName(TEST_TOPIC_2))
                .containsExactly(Schema.from("stock", Set.of("index")));

        assertWithMessage("The item matches at least a template")
                .that(templates.matches(Items.subscribedFrom("stock-[index=1]")))
                .isTrue();
        assertWithMessage("The item matches at least a template")
                .that(templates.matches(Items.subscribedFrom("stock-[index=2]")))
                .isTrue();
        assertWithMessage("The item does not match any defined template")
                .that(templates.matches(Items.subscribedFrom("anItem")))
                .isFalse();
    }

    @Test
    public void shouldCreateFromMixedTemplatesAndSimpleItems() throws ExtractionException {
        TopicMappingConfig tm =
                TopicMappingConfig.fromDelimitedMappings(
                        TEST_TOPIC_1, "item-template.template1,simple-item-1");

        ItemTemplateConfigs templateConfigs =
                ItemTemplateConfigs.from(Map.of("template1", "stock-#{index=KEY.attrib}"));
        TopicConfigurations topicsConfig = TopicConfigurations.of(templateConfigs, List.of(tm));

        ItemTemplates<Object, Object> templates = Items.templatesFrom(topicsConfig, Object());
        assertThat(templates.topics()).containsExactly(TEST_TOPIC_1);
        assertThat(templates.groupExtractors()).hasSize(1);
        assertThat(templates.groupExtractors().get(TEST_TOPIC_1)).hasSize(2);
        assertThat(templates.getExtractorSchemasByTopicName(TEST_TOPIC_1))
                .containsExactly(
                        Schema.from("stock", Set.of("index")),
                        Schema.from("simple-item-1", emptySet()));

        assertThat(templates.matches(Items.subscribedFrom("simple-item-1"))).isTrue();
        assertThat(templates.matches(Items.subscribedFrom("stock-[index=1]"))).isTrue();
        assertThat(templates.matches(Items.subscribedFrom("stock-[index=2]"))).isTrue();
        assertThat(templates.matches(Items.subscribedFrom("simple-item-2"))).isFalse();
        assertThat(templates.matches(Items.subscribedFrom("stock-[key=1]"))).isFalse();
    }

    @Test
    public void shouldCreateOneToOneItemTemplateFromSimpleItem() throws ExtractionException {
        // One topic mapping one item
        TopicMappingConfig tm =
                TopicMappingConfig.fromDelimitedMappings(TEST_TOPIC_1, "simple-item-1");
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(ItemTemplateConfigs.empty(), List.of(tm));

        ItemTemplates<Object, Object> templates = Items.templatesFrom(topicsConfig, Object());
        assertThat(templates.topics()).containsExactly(TEST_TOPIC_1);
        assertThat(templates.groupExtractors()).hasSize(1);
        assertThat(templates.groupExtractors().get(TEST_TOPIC_1)).hasSize(1);
        assertThat(templates.groupExtractors().get(TEST_TOPIC_1))
                .containsExactly(extractor(Object(), "simple-item-1"));
        assertThat(templates.getExtractorSchemasByTopicName(TEST_TOPIC_1))
                .containsExactly(Schema.from("simple-item-1", emptySet()));
    }

    @Test
    public void shouldCreateOneToOneItemTemplate() throws ExtractionException {
        // One topic mapping one item template
        TopicMappingConfig topicMapping =
                TopicMappingConfig.fromDelimitedMappings("stocks", "item-template.template");
        ItemTemplateConfigs templateConfigs =
                ItemTemplateConfigs.from(Map.of("template", "stock-#{index=KEY.attrib}"));
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(templateConfigs, List.of(topicMapping));

        ItemTemplates<Object, Object> templates = Items.templatesFrom(topicsConfig, Object());
        assertThat(templates.topics()).containsExactly("stocks");
        assertThat(templates.groupExtractors()).hasSize(1);
        assertThat(templates.groupExtractors().get("stocks")).hasSize(1);
        assertThat(templates.getExtractorSchemasByTopicName("stocks"))
                .containsExactly(Schema.from("stock", Set.of("index")));

        assertThat(templates.matches(Items.subscribedFrom("stock-[index=1]"))).isTrue();
        assertThat(templates.matches(Items.subscribedFrom("stock-[key=1]"))).isFalse();
    }

    @Test
    public void shouldCreateOneToManyItemTemplateFromSimpleItem() throws ExtractionException {
        // One topic mapping two items.
        TopicMappingConfig tm =
                TopicMappingConfig.fromDelimitedMappings(
                        TEST_TOPIC_1, "simple-item-1,simple-item-2");
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(ItemTemplateConfigs.empty(), List.of(tm));

        ItemTemplates<String, JsonNode> templates = Items.templatesFrom(topicsConfig, JsonValue());
        assertThat(templates.topics()).containsExactly(TEST_TOPIC_1);
        assertThat(templates.groupExtractors()).hasSize(1);
        assertThat(templates.groupExtractors().get(TEST_TOPIC_1)).hasSize(2);
        assertThat(templates.getExtractorSchemasByTopicName(TEST_TOPIC_1))
                .containsExactly(
                        Schema.from("simple-item-1", emptySet()),
                        Schema.from("simple-item-2", emptySet()));

        SubscribedItem item1 = Items.subscribedFrom("simple-item-1", "itemHandle1");
        assertThat(templates.matches(item1)).isTrue();

        SubscribedItem item2 = Items.subscribedFrom("simple-item-2", "itemHandle2");
        assertThat(templates.matches(item2)).isTrue();

        SubscribedItem item3 = Items.subscribedFrom("simple-item-3", "itemHandle2");
        assertThat(templates.matches(item3)).isFalse();
    }

    @Test
    public void shouldCreateOneToManyItemTemplate() throws ExtractionException {
        // One topic mapping two item templates.
        TopicMappingConfig tm =
                TopicMappingConfig.fromDelimitedMappings(
                        TEST_TOPIC_1, "item-template.family,item-template.relatives");
        ItemTemplateConfigs templateConfigs =
                ItemTemplateConfigs.from(
                        Map.of(
                                "family",
                                "template-family-#{topic=TOPIC,info=PARTITION}",
                                "relatives",
                                "template-relatives-#{topic=TOPIC,info1=TIMESTAMP}"));

        TopicConfigurations topicsConfig = TopicConfigurations.of(templateConfigs, List.of(tm));

        ItemTemplates<String, JsonNode> templates = Items.templatesFrom(topicsConfig, JsonValue());
        assertThat(templates.topics()).containsExactly(TEST_TOPIC_1);
        assertThat(templates.groupExtractors()).hasSize(1);
        assertThat(templates.groupExtractors().get(TEST_TOPIC_1)).hasSize(2);
        assertThat(templates.getExtractorSchemasByTopicName(TEST_TOPIC_1))
                .containsExactly(
                        Schema.from("template-family", Set.of("topic", "info")),
                        Schema.from("template-relatives", Set.of("topic", "info1")));

        SubscribedItem item1 =
                Items.subscribedFrom(
                        "template-family-[topic=" + TEST_TOPIC_1 + ",info=150]", "itemHandle1");
        assertThat(templates.matches(item1)).isTrue();

        SubscribedItem item2 =
                Items.subscribedFrom(
                        "template-relatives-[topic=" + TEST_TOPIC_1 + ",info1=-1]", "itemHandle2");
        assertThat(templates.matches(item2)).isTrue();
    }

    @Test
    public void shouldCreateManyToOneItemTemplateFromSimpleItem() throws ExtractionException {
        KeyValueSelectorSuppliers<String, JsonNode> sSuppliers = JsonValue();

        // One item.
        String item = "orders";

        // Two topics mapping the item.
        String newOrdersTopic = "new_orders";
        String pastOrderTopic = "past_orders";
        TopicMappingConfig orderMapping =
                TopicMappingConfig.fromDelimitedMappings(newOrdersTopic, item);
        TopicMappingConfig pastOrderMapping =
                TopicMappingConfig.fromDelimitedMappings(pastOrderTopic, item);
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(
                        ItemTemplateConfigs.empty(), List.of(orderMapping, pastOrderMapping));

        ItemTemplates<String, JsonNode> templates = Items.templatesFrom(topicsConfig, sSuppliers);
        assertThat(templates.topics()).containsExactly(newOrdersTopic, pastOrderTopic);
        assertThat(templates.groupExtractors()).hasSize(2);
        assertThat(templates.groupExtractors().get(newOrdersTopic)).hasSize(1);
        assertThat(templates.groupExtractors().get(pastOrderTopic)).hasSize(1);

        SubscribedItem subcribingItem = Items.subscribedFrom("orders", "");
        assertThat(templates.matches(subcribingItem)).isTrue();
    }

    @Test
    public void shouldManyToOne() throws ExtractionException {
        KeyValueSelectorSuppliers<String, JsonNode> sSuppliers = JsonValue();

        // One template.
        ItemTemplateConfigs templateConfigs =
                ItemTemplateConfigs.from(
                        Map.of("template-order", "template-orders-#{topic=TOPIC}"));

        // Two topics mapping the template.
        String newOrdersTopic = "new_orders";
        String pastOrderTopic = "past_orders";
        TopicMappingConfig orderMapping =
                TopicMappingConfig.fromDelimitedMappings(
                        newOrdersTopic, "item-template.template-order");
        TopicMappingConfig pastOrderMapping =
                TopicMappingConfig.fromDelimitedMappings(
                        pastOrderTopic, "item-template.template-order");
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(templateConfigs, List.of(orderMapping, pastOrderMapping));

        ItemTemplates<String, JsonNode> templates = Items.templatesFrom(topicsConfig, sSuppliers);
        assertThat(templates.topics()).containsExactly(newOrdersTopic, pastOrderTopic);
        assertThat(templates.groupExtractors()).hasSize(2);
        assertThat(templates.groupExtractors().get(newOrdersTopic)).hasSize(1);
        assertThat(templates.groupExtractors().get(pastOrderTopic)).hasSize(1);

        SubscribedItem itemFilteringTopic1 =
                Items.subscribedFrom("template-orders-[topic=new_orders]", "");
        assertThat(templates.matches(itemFilteringTopic1)).isTrue();

        SubscribedItem itemFilteringTopic2 =
                Items.subscribedFrom("template-orders-[topic=past_orders]", "");
        assertThat(templates.matches(itemFilteringTopic2)).isTrue();
    }

    @Test
    public void shouldCreateWithRegexDisabledByDefault() throws ExtractionException {
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(ItemTemplateConfigs.empty(), Collections.emptyList());
        ItemTemplates<Object, Object> templates = Items.templatesFrom(topicsConfig, Object());
        assertThat(templates.isRegexEnabled()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldCreateWithRegexEnablement(boolean regex) throws ExtractionException {
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(ItemTemplateConfigs.empty(), Collections.emptyList(), regex);
        ItemTemplates<Object, Object> templates = Items.templatesFrom(topicsConfig, Object());
        assertThat(templates.isRegexEnabled()).isEqualTo(regex);
        if (regex) {
            assertThat(templates.subscriptionPattern()).isPresent();
        } else {
            assertThat(templates.subscriptionPattern()).isEmpty();
        }
    }

    @Test
    public void shouldReturnSubscriptionPatternFromSingleRegex() throws ExtractionException {
        TopicMappingConfig topicMapping =
                TopicMappingConfig.fromDelimitedMappings("topic_\\d+", "item1");
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(ItemTemplateConfigs.empty(), List.of(topicMapping), true);
        ItemTemplates<Object, Object> templates = Items.templatesFrom(topicsConfig, Object());
        Optional<Pattern> subscriptionPattern = templates.subscriptionPattern();
        assertThat(subscriptionPattern.get().pattern()).isEqualTo("(?:topic_\\d+)");
    }

    @Test
    public void shouldReturnSubscriptionPatternFromMultipleRegex() throws ExtractionException {
        TopicMappingConfig topicMapping1 =
                TopicMappingConfig.fromDelimitedMappings("topicA_\\d+", "item1,item2");
        TopicMappingConfig topicMapping2 =
                TopicMappingConfig.fromDelimitedMappings("topicB_\\d+", "item1,item2");
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(
                        ItemTemplateConfigs.empty(), List.of(topicMapping1, topicMapping2), true);
        ItemTemplates<Object, Object> templates = Items.templatesFrom(topicsConfig, Object());
        Optional<Pattern> subscriptionPattern = templates.subscriptionPattern();
        Pattern pattern = subscriptionPattern.get();
        assertThat(pattern.pattern()).isEqualTo("(?:topicA_\\d+)|(?:topicB_\\d+)");

        Matcher matcher = pattern.matcher("topicA_123");
        assertThat(matcher.matches());
        assertThat(matcher.groupCount()).isEqualTo(0);
    }
}
