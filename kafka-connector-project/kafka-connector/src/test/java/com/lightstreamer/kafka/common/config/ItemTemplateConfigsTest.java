
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

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.kafka.common.config.TopicConfigurations.ItemTemplateConfigs;
import com.lightstreamer.kafka.common.expressions.ExpressionEvaluators.ExtractionExpression;
import com.lightstreamer.kafka.common.expressions.ExpressionEvaluators.TemplateExpression;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.Map;

public class ItemTemplateConfigsTest {

    @Test
    void shouldCreateFromEmptyMap() {
        ItemTemplateConfigs templateConfigs = ItemTemplateConfigs.from(Collections.emptyMap());
        assertThat(templateConfigs.expressions()).isEmpty();
    }

    @Test
    void shouldReturnEmptyConfigs() {
        ItemTemplateConfigs templateConfigs = ItemTemplateConfigs.empty();
        assertThat(templateConfigs.expressions()).isEmpty();
    }

    @Test
    void shouldCreateItemTemplateConfigFromMapWithOneEntryOneParam() {
        ItemTemplateConfigs it =
                ItemTemplateConfigs.from(Map.of("template-name", "template-prefix-#{param=v}"));
        assertThat(it.expressions()).hasSize((1));
        assertThat(it.contains("template-name")).isTrue();
        TemplateExpression expression = it.getExpression("template-name");
        assertThat(expression.prefix()).isEqualTo("template-prefix");
        assertThat(expression.params()).containsExactly("param", ExtractionExpression.of("v"));
    }

    @Test
    void shouldCreateItemTemplateConfigFromMapWithOneEntryMultipleParams() {
        ItemTemplateConfigs it =
                ItemTemplateConfigs.from(
                        Map.of(
                                "template-name",
                                "template-prefix-#{param1=v1,param2=v2,param3=v3}"));
        assertThat(it.expressions()).hasSize((1));
        assertThat(it.contains("template-name")).isTrue();
        TemplateExpression expression = it.getExpression("template-name");
        assertThat(expression.prefix()).isEqualTo("template-prefix");
        assertThat(expression.params())
                .containsExactly(
                        "param1",
                        ExtractionExpression.of("v1"),
                        "param2",
                        ExtractionExpression.of("v2"),
                        "param3",
                        ExtractionExpression.of("v3"));
    }

    @Test
    void shouldCreateItemTemplateConfigFromMapWithMultipleEntriesMultipleParams() {
        ItemTemplateConfigs it =
                ItemTemplateConfigs.from(
                        Map.of(
                                "template-name-a",
                                "template-prefix-a-#{param1a=v1a,param2a=v2a,param3a=v3a}",
                                "template-name-b",
                                "template-prefix-b-#{param1b=v1b,param2b=v2b,param3b=v3b}"));
        assertThat(it.expressions()).hasSize((2));
        assertThat(it.contains("template-name-a")).isTrue();

        TemplateExpression expression_a = it.getExpression("template-name-a");
        assertThat(expression_a.prefix()).isEqualTo("template-prefix-a");
        assertThat(expression_a.params())
                .containsExactly(
                        "param1a",
                        ExtractionExpression.of("v1a"),
                        "param2a",
                        ExtractionExpression.of("v2a"),
                        "param3a",
                        ExtractionExpression.of("v3a"));

        TemplateExpression expression_b = it.getExpression("template-name-b");
        assertThat(expression_b.prefix()).isEqualTo("template-prefix-b");
        assertThat(expression_b.params())
                .containsExactly(
                        "param1b",
                        ExtractionExpression.of("v1b"),
                        "param2b",
                        ExtractionExpression.of("v2b"),
                        "param3b",
                        ExtractionExpression.of("v3b"));
    }

    @ParameterizedTest
    @EmptySource
    @ValueSource(
            strings = {
                "-",
                "-",
                "\\",
                "@",
                "|",
                "!",
                "item-first",
                "item_123_",
                "item!",
                "item@",
                "item\\",
                "item-",
                "prefix-#{}",
                "prefix-#{VALUE}"
            })
    public void shouldNotAllowInvalidTemplateExpression(String templateExpression) {
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                ItemTemplateConfigs.from(
                                        Map.of("template-name", templateExpression)));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Found the invalid expression ["
                                + templateExpression
                                + "] while"
                                + " evaluating [template-name]: <Invalid template expression>");
    }

    @Test
    public void shouldNotAllowDuplicatedKeysOnTheSameTemplate() {
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                ItemTemplateConfigs.from(
                                        Map.of(
                                                "template-name",
                                                "item-#{name=VALUE,name=PARTITION}")));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Found the invalid expression [item-#{name=VALUE,name=PARTITION}] while"
                                + " evaluating [template-name]: <No duplicated keys are allowed>");
    }
}
