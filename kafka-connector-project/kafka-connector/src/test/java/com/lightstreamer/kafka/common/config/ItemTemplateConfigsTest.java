
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
import com.lightstreamer.kafka.common.mapping.selectors.Expressions;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.TemplateExpression;

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
        assertThat(templateConfigs.templates()).isEmpty();
    }

    @Test
    void shouldReturnEmptyConfigs() {
        ItemTemplateConfigs templateConfigs = ItemTemplateConfigs.empty();
        assertThat(templateConfigs.templates()).isEmpty();
    }

    @Test
    void shouldCreateItemTemplateConfigFromMapWithOneEntryOneParam() {
        ItemTemplateConfigs it =
                ItemTemplateConfigs.from(
                        Map.of("template-name", "template-prefix-#{param=OFFSET}"));
        assertThat(it.templates()).hasSize((1));
        assertThat(it.contains("template-name")).isTrue();
        TemplateExpression expression = it.getTemplateExpression("template-name");
        assertThat(expression.prefix()).isEqualTo("template-prefix");
        assertThat(expression.params()).containsExactly("param", Expressions.Expression("OFFSET"));
    }

    @Test
    void shouldCreateItemTemplateConfigFromMapWithOneEntryMultipleParams() {
        ItemTemplateConfigs it =
                ItemTemplateConfigs.from(
                        Map.of(
                                "template-name",
                                "template-prefix-#{param1=OFFSET,param2=PARTITION,param3=TIMESTAMP}"));
        assertThat(it.templates()).hasSize((1));
        assertThat(it.contains("template-name")).isTrue();
        TemplateExpression expression = it.getTemplateExpression("template-name");
        assertThat(expression.prefix()).isEqualTo("template-prefix");
        assertThat(expression.params())
                .containsExactly(
                        "param1",
                        Expressions.Expression("OFFSET"),
                        "param2",
                        Expressions.Expression("PARTITION"),
                        "param3",
                        Expressions.Expression("TIMESTAMP"));
    }

    @Test
    void shouldCreateItemTemplateConfigFromMapWithMultipleEntriesMultipleParams() {
        ItemTemplateConfigs it =
                ItemTemplateConfigs.from(
                        Map.of(
                                "template-name-a",
                                "template-prefix-a-#{param1a=VALUE,param2a=KEY,param3a=PARTITION}",
                                "template-name-b",
                                "template-prefix-b-#{param1b=VALUE.b,param2b=KEY.b,param3b=KEY.c}"));
        assertThat(it.templates()).hasSize((2));
        assertThat(it.contains("template-name-a")).isTrue();

        TemplateExpression expression_a = it.getTemplateExpression("template-name-a");
        assertThat(expression_a.prefix()).isEqualTo("template-prefix-a");
        assertThat(expression_a.params())
                .containsExactly(
                        "param1a",
                        Expressions.Expression("VALUE"),
                        "param2a",
                        Expressions.Expression("KEY"),
                        "param3a",
                        Expressions.Expression("PARTITION"));

        TemplateExpression expression_b = it.getTemplateExpression("template-name-b");
        assertThat(expression_b.prefix()).isEqualTo("template-prefix-b");
        assertThat(expression_b.params())
                .containsExactly(
                        "param1b",
                        Expressions.Expression("VALUE.b"),
                        "param2b",
                        Expressions.Expression("KEY.b"),
                        "param3b",
                        Expressions.Expression("KEY.c"));
    }

    @ParameterizedTest
    @EmptySource
    @ValueSource(
            strings = {
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
                "prefix-#{VALUE}",
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
                        "Got the following error while evaluating the template [template-name] containing the expression ["
                                + templateExpression
                                + "]: <Invalid template expression>");
    }

    @Test
    public void shouldNotAllowDuplicatedKeysInTheSameTemplate() {
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
                        "Got the following error while evaluating the template [template-name] containing the expression [item-#{name=VALUE,name=PARTITION}]: <No duplicated keys are allowed>");
    }
}
