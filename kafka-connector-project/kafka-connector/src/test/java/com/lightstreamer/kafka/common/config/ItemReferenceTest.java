
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

import static org.junit.Assert.assertThrows;

import com.lightstreamer.kafka.common.config.TopicConfigurations.ItemReference;
import com.lightstreamer.kafka.common.config.TopicConfigurations.ItemTemplateConfigs;
import com.lightstreamer.kafka.common.expressions.Expressions;
import com.lightstreamer.kafka.common.expressions.Expressions.TemplateExpression;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;

import java.util.Map;

public class ItemReferenceTest {

    @Test
    void shouldCreateSimpleItemNames() {
        ItemReference i1 = ItemReference.name("simple-item-name");
        assertThat(i1.isTemplate()).isFalse();
        assertThat(i1.itemName()).isEqualTo("simple-item-name");
        assertThat(i1.equals(i1)).isTrue();
        assertThrows(RuntimeException.class, () -> i1.template());

        ItemReference i2 = ItemReference.name("simple-item-name");
        assertThat(i1.equals(i2)).isTrue();
        assertThat(i1.hashCode() == i2.hashCode()).isTrue();
    }

    @ParameterizedTest
    @NullAndEmptySource
    void shouldNotCreateEmptySimpleItemName(String item) {
        ConfigException ce = assertThrows(ConfigException.class, () -> ItemReference.name(item));
        assertThat(ce.getMessage()).isEqualTo("Item name must be a non-empty string");
    }

    @Test
    void shouldCreateTemplates() {
        ItemReference i1 =
                ItemReference.template(
                        new TemplateExpression(
                                "template-prefix", Map.of("a", Expressions.Expression("VALUE"))));
        assertThat(i1.isTemplate()).isTrue();
        assertThat(i1.template().prefix()).isEqualTo("template-prefix");
        assertThat(i1.template().params()).containsExactly("a", Expressions.Expression("VALUE"));
        assertThrows(RuntimeException.class, () -> i1.itemName());

        ItemReference i2 =
                ItemReference.template(
                        new TemplateExpression(
                                "template-prefix", Map.of("a", Expressions.Expression("VALUE"))));
        assertThat(i1.equals(i2)).isTrue();
        assertThat(i1.hashCode() == i2.hashCode()).isTrue();
    }

    @Test
    void shouldCreateSimpleItemFromFactoryMethod() {
        ItemReference i1 = ItemReference.from("item", ItemTemplateConfigs.empty());
        assertThat(i1.isTemplate()).isFalse();
        assertThat(i1.itemName()).isEqualTo("item");
    }

    @Test
    void shouldCreateTemplateFromFactoryMethod() {
        ItemReference i1 =
                ItemReference.from(
                        "item-template.template",
                        ItemTemplateConfigs.from(Map.of("template", "template-#{a=VALUE}")));
        assertThat(i1.isTemplate()).isTrue();
        TemplateExpression te = i1.template();
        assertThat(te.prefix()).isEqualTo("template");
        assertThat(te.params()).containsAtLeast("a", Expressions.Expression("VALUE"));
    }

    @Test
    void shouldNotCreateTemplateFromFactoryMethodDueToMissingTemplate() {
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                ItemReference.from(
                                        "item-template.template", ItemTemplateConfigs.empty()));
        assertThat(ce.getMessage()).isEqualTo("No item template [template] found");
    }

    @Test
    void shouldNotCreateTemplateFromFactoryMethodDueToEmotyTemplateName() {
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ItemReference.from("item-template.", ItemTemplateConfigs.empty()));
        assertThat(ce.getMessage()).isEqualTo("Item template reference must be a non-empty string");
    }

    @Test
    void shouldNotInvokeFactoryMethodFromNullArg() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ItemReference.from(null, ItemTemplateConfigs.empty()));
    }
}
