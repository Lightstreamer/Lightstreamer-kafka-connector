
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

package com.lightstreamer.kafka.adapters.config.specs;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.TEXT;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.DefaultHolder.defaultNull;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.DefaultHolder.defaultValue;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandModeStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfParameter;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.Options;
import com.lightstreamer.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ConfigSpecTest {

    @Test
    void shouldClone() {
        ConfigsSpec parent = new ConfigsSpec("parent");
        parent.add("prop1", ConfType.TEXT);
        parent.add("prop2", Options.evaluatorTypes());

        ConfigsSpec child = new ConfigsSpec("child");
        child.add("prop3", ConfType.TEXT);

        parent.withChildConfigs(child);

        ConfigsSpec cloned = new ConfigsSpec(parent);
        assertThat(parent).isEqualTo(cloned);
    }

    @Test
    void shouldReturnSimpleNameSpacedConfigSpec() {
        ConfigsSpec source = new ConfigsSpec("source");
        source.add("prop1", ConfType.TEXT);

        ConfigsSpec sub = source.newSpecWithNameSpace("sub");

        ConfParameter origin = source.getParameter("prop1");
        ConfParameter nameSpaced = sub.getParameter("sub.prop1");

        assertThat(nameSpaced).isNotNull();
        assertParams("sub", nameSpaced, origin);
    }

    @Test
    void shouldReturnNestedNameSpacedConfigSpec() {
        ConfigsSpec source = new ConfigsSpec("root");
        source.add("prop1", ConfType.TEXT);
        source.add("enabled.nested", true, false, ConfType.BOOL, defaultValue("true"));

        ConfigsSpec nested = new ConfigsSpec("nested");
        nested.add("nested.prop1", ConfType.TEXT);
        source.withEnabledChildConfigs(nested, "enabled.nested");

        ConfigsSpec sub = source.newSpecWithNameSpace("sub");

        ConfParameter origin = source.getParameter("nested.prop1");
        ConfParameter nameSpaced = sub.getParameter("sub.nested.prop1");

        assertThat(nameSpaced).isNotNull();
        assertParams("sub", nameSpaced, origin);
    }

    void assertParams(String nameSpace, ConfParameter nameSpaced, ConfParameter origin) {
        assertThat(nameSpaced.name()).isEqualTo(nameSpace + "." + origin.name());
        assertThat(nameSpaced.defaultValue()).isEqualTo(origin.defaultValue());
        assertThat(nameSpaced.defaultValue()).isEqualTo(origin.defaultValue());
        assertThat(nameSpaced.multiple()).isEqualTo(origin.multiple());
        assertThat(nameSpaced.required()).isEqualTo(origin.required());
        assertThat(nameSpaced.mutable()).isEqualTo(origin.mutable());
        assertThat(nameSpaced.suffix()).isEqualTo(origin.suffix());
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                CONFIG        | SUFFIX | KEY                        | EXPECTED_INFIX
                field         |        | field.name                 | name
                field         |        | field.                     | ''
                field         |        | field..                    | '.'
                field         |        | field.name.                | name.
                field         |        | field.field                | field
                field         |        | field..field               | .field
                field         |        | myfield.name               | ''
                field         |        | field.my.name              | my.name
                field         |        | field.my.first.name        | my.first.name
                map           | to     | map.topic.to               | topic
                map           | to     | map...to                   | .
                map           | to     | map. . .to                 | ' . '
                map           | to     | map.topicprefix.topic.to   | topicprefix.topic
                map           | to     | map.topic                  | ''
                map           | to     | map.to                     | ''
                map           | to     | map.topic.                 | ''
                map           | to     | pam.topic.to               | ''
                map           | to     | map.map.my.topic.to.to     | map.my.topic.to
                item-template |        | item-template.template1    | template1
                item-template |        | myitem.template1           | ''
                item-template |        | item-template.my.template1 | my.template1
                item-template |        | item-template              | ''
                item-template |        | item-template.             | ''
                    """)
    public void shouldExtractInfix(String config, String suffix, String key, String expectedInfix) {
        ConfParameter param =
                new ConfParameter(config, true, true, suffix, TEXT, true, defaultNull());
        Optional<String> infix = ConfigsSpec.extractInfix(param, key);
        if (!expectedInfix.isBlank()) {
            assertThat(infix).hasValue(expectedInfix);
        } else {
            assertThat(infix).isEmpty();
        }
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                CONFIG        | SUFFIX | KEY                    | VALUE
                field         |        | field.name             | name
                field         |        | field..                | name
                map           | to     | map.topic.to           | topic
                map           | to     | map...to               | topic
                item-template |        | item-template.template | my-template
                item-template |        | item-template..        | my-template
                    """)
    public void shouldPopulateMultipleParam(
            String config, String suffix, String key, String expectedValue) {
        ConfParameter param =
                new ConfParameter(config, true, true, suffix, TEXT, true, defaultNull());
        Map<String, String> source = Map.of(key, expectedValue);
        Map<String, String> dest = new HashMap<>();
        param.populate(source, dest);
        assertThat(dest).containsExactly(key, expectedValue);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                CONFIG        | SUFFIX | KEY
                field         |        | field
                field         |        | field.
                map           | to     | map.to
                map           | to     | map..to
                map           | to     | map. .to
                item-template |        | item-template
                item-template |        | item-template.
                    """)
    public void shouldNotPopulateMultipleParamDueToMissingInfix(
            String config, String suffix, String key) {
        ConfParameter param =
                new ConfParameter(config, true, true, suffix, TEXT, true, defaultNull());
        Map<String, String> source = Map.of(key, "value");
        Map<String, String> dest = new HashMap<>();
        assertThrows(ConfigException.class, () -> param.populate(source, dest));
    }

    @Test
    public void shouldGetCommandModeStrategy() {
        assertThat(CommandModeStrategy.from(true, false)).isEqualTo(CommandModeStrategy.AUTO);
        assertThat(CommandModeStrategy.from(true, true)).isEqualTo(CommandModeStrategy.AUTO);
        assertThat(CommandModeStrategy.from(false, true)).isEqualTo(CommandModeStrategy.ENFORCE);
        assertThat(CommandModeStrategy.from(false, false)).isEqualTo(CommandModeStrategy.NONE);
    }

    @Test
    public void shouldCommandModeStrategyMangeSnapshot() {
        assertThat(CommandModeStrategy.AUTO.manageSnapshot()).isFalse();
        assertThat(CommandModeStrategy.ENFORCE.manageSnapshot()).isTrue();
        assertThat(CommandModeStrategy.NONE.manageSnapshot()).isFalse();
    }
}
