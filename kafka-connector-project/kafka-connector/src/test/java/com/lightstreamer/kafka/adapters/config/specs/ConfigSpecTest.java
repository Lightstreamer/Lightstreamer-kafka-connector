
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
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.DefaultHolder.defaultValue;

import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfParameter;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.Options;

import org.junit.jupiter.api.Test;

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
}
