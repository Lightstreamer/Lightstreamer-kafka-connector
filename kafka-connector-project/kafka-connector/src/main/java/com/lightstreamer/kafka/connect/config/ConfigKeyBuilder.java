
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

package com.lightstreamer.kafka.connect.config;

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Recommender;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Collections;
import java.util.List;

public class ConfigKeyBuilder {
    private String name;
    private Type type;
    private String documentation;
    private Object defaultValue = NO_DEFAULT_VALUE;
    private Validator validator;
    private Importance importance;
    private String group;
    private int orderInGroup = -1;
    private Width width = Width.NONE;
    private String displayName;
    private List<String> dependents = Collections.emptyList();
    private Recommender recommender;
    private boolean internalConfig = false;

    ConfigKeyBuilder name(String name) {
        this.name = name;
        return this;
    }

    ConfigKeyBuilder type(Type type) {
        this.type = type;
        return this;
    }

    ConfigKeyBuilder documentation(String documentation) {
        this.documentation = documentation;
        return this;
    }

    ConfigKeyBuilder defaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    ConfigKeyBuilder validator(Validator validator) {
        this.validator = validator;
        return this;
    }

    ConfigKeyBuilder importance(Importance importance) {
        this.importance = importance;
        return this;
    }

    ConfigKeyBuilder group(String group) {
        this.group = group;
        return this;
    }

    ConfigKeyBuilder orderInGroup(int orderInGroup) {
        this.orderInGroup = orderInGroup;
        return this;
    }

    ConfigKeyBuilder width(Width orderInGroup) {
        this.width = orderInGroup;
        return this;
    }

    ConfigKeyBuilder displayName(String displayName) {
        this.displayName = displayName;
        return this;
    }

    ConfigKeyBuilder dependents(List<String> displayName) {
        this.dependents = displayName;
        return this;
    }

    ConfigKeyBuilder recommender(Recommender recommender) {
        this.recommender = recommender;
        return this;
    }

    ConfigKeyBuilder internalConfig(boolean internalConfig) {
        this.internalConfig = internalConfig;
        return this;
    }

    ConfigKey build() {
        return new ConfigKey(
                name,
                type,
                defaultValue,
                validator,
                importance,
                documentation,
                group,
                orderInGroup,
                width,
                displayName,
                dependents,
                recommender,
                internalConfig);
    }
}
