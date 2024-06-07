
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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

public class LightstreamerConnectorConfig extends AbstractConfig {

    public static final String LIGHTREAMER_HOST = "lightstreamer.host";

    public static final String LIGHTREAMER_PORT = "lightstreamer.port";

    public static final String ITEM_TEMPLATES = "item.templates";

    public static final String MAP_TOPICS_TO = "map.topics.to";

    public static final String FIELD_NAMES = "field.names";

    public LightstreamerConnectorConfig(Map<?, ?> originals) {
        super(configDef(), originals);
    }

    protected static ConfigDef configDef() {
        return new ConfigDef()
                .define(
                        LIGHTREAMER_HOST,
                        Type.STRING,
                        null,
                        Importance.HIGH,
                        "Lightstreamer server hostname")
                .define(
                        LIGHTREAMER_PORT,
                        Type.INT,
                        null,
                        Importance.HIGH,
                        "Lightstreamer server port")
                .define(
                        ITEM_TEMPLATES,
                        ConfigDef.Type.LIST,
                        null,
                        ConfigDef.Importance.HIGH,
                        "Item template expressions")
                .define(MAP_TOPICS_TO, ConfigDef.Type.LIST, null, ConfigDef.Importance.HIGH, "")
                .define(
                        FIELD_NAMES,
                        ConfigDef.Type.LIST,
                        null,
                        ConfigDef.Importance.HIGH,
                        "Name of the Lightsteramer fields to be mapped");
    }
}
