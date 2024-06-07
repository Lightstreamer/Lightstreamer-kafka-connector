
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

package com.lightstreamer.kafka.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LightstreamerSinkConnector extends SinkConnector {

    static final ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            "lightstreamer.host",
                            Type.STRING,
                            null,
                            Importance.HIGH,
                            "Lightstreamer server hostname")
                    .define(
                            "lightstreamer.port",
                            Type.INT,
                            null,
                            Importance.HIGH,
                            "Lightstreamer server port");

    private static Logger logger = LoggerFactory.getLogger(LightstreamerSinkConnector.class);

    private Map<String, String> props;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        logger.info("Starting LightstreamerSinkConnector");
        this.props = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return LightstreamerSinkConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(props);
        }
        return configs;
    }

    @Override
    public void stop() {
        logger.info("Stopping LightstreamerSinkConnector");
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
