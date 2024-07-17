
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

import com.lightstreamer.kafka.config.TopicsConfig;
import com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig;
import com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.connect.mapping.selectors.ConnectSelectorsSuppliers;
import com.lightstreamer.kafka.mapping.Fields;
import com.lightstreamer.kafka.mapping.Items;
import com.lightstreamer.kafka.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.mapping.selectors.SelectorSuppliers;
import com.lightstreamer.kafka.mapping.selectors.ValuesExtractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.Map;

public class DataAdapterConfigurator {

    public interface DataAdapterConfig {

        SocketAddress proxyAdapterAddress();

        ValuesExtractor<Object, Object> fieldsExtractor();

        ItemTemplates<Object, Object> itemTemplates();

        RecordErrorHandlingStrategy recordErrorHandlingStrategy();
    }

    private static record DataAdapterConfigImpl(
            SocketAddress proxyAdapterAddress,
            ValuesExtractor<Object, Object> fieldsExtractor,
            ItemTemplates<Object, Object> itemTemplates,
            RecordErrorHandlingStrategy recordErrorHandlingStrategy)
            implements DataAdapterConfig {}

    private static Logger logger = LoggerFactory.getLogger(LightstreamerSinkConnectorTask.class);

    private DataAdapterConfigurator() {}

    static DataAdapterConfig configure(Map<String, String> props) {
        LightstreamerConnectorConfig config = new LightstreamerConnectorConfig(props);
        TopicsConfig topicsConfig =
                TopicsConfig.of(config.getItemTemplates(), config.getTopicMappings());
        SelectorSuppliers<Object, Object> sSuppliers =
                SelectorSuppliers.of(
                        ConnectSelectorsSuppliers.keySelectorSupplier(),
                        ConnectSelectorsSuppliers.valueSelectorSupplier());
        ItemTemplates<Object, Object> templates = Items.from(topicsConfig, sSuppliers);
        // logger.info("Constructed item templates: {}", itemTemplates);

        Map<String, String> fieldMappings = config.getFieldMappings();
        logger.info("fieldsMapping: {}", fieldMappings);

        ValuesExtractor<Object, Object> fieldsExtractor =
                Fields.fromMapping(fieldMappings, sSuppliers);

        return new DataAdapterConfigImpl(
                config.getProxyAdapterAddress(),
                fieldsExtractor,
                templates,
                config.getErrRecordErrorHandlingStrategy());
    }
}
