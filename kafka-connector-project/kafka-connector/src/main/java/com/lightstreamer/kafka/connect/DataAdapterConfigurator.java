
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

import com.lightstreamer.kafka.common.config.ConfigException;
import com.lightstreamer.kafka.common.config.FieldConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.selectors.DataExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KeyValueSelectorSuppliers;
import com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig;
import com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.connect.mapping.selectors.ConnectSelectorsSuppliers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataAdapterConfigurator {

    public interface DataAdapterConfig {

        DataExtractor<Object, Object> fieldsExtractor();

        ItemTemplates<Object, Object> itemTemplates();

        RecordErrorHandlingStrategy recordErrorHandlingStrategy();
    }

    private static record DataAdapterConfigImpl(
            DataExtractor<Object, Object> fieldsExtractor,
            ItemTemplates<Object, Object> itemTemplates,
            RecordErrorHandlingStrategy recordErrorHandlingStrategy)
            implements DataAdapterConfig {}

    private static Logger logger = LoggerFactory.getLogger(LightstreamerSinkConnectorTask.class);

    private DataAdapterConfigurator() {}

    static DataAdapterConfig configure(LightstreamerConnectorConfig config) throws ConfigException {
        TopicConfigurations topicsConfig =
                TopicConfigurations.of(
                        config.getItemTemplateConfigs(),
                        config.getTopicMappings(),
                        config.isRegexEnabled());
        KeyValueSelectorSuppliers<Object, Object> sSuppliers = new ConnectSelectorsSuppliers();
        try {
            ItemTemplates<Object, Object> templates = Items.templatesFrom(topicsConfig, sSuppliers);
            logger.info("Constructed item templates: {}", templates);

            FieldConfigs fieldConfigs = config.getFieldConfigs();
            logger.info("fieldsMapping: {}", fieldConfigs);

            DataExtractor<Object, Object> fieldsExtractor =
                    fieldConfigs.extractor(
                            sSuppliers,
                            config.isRecordMappingSkipFailedEnabled(),
                            config.isRecordMappingMapNonScalarValuesEnabled());

            return new DataAdapterConfigImpl(
                    fieldsExtractor, templates, config.getErrRecordErrorHandlingStrategy());
        } catch (ExtractionException e) {
            throw new ConfigException(e.getMessage());
        }
    }
}
