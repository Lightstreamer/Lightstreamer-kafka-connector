
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

import com.lightstreamer.adapters.remote.DataProviderServer;
import com.lightstreamer.adapters.remote.MetadataProviderServer;
import com.lightstreamer.adapters.remote.metadata.LiteralBasedProvider;
import com.lightstreamer.kafka.config.TopicsConfig;
import com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig;
import com.lightstreamer.kafka.connect.mapping.selectors.ConnectSelectorsSuppliers;
import com.lightstreamer.kafka.mapping.Fields;
import com.lightstreamer.kafka.mapping.Items;
import com.lightstreamer.kafka.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.mapping.selectors.SelectorSuppliers;
import com.lightstreamer.kafka.mapping.selectors.ValuesExtractor;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public class LightstreamerSinkConnectorTask extends SinkTask {

    private Map<String, String> props = null;

    private StreamingDataAdapter adapter;

    private Socket socket;

    private LightstreamerConnectorConfig config;

    private static Logger logger = LoggerFactory.getLogger(LightstreamerSinkConnectorTask.class);

    public LightstreamerSinkConnectorTask() {}

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public void start(Map<String, String> props) {
        logger.info("Starting LightstreamerSinkConnectorTask");
        this.props = props;
        this.config = new LightstreamerConnectorConfig(props);
        MetadataProviderServer metadataProviderServer = new MetadataProviderServer();
        metadataProviderServer.setAdapter(new LiteralBasedProvider());

        Map<String, String> topicMappings = config.getTopicMappings();
        logger.info("topic.mappings: {}", topicMappings);

        Map<String, String> itemTemplates = config.getItemTemplates();
        logger.info("item.templates: {}", itemTemplates);

        TopicsConfig topicsConfig = TopicsConfig.of(itemTemplates, topicMappings);
        SelectorSuppliers<Object, Object> sSuppliers =
                SelectorSuppliers.of(
                        ConnectSelectorsSuppliers.keySelectorSupplier(false),
                        ConnectSelectorsSuppliers.valueSelectorSupplier(false));
        ItemTemplates<Object, Object> templates = Items.templatesFrom(topicsConfig, sSuppliers);
        logger.info("Constructed item templates: {}", itemTemplates);

        Map<String, String> fieldMappings =
                config.getList(LightstreamerConnectorConfig.FIELD_MAPPINGS).stream()
                        .collect(Collectors.toMap(s -> s.split(":")[0], s -> s.split(":")[1]));

        logger.info("fieldsMapping: {}", fieldMappings);
        ValuesExtractor<Object, Object> fieldsExtractor =
                Fields.fromMapping(fieldMappings, sSuppliers);

        adapter =
                new StreamingDataAdapter(
                        templates,
                        fieldsExtractor,
                        context,
                        config.getErrRecordErrorHandlingStrategy());
        DataProviderServer dataProviderServer = new DataProviderServer();
        dataProviderServer.setAdapter(adapter);

        socket = startAdapter(props, dataProviderServer);
    }

    private Socket startAdapter(Map<String, String> props, DataProviderServer dataProviderServer) {
        try {
            String address =
                    config.getString(
                            LightstreamerConnectorConfig.LIGHTREAMER_PROXY_ADAPTER_ADDRESS);
            String[] addressTokens = address.split(":");
            Socket socket = new Socket(addressTokens[0], Integer.valueOf(addressTokens[1]));
            dataProviderServer.setReplyStream(socket.getOutputStream());
            dataProviderServer.setRequestStream(socket.getInputStream());
            dataProviderServer.start();
            return socket;
        } catch (Exception e) {
            logger.error("", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        adapter.streamEvents(sinkRecords);
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {}

    @Override
    public void stop() {
        if (socket != null && socket.isConnected()) {
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
