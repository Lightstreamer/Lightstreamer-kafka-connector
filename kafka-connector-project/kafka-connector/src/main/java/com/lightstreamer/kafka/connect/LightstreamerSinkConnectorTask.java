
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

import com.lightstreamer.kafka.common.utils.Version;
import com.lightstreamer.kafka.connect.DataAdapterConfigurator.DataAdapterConfig;
import com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig;
import com.lightstreamer.kafka.connect.proxy.ProxyAdapterClient;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class LightstreamerSinkConnectorTask extends SinkTask {

    private static Logger logger = LoggerFactory.getLogger(LightstreamerSinkConnectorTask.class);

    private StreamingDataAdapter adapter;
    private ProxyAdapterClient proxyAdapterClient;

    public LightstreamerSinkConnectorTask() {}

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        logger.info("Starting LightstreamerSinkConnectorTask");
        LightstreamerConnectorConfig cfg = new LightstreamerConnectorConfig(props);
        DataAdapterConfig config = DataAdapterConfigurator.configure(cfg);

        adapter = new StreamingDataAdapter(config, context);
        proxyAdapterClient = new ProxyAdapterClient(cfg.getProxyAdapterClientOptions());
        proxyAdapterClient.start(adapter);
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        adapter.streamEvents(sinkRecords);
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {}

    @Override
    public void stop() {
        proxyAdapterClient.stop();
    }
}
