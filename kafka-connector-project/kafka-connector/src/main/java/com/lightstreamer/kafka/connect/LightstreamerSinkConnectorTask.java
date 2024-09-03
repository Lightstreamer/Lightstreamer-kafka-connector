
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
import com.lightstreamer.kafka.connect.proxy.ProxyAdapterClient.ProxyAdapterConnection;
import com.lightstreamer.kafka.connect.proxy.ProxyAdapterClientOptions;
import com.lightstreamer.kafka.connect.proxy.RemoteDataProviderServer;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public class LightstreamerSinkConnectorTask extends SinkTask {

    private static Logger logger = LoggerFactory.getLogger(LightstreamerSinkConnectorTask.class);

    private final Function<ProxyAdapterClientOptions, ProxyAdapterConnection> connectionFactory;
    private final BiFunction<DataAdapterConfig, SinkTaskContext, RecordSender> recordSenderFactory;
    private final RemoteDataProviderServer dataProviderServer;
    private RecordSender recordSender;
    private ProxyAdapterClient proxyAdapterClient;

    public LightstreamerSinkConnectorTask() {
        this(
                ProxyAdapterConnection::newConnection,
                (config, context) -> new StreamingDataAdapter(config, context),
                RemoteDataProviderServer.newDataProviderServer());
    }

    LightstreamerSinkConnectorTask(
            Function<ProxyAdapterClientOptions, ProxyAdapterConnection>
                    proxyAdapterConnectionFactory,
            BiFunction<DataAdapterConfig, SinkTaskContext, RecordSender> recordSenderFactory,
            RemoteDataProviderServer dataProviderServer) {
        this.connectionFactory = proxyAdapterConnectionFactory;
        this.recordSenderFactory = recordSenderFactory;
        this.dataProviderServer = dataProviderServer;
    }

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        logger.info("Starting LightstreamerSinkConnectorTask");
        LightstreamerConnectorConfig cfg = new LightstreamerConnectorConfig(props);
        DataAdapterConfig config = DataAdapterConfigurator.configure(cfg);

        this.recordSender = recordSenderFactory.apply(config, context);
        this.proxyAdapterClient =
                new ProxyAdapterClient(
                        cfg.getProxyAdapterClientOptions(),
                        Thread.currentThread(),
                        connectionFactory,
                        dataProviderServer);
        this.proxyAdapterClient.start(recordSender);
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        this.recordSender.sendRecords(sinkRecords);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(
            Map<TopicPartition, OffsetAndMetadata> offsets) {
        return this.recordSender.preCommit(offsets);
    }

    @Override
    public void stop() {
        logger.info("Stopping LightstreamerSinkConnectorTask");
        proxyAdapterClient.stop();
        proxyAdapterClient
                .closingException()
                .ifPresent(
                        e -> {
                            logger.info(
                                    "Task closed due to the exception {}", e.getClass().getName());
                        });
    }
}
