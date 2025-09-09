
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
import com.lightstreamer.kafka.connect.client.ProxyAdapterClient;
import com.lightstreamer.kafka.connect.client.ProxyAdapterClient.ProxyAdapterConnection;
import com.lightstreamer.kafka.connect.client.ProxyAdapterClientOptions;
import com.lightstreamer.kafka.connect.common.DataProviderWrapper;
import com.lightstreamer.kafka.connect.common.ProxyCommunicator;
import com.lightstreamer.kafka.connect.config.DataAdapterConfig;
import com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig;
import com.lightstreamer.kafka.connect.server.ProviderServer;
import com.lightstreamer.kafka.connect.server.ProviderServer.ProviderServerConnection;
import com.lightstreamer.kafka.connect.server.ProviderServerOptions;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class LightstreamerSinkConnectorTask extends SinkTask {

    private static Logger logger = LoggerFactory.getLogger(LightstreamerSinkConnectorTask.class);

    private final Function<ProxyAdapterClientOptions, ProxyAdapterConnection>
            clientConnectionFactory;
    private final Function<ProviderServerOptions, ProviderServerConnection>
            providerConnectionFactory;
    private final Function<DataAdapterConfig, DataProviderWrapper> dataProviderFactory;
    private ProxyCommunicator communicator;

    public LightstreamerSinkConnectorTask() {
        this(
                ProxyAdapterConnection::newConnection,
                ProviderServerConnection::newServerConnection,
                config -> DataProviderWrapper.newWrapper(new StreamingDataAdapter(config)));
    }

    LightstreamerSinkConnectorTask(
            Function<ProxyAdapterClientOptions, ProxyAdapterConnection> adapterConnectionFactory,
            Function<ProviderServerOptions, ProviderServerConnection> providerConnectionFactory,
            Function<DataAdapterConfig, DataProviderWrapper> dataProviderFactory) {
        this.clientConnectionFactory = adapterConnectionFactory;
        this.providerConnectionFactory = providerConnectionFactory;
        this.dataProviderFactory = dataProviderFactory;
    }

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        logger.info("Starting LightstreamerSinkConnectorTask");
        LightstreamerConnectorConfig cfg = new LightstreamerConnectorConfig(props);
        DataAdapterConfig config = DataAdapterConfigurator.configure(cfg, context);
        if (cfg.isConnectionInversionEnabled()) {
            logger.info("Connection inversion is enabled, using ProviderServer");
            this.communicator =
                    new ProviderServer(
                            cfg.getProviderServerOptions(),
                            Thread.currentThread(),
                            providerConnectionFactory);

        } else {
            logger.info("Connection inversion is disabled, using ProxyAdapterClient");
            logger.info(
                    "Using Lightstreamer Proxy Adapter Client Options: {}",
                    cfg.getProxyAdapterClientOptions());

            this.communicator =
                    new ProxyAdapterClient(
                            cfg.getProxyAdapterClientOptions(),
                            Thread.currentThread(),
                            clientConnectionFactory);
        }
        communicator.start(toSupplier(config, dataProviderFactory));
    }

    private Supplier<DataProviderWrapper> toSupplier(
            DataAdapterConfig config, Function<DataAdapterConfig, DataProviderWrapper> func) {
        return () -> func.apply(config);
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        communicator.sendRecords(sinkRecords);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(
            Map<TopicPartition, OffsetAndMetadata> offsets) {
        return communicator.preCommit(offsets);
    }

    @Override
    public void stop() {
        logger.info("Stopping LightstreamerSinkConnectorTask");
        communicator.stop();
        communicator
                .closingException()
                .ifPresent(
                        e -> {
                            logger.warn(
                                    "Task closed due to the exception {}", e.getClass().getName());
                        });
    }
}
