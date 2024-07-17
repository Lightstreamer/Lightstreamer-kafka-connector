
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
import com.lightstreamer.kafka.connect.DataAdapterConfigurator.DataAdapterConfig;
import com.lightstreamer.kafka.utils.Version;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Map;

public class LightstreamerSinkConnectorTask extends SinkTask {

    private static Logger logger = LoggerFactory.getLogger(LightstreamerSinkConnectorTask.class);

    private Map<String, String> props;
    private StreamingDataAdapter adapter;
    private Socket socket;

    public LightstreamerSinkConnectorTask() {}

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        logger.info("Starting LightstreamerSinkConnectorTask");
        this.props = props;
        DataAdapterConfig config = DataAdapterConfigurator.configure(props);

        adapter = new StreamingDataAdapter(config, context);
        DataProviderServer dataProviderServer = new DataProviderServer();
        dataProviderServer.setAdapter(adapter);

        socket = startAdapter(dataProviderServer, config.proxyAdapterAddress());
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
                logger.error("Error while closing the connection with the Proxy Adapter", e);
            }
        }
    }

    private Socket startAdapter(DataProviderServer dataProviderServer, SocketAddress address) {
        try {
            Socket socket = new Socket();
            socket.connect(address);
            dataProviderServer.setReplyStream(socket.getOutputStream());
            dataProviderServer.setRequestStream(socket.getInputStream());
            dataProviderServer.start();
            return socket;
        } catch (Exception e) {
            logger.error("Error while opening the connection with the Proxy Adapter", e);
            throw new RuntimeException(e);
        }
    }
}
