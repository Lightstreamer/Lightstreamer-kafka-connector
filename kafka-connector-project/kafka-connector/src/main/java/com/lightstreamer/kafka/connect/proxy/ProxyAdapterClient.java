
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

package com.lightstreamer.kafka.connect.proxy;

import com.lightstreamer.adapters.remote.DataProvider;
import com.lightstreamer.adapters.remote.DataProviderServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.concurrent.TimeUnit;

public class ProxyAdapterClient {

    private static Logger logger = LoggerFactory.getLogger(ProxyAdapterClient.class);

    private final ProxyAdapterClientOptions options;
    private Socket socket;
    private DataProviderServer dataProviderServer;

    public ProxyAdapterClient(ProxyAdapterClientOptions options) {
        this.options = options;
    }

    public void start(DataProvider provider) {
        logger.info(
                "Starting connection with Lighstreamer'server Proxy Adapter at {}:{}",
                options.hostname,
                options.port);
        dataProviderServer = new DataProviderServer();
        dataProviderServer.setAdapter(provider);
        socket = new Socket();
        try {
            int retries = options.connectionRetriesCount;
            while (retries-- >= 0) {
                try {
                    SocketAddress address = new InetSocketAddress(options.hostname, options.port);
                    socket.connect(address, options.timeout);
                } catch (SocketTimeoutException e) {
                    logger.warn("Socket timeout", e);
                    if (retries > 0) {
                        logger.info("Waiting for %d ms before retrying the connection");
                        TimeUnit.MILLISECONDS.sleep(options.connectionRetryDelayMs);
                    }
                }
            }

            dataProviderServer.setReplyStream(socket.getOutputStream());
            dataProviderServer.setRequestStream(socket.getInputStream());
            if (options.username != null && options.password != null) {
                dataProviderServer.setRemoteUser(options.password);
                dataProviderServer.setRemotePassword(options.password);
            }
            dataProviderServer.start();
            logger.info("Connected to Lightstreame Proxy Aadapter");
        } catch (Exception e) {
            logger.error("Error while opening the connection with Lightstreamer Proxy Adapter", e);
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        if (socket != null && socket.isConnected()) {
            try {
                socket.close();
            } catch (IOException e) {
                logger.error("Error while closing the connection with the Proxy Adapter", e);
            }
        }
        if (dataProviderServer != null) {
            dataProviderServer.close();
        }
    }
}