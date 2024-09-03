
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

import com.lightstreamer.adapters.remote.ExceptionHandler;
import com.lightstreamer.adapters.remote.RemotingException;
import com.lightstreamer.kafka.connect.RecordSender;
import com.lightstreamer.kafka.connect.proxy.RemoteDataProviderServer.IOStreams;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public final class ProxyAdapterClient implements ExceptionHandler {

    public interface ProxyAdapterConnection {

        IOStreams open() throws IOException;

        default void notifyRetry() {}

        void close();

        static ProxyAdapterConnection newConnection(ProxyAdapterClientOptions options) {
            return new DefaultProxyAdapterConnection(options);
        }
    }

    private static class DefaultProxyAdapterConnection implements ProxyAdapterConnection {

        private final ProxyAdapterClientOptions options;
        private final Socket socket;

        DefaultProxyAdapterConnection(ProxyAdapterClientOptions options) {
            this.options = options;
            this.socket = new Socket();
        }

        @Override
        public final IOStreams open() throws IOException {
            SocketAddress address = new InetSocketAddress(options.hostname, options.port);
            socket.connect(address, options.connectionTimeout);
            return new IOStreams(socket.getInputStream(), socket.getOutputStream());
        }

        @Override
        public final void close() {
            if (socket != null && socket.isConnected()) {
                try {
                    socket.close();
                } catch (IOException e) {
                    logger.error("Error while closing the connection with the Proxy Adapter", e);
                }
            }
        }
    }

    private static Logger logger = LoggerFactory.getLogger(ProxyAdapterClient.class);

    private final ProxyAdapterClientOptions options;
    private final ProxyAdapterConnection connection;
    private RemoteDataProviderServer dataProviderServer;

    // The Sync task thread invoking the start method.
    private Thread currentSyncThread;

    // Holds the exception caught while communicating with Proxy Adapter.
    private AtomicReference<Throwable> closingException = new AtomicReference<>();

    public ProxyAdapterClient(ProxyAdapterClientOptions options, Thread synckTaskThread) {
        this(
                options,
                synckTaskThread,
                ProxyAdapterConnection::newConnection,
                new RemoteDataProviderServerImpl());
    }

    public ProxyAdapterClient(
            ProxyAdapterClientOptions options,
            Thread syncTaskThread,
            Function<ProxyAdapterClientOptions, ProxyAdapterConnection> connectionHandler,
            RemoteDataProviderServer dataProviderServer) {
        this.options = options;
        this.currentSyncThread = syncTaskThread;
        this.connection = connectionHandler.apply(options);
        this.dataProviderServer = dataProviderServer;
    }

    public void start(RecordSender sender) {
        logger.info(
                "Opening connection with Lightstreamer'server Proxy Adapter at {}:{}",
                options.hostname,
                options.port);

        int retries = options.connectionMaxRetries;
        while (retries >= 0) {
            try {
                IOStreams ioStreams = connection.open();
                logger.info("Connected to Lightstreamer Proxy Adapter");

                configureProviderServer(sender, ioStreams);

                logger.info("Starting communication with Lightstreamer Proxy Adapter");
                dataProviderServer.start();
                logger.info("Communication started");
                break;
            } catch (IOException io) {
                logger.error(
                        "Error while opening the connection with Lightstreamer Proxy Adapter", io);
                if (retries > 0) {
                    logger.info("{} retries left", retries);
                    retries--;
                    connection.notifyRetry();
                    try {
                        logger.info(
                                "Waiting for {} ms before retrying the connection",
                                options.connectionRetryDelayMs);
                        TimeUnit.MILLISECONDS.sleep(options.connectionRetryDelayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new ConnectException(ie);
                    }
                } else {
                    logger.info("No more retries left");
                    throw new ConnectException(io);
                }
            }
        }
    }

    public void stop() {
        logger.info("Closing connection with Lightstreamer Proxy Adapter");
        connection.close();
        logger.info("Stopping communication with Lightstreamer Proxy Adapter");
        dataProviderServer.close();
    }

    @Override
    public boolean handleIOException(IOException exception) {
        logger.warn("Handling IOException");
        return onException(exception);
    }

    @Override
    public boolean handleException(RemotingException exception) {
        logger.error("Handling RemotingException");
        return onException(exception);
    }

    public Optional<Throwable> closingException() {
        return Optional.ofNullable(closingException.get());
    }

    private void configureProviderServer(RecordSender sender, IOStreams ioStreams) {
        dataProviderServer.setIOStreams(ioStreams);
        dataProviderServer.setAdapter(sender);
        dataProviderServer.setExceptionHandler(this);
        if (options.username != null && options.password != null) {
            dataProviderServer.setCredentials(options.username, options.password);
        }
    }

    private boolean onException(Throwable t) {
        if (closingException.compareAndSet(null, t)) {
            logger.warn("Interrupting Sync task thread");
            currentSyncThread.interrupt();
        }
        return false;
    }
}
