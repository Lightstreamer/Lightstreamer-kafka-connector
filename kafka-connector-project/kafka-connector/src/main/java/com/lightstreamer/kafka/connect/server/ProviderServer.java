
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

package com.lightstreamer.kafka.connect.server;

import com.lightstreamer.adapters.remote.ExceptionHandler;
import com.lightstreamer.adapters.remote.RemotingException;
import com.lightstreamer.kafka.connect.common.DataProviderWrapper;
import com.lightstreamer.kafka.connect.common.DataProviderWrapper.CloseHook;
import com.lightstreamer.kafka.connect.common.DataProviderWrapper.IOStreams;
import com.lightstreamer.kafka.connect.common.ProxyCommunicator;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

public final class ProviderServer implements ProxyCommunicator {

    public interface ProviderServerConnection extends AutoCloseable {

        IOStreams accept() throws IOException;

        @Override
        void close() throws IOException;

        static ProviderServerConnection newServerConnection(ProviderServerOptions options) {
            try {
                return new DefaultProviderServerConnection(options);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create ProviderServerConnection", e);
            }
        }

        boolean isClosed();
    }

    private static class DefaultProviderServerConnection implements ProviderServerConnection {

        private final ProviderServerOptions options;
        private final ServerSocket serverSocket;

        DefaultProviderServerConnection(ProviderServerOptions options) throws IOException {
            this.options = options;
            this.serverSocket = new ServerSocket(options.port);
        }

        public IOStreams accept() throws IOException {
            return new IOStreams(serverSocket.accept());
        }

        @Override
        public void close() throws IOException {
            serverSocket.close();
        }

        @Override
        public boolean isClosed() {
            return serverSocket.isClosed();
        }
    }

    private static class ProviderExceptionHandler implements ExceptionHandler {

        private final DataProviderWrapper provider;
        private volatile boolean closed = false;

        ProviderExceptionHandler(DataProviderWrapper provider) {
            this.provider = provider;
        }

        @Override
        public boolean handleException(RemotingException exception) {
            logger.error("Handling RemotingException");
            return onException(exception);
        }

        @Override
        public boolean handleIOException(IOException exception) {
            logger.warn("Handling IOException");
            return onException(exception);
        }

        private boolean onException(Throwable t) {
            synchronized (this) {
                if (!closed) {
                    closed = true;
                    logger.error("Connection with Lightstreamer Proxy Adapter closed", t);
                    provider.close();
                }
            }
            return false;
        }
    }

    private static Logger logger = LoggerFactory.getLogger(ProviderServer.class);

    private final ProviderServerOptions options;

    // The Sync task thread invoking the start method.
    private final Thread currentSyncThread;

    private final Function<ProviderServerOptions, ProviderServerConnection> connectionFactory;

    // Holds the exception caught while communicating with Proxy Adapter.
    private AtomicReference<Throwable> closingException = new AtomicReference<>();

    private final Semaphore semaphore;

    private final Set<DataProviderWrapper> activeProviders = new HashSet<>();

    private final CloseHook closeHook;

    private volatile boolean acceptConnections = true;

    private CompletableFuture<Void> acceptLoop;

    private ProviderServerConnection serverConnection;

    public ProviderServer(
            ProviderServerOptions options,
            Thread syncTaskThread,
            Function<ProviderServerOptions, ProviderServerConnection> connectionFactory) {
        this.options = options;
        this.currentSyncThread = syncTaskThread;
        this.connectionFactory = connectionFactory;
        this.semaphore = new Semaphore(options.maxProxyAdapterConnections);
        this.closeHook =
                provider -> {
                    logger.info("Provider closed, releasing semaphore");
                    semaphore.release();
                    activeProviders.remove(provider);
                    logger.info("{} active providers left", activeProviders.size());
                };
    }

    @Override
    public void start(Supplier<DataProviderWrapper> dataProviderSupplier) {
        logger.info("Start listening on port {}...", options.port);
        this.acceptLoop = CompletableFuture.runAsync(runAcceptLoop(dataProviderSupplier));
    }

    @Override
    public void sendRecords(Collection<SinkRecord> records) {
        for (DataProviderWrapper provider : activeProviders) {
            provider.sendRecords(records);
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(
            Map<TopicPartition, OffsetAndMetadata> offsets) {
        return offsets;
    }

    private Runnable runAcceptLoop(Supplier<DataProviderWrapper> dataProviderSupplier) {
        return () -> {
            try (ProviderServerConnection connection = connectionFactory.apply(options)) {
                this.serverConnection = connection;
                while (acceptConnections) {
                    try {
                        logger.info("Ready to accept {} connections", semaphore.availablePermits());
                        semaphore.acquire();

                        IOStreams ioStreams = connection.accept();
                        logger.info(
                                "Accepted connection, spinning up a new Remote Provider to handle the communication with Lightstreamer Proxy Adapter");
                        DataProviderWrapper dataProvider =
                                configureProviderServer(dataProviderSupplier, ioStreams);
                        activeProviders.add(dataProvider);

                        logger.info("Starting communication with Lightstreamer Proxy Adapter");
                        dataProvider.start();
                        logger.info("Communication started");
                    } catch (IOException | InterruptedException e) {
                        logger.error(
                                "Error while accepting the connection from Lightstreamer Proxy Adapter",
                                e);
                        semaphore.release();
                    }
                }
            } catch (Exception e) {
                logger.error("Error while starting to listen on port {}", options.port, e);
                logger.warn("Interrupting Sync task thread");
                closingException.set(e);
                currentSyncThread.interrupt();
            }
        };
    }

    @Override
    public void stop() {
        logger.info("Stopping communication with Lightstreamer Proxy Adapter");
        if (!acceptConnections) {
            return;
        }

        acceptConnections = false;

        logger.info("Closing all active Remote Providers");
        // This also ensures that all acquired semaphores are released.
        activeProviders.forEach(DataProviderWrapper::close);

        if (serverConnection != null && !serverConnection.isClosed()) {
            try {
                serverConnection.close();
            } catch (IOException e) {
                logger.warn("Error while closing server connection", e);
            }
        }

        acceptLoop.join();
        logger.info("Communication stopped");
    }

    @Override
    public Optional<Throwable> closingException() {
        return Optional.ofNullable(closingException.get());
    }

    private DataProviderWrapper configureProviderServer(
            Supplier<DataProviderWrapper> dataProviderSupplier, IOStreams ioStreams) {
        DataProviderWrapper wrapper = dataProviderSupplier.get();
        wrapper.setIOStreams(ioStreams);
        wrapper.setExceptionHandler(new ProviderExceptionHandler(wrapper));
        wrapper.setCloseHook(closeHook);
        if (options.username != null && options.password != null) {
            wrapper.setCredentials(options.username, options.password);
        }
        return wrapper;
    }
}
