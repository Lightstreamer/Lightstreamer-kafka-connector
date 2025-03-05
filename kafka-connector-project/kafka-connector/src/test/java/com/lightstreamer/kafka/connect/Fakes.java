
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

import com.lightstreamer.adapters.remote.DataProvider;
import com.lightstreamer.adapters.remote.DataProviderException;
import com.lightstreamer.adapters.remote.DiffAlgorithm;
import com.lightstreamer.adapters.remote.ExceptionHandler;
import com.lightstreamer.adapters.remote.FailureException;
import com.lightstreamer.adapters.remote.IndexedItemEvent;
import com.lightstreamer.adapters.remote.ItemEvent;
import com.lightstreamer.adapters.remote.ItemEventListener;
import com.lightstreamer.adapters.remote.RemotingException;
import com.lightstreamer.adapters.remote.SubscriptionException;
import com.lightstreamer.kafka.connect.proxy.ProxyAdapterClient.ProxyAdapterConnection;
import com.lightstreamer.kafka.connect.proxy.ProxyAdapterClientOptions;
import com.lightstreamer.kafka.connect.proxy.RemoteDataProviderServer;
import com.lightstreamer.kafka.connect.proxy.RemoteDataProviderServer.IOStreams;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** Provides fake implementations of several interfaces to support the unit tests. */
public class Fakes {

    public static class FakeProxyConnection implements ProxyAdapterConnection {

        public static FakeProxyConnection newFakeProxyConnection(ProxyAdapterClientOptions opts) {
            return new FakeProxyConnection();
        }

        public boolean openInvoked = false;
        public boolean closedInvoked = false;
        public IOStreams io;
        private int fakeFailures;
        public int retries = 0;

        public FakeProxyConnection() {
            this(0);
        }

        public FakeProxyConnection(int fakeFailures) {
            ByteArrayInputStream in = new ByteArrayInputStream("HelloWorld".getBytes());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            io = new IOStreams(in, out);
            this.fakeFailures = fakeFailures;
        }

        @Override
        public IOStreams open() throws IOException {
            while (fakeFailures > 0) {
                fakeFailures--;
                throw new IOException("Simulating failure connection");
            }
            this.openInvoked = true;
            return io;
        }

        @Override
        public void notifyRetry() {
            retries++;
        }

        @Override
        public void close() {
            this.closedInvoked = true;
        }
    }

    public static class FakeRemoteDataProviderServer implements RemoteDataProviderServer {

        // Credentials to be passed to the DataProviderServer.
        public String username;
        public String password;

        // The DataProvider to be managed by this instance.
        public DataProvider provider;

        // Flag indicating whether the start method has been invoked or not.
        public boolean startInvoked;

        // Flag indicating whether the close method has been invoked or not.
        public boolean closedInvoked = false;

        // The InputStream/OutputStream pair returned by the open method.
        public IOStreams ioStreams;

        // The ExceptionHandler for notifying errors.
        private ExceptionHandler handler;

        // The fake RemotingException to be passed to the ExceptionHandler to simulate
        // an error caught by the DataProviderServer.
        private Throwable fakeException;

        public FakeRemoteDataProviderServer(Throwable fakeException) {
            this.fakeException = fakeException;
        }

        public FakeRemoteDataProviderServer() {
            this(null);
        }

        @Override
        public void setCredentials(String username, String password) {
            this.username = username;
            this.password = password;
        }

        @Override
        public void setIOStreams(IOStreams streams) {
            this.ioStreams = streams;
        }

        @Override
        public void setAdapter(DataProvider provider) {
            this.provider = provider;
        }

        @Override
        public void start() {
            this.startInvoked = true;
            if (fakeException != null) {
                // Generate a simulated asynchronous exception immediately after a few milliseconds.
                Executor delayedExecutor =
                        CompletableFuture.delayedExecutor(500, TimeUnit.MILLISECONDS);
                Runnable r =
                        () -> {
                            if (fakeException instanceof RemotingException re) {
                                // The following thread is going to trigger the interruption of the
                                // Sync task thread.
                                handler.handleException(re);
                                // Being arrived later, the following exception will be not able to
                                // interrupt the Sync task thread.
                                handler.handleIOException(new IOException("Latecomer exception"));
                            } else if (fakeException instanceof IOException io) {
                                // The following thread is going to trigger the interruption of the
                                // Sync task thread.
                                handler.handleIOException(io);
                                // Being arrived later, the following exception will be not able to
                                // interrupt the Sync task thread.
                                handler.handleException(
                                        new RemotingException("Latecomer exception"));
                            } else {
                                throw new RuntimeException(
                                        "The provided exception is not of the expected type");
                            }
                        };
                CompletableFuture.runAsync(r, delayedExecutor);
            }
        }

        @Override
        public void close() {
            this.closedInvoked = true;
        }

        @Override
        public void setExceptionHandler(ExceptionHandler handler) {
            this.handler = handler;
        }
    }

    public static class FakeRecordSender implements RecordSender {

        public Collection<SinkRecord> records;

        @Override
        public void init(Map<String, String> parameters, String configFile)
                throws DataProviderException {}

        @Override
        public void setListener(ItemEventListener eventListener) {}

        @Override
        public void subscribe(String itemName) throws SubscriptionException, FailureException {}

        @Override
        public void unsubscribe(String itemName) throws SubscriptionException, FailureException {}

        @Override
        public boolean isSnapshotAvailable(String itemName) throws SubscriptionException {
            return false;
        }

        @Override
        public void sendRecords(Collection<SinkRecord> records) {
            this.records = records;
        }

        @Override
        public Map<TopicPartition, OffsetAndMetadata> preCommit(
                Map<TopicPartition, OffsetAndMetadata> offsets) {
            throw new UnsupportedOperationException("Unimplemented method 'preCommit'");
        }
    }

    public static class FakeSinkContext implements SinkTaskContext {

        public static class FakeErrantRecordReporter implements ErrantRecordReporter {

            public SinkRecord record;
            public Throwable caughtError;

            @Override
            public Future<Void> report(SinkRecord record, Throwable error) {
                this.record = record;
                this.caughtError = error;
                return CompletableFuture.failedFuture(error);
            }
        }

        private FakeErrantRecordReporter errantRecordReporter;

        public FakeSinkContext() {
            this(false);
        }

        public FakeSinkContext(boolean withReporter) {
            this.errantRecordReporter = withReporter ? new FakeErrantRecordReporter() : null;
        }

        @Override
        public Map<String, String> configs() {
            throw new UnsupportedOperationException("Unimplemented method 'configs'");
        }

        @Override
        public void offset(Map<TopicPartition, Long> offsets) {
            throw new UnsupportedOperationException("Unimplemented method 'offset'");
        }

        @Override
        public void offset(TopicPartition tp, long offset) {
            throw new UnsupportedOperationException("Unimplemented method 'offset'");
        }

        @Override
        public void timeout(long timeoutMs) {
            throw new UnsupportedOperationException("Unimplemented method 'timeout'");
        }

        @Override
        public Set<TopicPartition> assignment() {
            throw new UnsupportedOperationException("Unimplemented method 'assignment'");
        }

        @Override
        public void pause(TopicPartition... partitions) {
            throw new UnsupportedOperationException("Unimplemented method 'pause'");
        }

        @Override
        public void resume(TopicPartition... partitions) {
            throw new UnsupportedOperationException("Unimplemented method 'resume'");
        }

        @Override
        public void requestCommit() {
            throw new UnsupportedOperationException("Unimplemented method 'requestCommit'");
        }

        @Override
        public ErrantRecordReporter errantRecordReporter() {
            return errantRecordReporter;
        }
    }

    /**
     * Implements a fake {@code ItemEventListener} to track all the notified events, which can be
     * later investigated in the unit tests.
     */
    public static class FakeItemEventListener implements ItemEventListener {

        // Stores all item events sent through the update method.
        public List<Map<String, ?>> events = new ArrayList<>();

        @Override
        public void update(String itemName, ItemEvent itemEvent, boolean isSnapshot) {
            throw new UnsupportedOperationException("Unimplemented method 'update'");
        }

        @Override
        public void update(String itemName, Map<String, ?> itemEvent, boolean isSnapshot) {
            // Store the events.
            events.add(itemEvent);
        }

        @Override
        public void update(String itemName, IndexedItemEvent itemEvent, boolean isSnapshot) {
            throw new UnsupportedOperationException("Unimplemented method 'update'");
        }

        @Override
        public void endOfSnapshot(String itemName) {
            throw new UnsupportedOperationException("Unimplemented method 'endOfSnapshot'");
        }

        @Override
        public void clearSnapshot(String itemName) {
            throw new UnsupportedOperationException("Unimplemented method 'clearSnapshot'");
        }

        @Override
        public void declareFieldDiffOrder(
                String itemName, Map<String, DiffAlgorithm[]> algorithmsMap) {
            throw new UnsupportedOperationException("Unimplemented method 'declareFieldDiffOrder'");
        }

        @Override
        public void failure(Exception exception) {
            throw new UnsupportedOperationException("Unimplemented method 'failure'");
        }
    }

    private Fakes() {}
}
