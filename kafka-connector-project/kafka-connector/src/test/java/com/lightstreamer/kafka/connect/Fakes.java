
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

import com.lightstreamer.adapters.remote.DataProviderException;
import com.lightstreamer.adapters.remote.ExceptionHandler;
import com.lightstreamer.adapters.remote.FailureException;
import com.lightstreamer.adapters.remote.ItemEventListener;
import com.lightstreamer.adapters.remote.RemotingException;
import com.lightstreamer.adapters.remote.SubscriptionException;
import com.lightstreamer.kafka.connect.client.ProxyAdapterClient.ProxyAdapterConnection;
import com.lightstreamer.kafka.connect.common.DataProviderWrapper;
import com.lightstreamer.kafka.connect.common.DataProviderWrapper.IOStreams;
import com.lightstreamer.kafka.connect.common.RecordSender;
import com.lightstreamer.kafka.connect.server.ProviderServer.ProviderServerConnection;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.SocketException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Provides fake implementations of several interfaces to support the unit tests. */
public class Fakes {

    public static class FakeProxyConnection implements ProxyAdapterConnection {

        public boolean openInvoked = false;
        public boolean closedInvoked = false;
        public IOStreams io;
        private int fakeFailures;
        public int retries = 0;

        public FakeProxyConnection() {
            this(0);
        }

        public FakeProxyConnection(int fakeFailures) {
            this.io =
                    new IOStreams(
                            new ByteArrayInputStream("HelloWorld".getBytes()),
                            new ByteArrayOutputStream());
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

    public static class FakeProviderConnection implements ProviderServerConnection {

        private final BlockingQueue<IOStreams> streamsQueue = new ArrayBlockingQueue<>(2);
        public final AtomicInteger acceptInvoked = new AtomicInteger();
        public boolean closedInvoked = false;

        public FakeProviderConnection() {
            this(false);
        }

        public FakeProviderConnection(boolean trowException) {
            if (trowException) {
                throw new RuntimeException("Simulated server connection issue at startup");
            }
        }

        public void triggerAcceptConnections(int numConnections) {
            for (int i = 0; i < numConnections; i++) {
                streamsQueue.offer(
                        new IOStreams(
                                new ByteArrayInputStream("Accept".getBytes()),
                                new ByteArrayOutputStream()));
            }
        }

        @Override
        public IOStreams accept() throws IOException {
            try {
                IOStreams streams = streamsQueue.take();
                BufferedReader bufferedReader =
                        new BufferedReader(new InputStreamReader(streams.in()));
                String line = bufferedReader.readLine();
                switch (line) {
                    case "Close" -> {
                        closedInvoked = true;
                        throw new SocketException("Simulated socket exception");
                    }
                    case "Accept" -> {
                        acceptInvoked.incrementAndGet();
                        return streams;
                    }
                    default ->
                            throw new RuntimeException(
                                    "Unexpected line read from the InputStream: " + line);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            this.closedInvoked = true;
            streamsQueue.offer(
                    new IOStreams(
                            new ByteArrayInputStream("Close".getBytes()),
                            new ByteArrayOutputStream()));
        }

        @Override
        public boolean isClosed() {
            return closedInvoked;
        }
    }

    public static class FakeDataProviderWrapper implements DataProviderWrapper {

        // Credentials to be passed to the DataProviderServer.
        public String username;
        public String password;

        // Flag indicating whether the start method has been invoked or not.
        public volatile boolean startInvoked;

        // Flag indicating whether the close method has been invoked or not.
        public volatile boolean closedInvoked = false;

        // The InputStream/OutputStream pair returned by the open method.
        public IOStreams ioStreams;

        // The ExceptionHandler for notifying errors.
        private ExceptionHandler handler;

        // The fake RemotingException to be passed to the ExceptionHandler to simulate
        // an error caught by the DataProviderServer.
        private Throwable fakeException;

        public Collection<SinkRecord> records;
        private CloseHook hook;

        public FakeDataProviderWrapper(Throwable fakeException) {
            this.fakeException = fakeException;
        }

        public FakeDataProviderWrapper() {
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
            if (hook != null) {
                hook.closed(this);
            }
        }

        @Override
        public void setExceptionHandler(ExceptionHandler handler) {
            this.handler = handler;
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

        @Override
        public void setCloseHook(CloseHook hook) {
            this.hook = hook;
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

    private Fakes() {}
}
