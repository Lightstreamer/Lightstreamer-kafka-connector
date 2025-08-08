
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

import static com.google.common.truth.Truth.assertThat;

import com.lightstreamer.kafka.connect.Fakes.FakeDataProviderWrapper;
import com.lightstreamer.kafka.connect.Fakes.FakeProviderConnection;
import com.lightstreamer.kafka.connect.common.DataProviderWrapper;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class ProviderServerTest {

    @Test
    void shouldStartWithCredentials() throws InterruptedException {
        ProviderServerOptions options =
                new ProviderServerOptions.Builder(6661)
                        .maxProxyAdapterConnections(1)
                        .username("username")
                        .password("password")
                        .build();

        FakeProviderConnection providerConnection = new FakeProviderConnection();

        FakeDataProviderWrapper wrapper = new FakeDataProviderWrapper();
        assertThat(wrapper.startInvoked).isFalse();

        ProviderServer server =
                new ProviderServer(options, Thread.currentThread(), opts -> providerConnection);
        server.start((() -> wrapper));

        // Simulate the connection loop accepting a connection.
        providerConnection.triggerAcceptConnections(1);

        // Just wait a bit to ensure the connection loop has started.
        TimeUnit.MILLISECONDS.sleep(100);

        assertThat(providerConnection.acceptInvoked.get()).isEqualTo(1);
        assertThat(wrapper.username).isEqualTo("username");
        assertThat(wrapper.password).isEqualTo("password");
        assertThat(wrapper.startInvoked).isTrue();
        assertThat(server.closingException()).isEmpty();
    }

    static Stream<Arguments> incompleteCredentials() {
        return Stream.of(
                Arguments.of("username", null),
                Arguments.of(null, "password"),
                Arguments.of(null, null));
    }

    @ParameterizedTest
    @MethodSource("incompleteCredentials")
    void shouldStartWithoutCredentials(String username, String password)
            throws InterruptedException {
        ProviderServerOptions options =
                new ProviderServerOptions.Builder(6661)
                        .maxProxyAdapterConnections(1)
                        .username(username)
                        .password(password)
                        .build();

        FakeProviderConnection providerConnection = new FakeProviderConnection();

        FakeDataProviderWrapper wrapper = new FakeDataProviderWrapper();
        assertThat(wrapper.startInvoked).isFalse();

        ProviderServer server =
                new ProviderServer(options, Thread.currentThread(), opts -> providerConnection);
        server.start((() -> wrapper));

        // Simulate the connection loop accepting a connection.
        providerConnection.triggerAcceptConnections(1);

        // Just wait a bit to ensure the connection loop has started.
        TimeUnit.MILLISECONDS.sleep(100);

        assertThat(providerConnection.acceptInvoked.get()).isEqualTo(1);
        assertThat(wrapper.username).isNull();
        assertThat(wrapper.password).isNull();
        assertThat(wrapper.startInvoked).isTrue();
        assertThat(server.closingException()).isEmpty();
    }

    @Test
    void shouldAcceptMultipleConnectionsAndStopGracefully() throws InterruptedException {
        ProviderServerOptions options =
                new ProviderServerOptions.Builder(6661).maxProxyAdapterConnections(2).build();

        FakeProviderConnection providerConnection = new FakeProviderConnection();

        ProviderServer server =
                new ProviderServer(options, Thread.currentThread(), opts -> providerConnection);
        server.start((() -> new FakeDataProviderWrapper()));

        // Simulate the connection loop accepting two connections.
        providerConnection.triggerAcceptConnections(2);

        // Just wait a bit to ensure the connection loop has started.
        TimeUnit.MILLISECONDS.sleep(100);

        assertThat(providerConnection.acceptInvoked.get()).isEqualTo(2);
        assertThat(server.getActiveProviders().size()).isEqualTo(2);
        for (DataProviderWrapper wrapper : server.getActiveProviders()) {
            assertThat(((FakeDataProviderWrapper) wrapper).startInvoked).isTrue();
        }

        // Simulate sending records to the Proxy Adapters.
        SinkRecord sinkRecord = new SinkRecord("topic", 1, null, null, Schema.INT8_SCHEMA, 1, 1);
        Set<SinkRecord> sinkRecords = Collections.singleton(sinkRecord);
        server.sendRecords(sinkRecords);

        // Verify that the records were sent to all active providers.
        for (DataProviderWrapper wrapper : server.getActiveProviders()) {
            FakeDataProviderWrapper fakeWrapper = (FakeDataProviderWrapper) wrapper;
            assertThat(fakeWrapper.records).isSameInstanceAs(sinkRecords);
        }

        assertThat(server.closingException()).isEmpty();

        server.stop();
        assertThat(server.getActiveProviders()).isEmpty();
    }

    @Test
    void shouldManageClosureOfRemoteProxyAdapter() throws InterruptedException {
        ProviderServerOptions options =
                new ProviderServerOptions.Builder(6661).maxProxyAdapterConnections(2).build();

        FakeProviderConnection providerConnection = new FakeProviderConnection();

        ProviderServer server =
                new ProviderServer(options, Thread.currentThread(), opts -> providerConnection);

        // The following supplier simulates a remote DataProvider that fails to start
        // after the first connection. This simulates a scenario where the remote Proxy Adapter
        // closes the connection after the first provider has been accepted, simulating a failure
        // in the connection loop.
        AtomicInteger proxyAdapterConnections = new AtomicInteger(0);
        Supplier<DataProviderWrapper> providerSupplier =
                () -> {
                    if (proxyAdapterConnections.incrementAndGet() > 1) {
                        return new FakeDataProviderWrapper(
                                new IOException("Simulated connection failure"));
                    }

                    return new FakeDataProviderWrapper();
                };
        server.start(providerSupplier);

        // Simulate the connection loop accepting two connections.
        providerConnection.triggerAcceptConnections(2);

        // Just wait a bit to ensure the connection loop has started.
        TimeUnit.MILLISECONDS.sleep(800);

        assertThat(providerConnection.acceptInvoked.get()).isEqualTo(2);
        // The first provider should have started successfully, while the second one should have
        // failed.
        assertThat(server.getActiveProviders().size()).isEqualTo(1);
    }

    @Test
    void shouldCloseDueToServerConnectionIssue() throws InterruptedException {
        ProviderServerOptions options =
                new ProviderServerOptions.Builder(6661).maxProxyAdapterConnections(2).build();

        Thread syncConnectorThread = new Thread();
        ProviderServer server =
                new ProviderServer(
                        options, syncConnectorThread, opts -> new FakeProviderConnection(true));

        server.start((() -> new FakeDataProviderWrapper()));

        // Just wait a bit to ensure the connection loop has started.
        TimeUnit.MILLISECONDS.sleep(100);

        assertThat(syncConnectorThread.isInterrupted()).isTrue();
        assertThat(server.closingException()).isPresent();
        assertThat(server.closingException().get())
                .hasMessageThat()
                .isEqualTo("Simulated server connection issue at startup");
    }
}
