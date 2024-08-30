
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

import static com.google.common.truth.Truth.assertThat;

import static org.junit.Assert.assertThrows;

import com.lightstreamer.adapters.remote.DataProvider;
import com.lightstreamer.adapters.remote.DataProviderException;
import com.lightstreamer.adapters.remote.FailureException;
import com.lightstreamer.adapters.remote.ItemEventListener;
import com.lightstreamer.adapters.remote.RemotingException;
import com.lightstreamer.adapters.remote.SubscriptionException;
import com.lightstreamer.kafka.connect.Fakes.FakeProxyConnection;
import com.lightstreamer.kafka.connect.Fakes.FakeRemoteDataProviderServer;
import com.lightstreamer.kafka.connect.proxy.ProxyAdapterClient.ProxyAdapterConnection;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class ProxyAdapterClientTest {

    static class FakeDataProvider implements DataProvider {

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
    }

    @Test
    void shouldStartWithCredentials() throws RemotingException, IOException {
        ProxyAdapterClientOptions options =
                new ProxyAdapterClientOptions.Builder("host", 6661)
                        .username("username")
                        .password("password")
                        .build();

        FakeProxyConnection proxyConnection = new FakeProxyConnection();
        Supplier<FakeProxyConnection> s = FakeProxyConnection::new;
        assertThat(proxyConnection.openInvoked).isFalse();

        FakeRemoteDataProviderServer dataProviderServer = new FakeRemoteDataProviderServer();
        assertThat(dataProviderServer.startInvoked).isFalse();

        // Necessary to satisfy the required arguments of the ProxyAdapterClient constructor.
        Function<ProxyAdapterClientOptions, ProxyAdapterConnection> proxyConnectionFactory =
                opts -> proxyConnection;
        ProxyAdapterClient client =
                new ProxyAdapterClient(
                        options,
                        Thread.currentThread(),
                        proxyConnectionFactory,
                        dataProviderServer);

        FakeDataProvider provider = new FakeDataProvider();
        client.start(provider);

        assertThat(proxyConnection.openInvoked).isTrue();
        assertThat(proxyConnection.retries).isEqualTo(0);
        assertThat(dataProviderServer.username).isEqualTo("username");
        assertThat(dataProviderServer.password).isEqualTo("password");
        assertThat(dataProviderServer.provider).isSameInstanceAs(provider);
        assertThat(dataProviderServer.startInvoked).isTrue();
        assertThat(dataProviderServer.ioStreams).isSameInstanceAs(proxyConnection.io);
        assertThat(client.closingException()).isEmpty();
    }

    static Stream<Arguments> configuredRetriesAbsorbeFailures() {
        return Stream.of(
                Arguments.of(0, 0), Arguments.of(1, 1), Arguments.of(2, 1), Arguments.of(5, 3));
    }

    @ParameterizedTest
    @MethodSource("configuredRetriesAbsorbeFailures")
    void shouldStartAfterFailures(int maxRetries, int scheduledFailures)
            throws RemotingException, IOException {
        ProxyAdapterClientOptions options =
                new ProxyAdapterClientOptions.Builder("host", 6661)
                        .username("username")
                        .password("password")
                        .connectionMaxRetries(maxRetries)
                        .build();

        FakeProxyConnection proxyConnection = new FakeProxyConnection(scheduledFailures);
        FakeRemoteDataProviderServer dataProviderServer = new FakeRemoteDataProviderServer();

        // Necessary to satisfy the required arguments of the ProxyAdapterClient constructor.
        Function<ProxyAdapterClientOptions, ProxyAdapterConnection> proxyConnectionFactory =
                opts -> proxyConnection;
        ProxyAdapterClient client =
                new ProxyAdapterClient(
                        options,
                        Thread.currentThread(),
                        proxyConnectionFactory,
                        dataProviderServer);

        FakeDataProvider provider = new FakeDataProvider();
        client.start(provider);

        assertThat(proxyConnection.openInvoked).isTrue();
        assertThat(proxyConnection.retries).isEqualTo(scheduledFailures);
        assertThat(dataProviderServer.username).isEqualTo("username");
        assertThat(dataProviderServer.password).isEqualTo("password");
        assertThat(dataProviderServer.provider).isSameInstanceAs(provider);
        assertThat(dataProviderServer.startInvoked).isTrue();
        assertThat(dataProviderServer.ioStreams).isSameInstanceAs(proxyConnection.io);
        assertThat(client.closingException()).isEmpty();
    }

    @Test
    void shouldNotStartWithNegativeMaxRetries() throws RemotingException, IOException {
        ProxyAdapterClientOptions options =
                new ProxyAdapterClientOptions.Builder("host", 6661)
                        .connectionMaxRetries(-1)
                        .build();

        FakeProxyConnection proxyConnection = new FakeProxyConnection();
        FakeRemoteDataProviderServer dataProviderServer = new FakeRemoteDataProviderServer();

        // Necessary to satisfy the required arguments of the ProxyAdapterClient constructor.
        Function<ProxyAdapterClientOptions, ProxyAdapterConnection> proxyConnectionFactory =
                opts -> proxyConnection;
        ProxyAdapterClient client =
                new ProxyAdapterClient(
                        options,
                        Thread.currentThread(),
                        proxyConnectionFactory,
                        dataProviderServer);

        FakeDataProvider provider = new FakeDataProvider();
        client.start(provider);

        assertThat(proxyConnection.openInvoked).isFalse();
        assertThat(proxyConnection.retries).isEqualTo(0);
        assertThat(dataProviderServer.provider).isNull();
        assertThat(dataProviderServer.startInvoked).isFalse();
        assertThat(dataProviderServer.ioStreams).isNull();
        assertThat(client.closingException()).isEmpty();
    }

    static Stream<Arguments> configuredRetriesDontAbsorbeFailures() {
        return Stream.of(Arguments.of(0, 1), Arguments.of(1, 2), Arguments.of(2, 6));
    }

    @ParameterizedTest
    @MethodSource("configuredRetriesDontAbsorbeFailures")
    void shouldNotStartAfterNoMoreRertries(int maxRetries, int scheduledFailures)
            throws RemotingException, IOException {
        ProxyAdapterClientOptions options =
                new ProxyAdapterClientOptions.Builder("host", 6661)
                        .username("username")
                        .password("password")
                        .connectionMaxRetries(maxRetries)
                        .build();

        FakeProxyConnection proxyConnection = new FakeProxyConnection(scheduledFailures);
        FakeRemoteDataProviderServer dataProviderServer = new FakeRemoteDataProviderServer();

        // Necessary to satisfy the required arguments of the ProxyAdapterClient constructor.
        Function<ProxyAdapterClientOptions, ProxyAdapterConnection> proxyConnectionFactory =
                opts -> proxyConnection;

        ProxyAdapterClient client =
                new ProxyAdapterClient(
                        options,
                        Thread.currentThread(),
                        proxyConnectionFactory,
                        dataProviderServer);

        FakeDataProvider provider = new FakeDataProvider();
        assertThrows(ConnectException.class, () -> client.start(provider));

        assertThat(proxyConnection.openInvoked).isFalse();
        assertThat(proxyConnection.retries).isEqualTo(maxRetries);
        assertThat(dataProviderServer.username).isNull();
        assertThat(dataProviderServer.password).isNull();
        assertThat(dataProviderServer.provider).isNull();
        assertThat(dataProviderServer.startInvoked).isFalse();
        assertThat(dataProviderServer.closedInvoked).isFalse();
        assertThat(dataProviderServer.ioStreams).isNull();
        assertThat(client.closingException()).isEmpty();
    }

    static Stream<Arguments> incompleteCredentials() {
        return Stream.of(
                Arguments.of("username", null),
                Arguments.of(null, "password"),
                Arguments.of(null, null));
    }

    @ParameterizedTest
    @MethodSource("incompleteCredentials")
    void shouldStartWithoutCredentials(String username, String password) throws RemotingException {
        ProxyAdapterClientOptions options =
                new ProxyAdapterClientOptions.Builder("host", 6661)
                        .username(username)
                        .username(password)
                        .build();

        FakeProxyConnection proxyConnection = new FakeProxyConnection();
        FakeRemoteDataProviderServer dataProviderServer = new FakeRemoteDataProviderServer();

        // Necessary to satisfy the required arguments of the ProxyAdapterClient constructor.
        Function<ProxyAdapterClientOptions, ProxyAdapterConnection> proxyConnectionFactory =
                opts -> proxyConnection;
        ProxyAdapterClient client =
                new ProxyAdapterClient(
                        options,
                        Thread.currentThread(),
                        proxyConnectionFactory,
                        dataProviderServer);

        FakeDataProvider provider = new FakeDataProvider();
        client.start(provider);

        assertThat(proxyConnection.openInvoked).isTrue();
        assertThat(dataProviderServer.username).isNull();
        assertThat(dataProviderServer.password).isNull();
        assertThat(dataProviderServer.provider).isSameInstanceAs(provider);
        assertThat(dataProviderServer.startInvoked).isTrue();
        assertThat(client.closingException()).isEmpty();
    }

    @Test
    void shoudStop() throws RemotingException {
        ProxyAdapterClientOptions options =
                new ProxyAdapterClientOptions.Builder("host", 6661).build();

        FakeProxyConnection proxyConnection = new FakeProxyConnection();
        FakeRemoteDataProviderServer dataProviderServer = new FakeRemoteDataProviderServer();

        // Necessary to satisfy the required arguments of the ProxyAdapterClient constructor.
        Function<ProxyAdapterClientOptions, ProxyAdapterConnection> proxyConnectionFactory =
                opts -> proxyConnection;
        ProxyAdapterClient client =
                new ProxyAdapterClient(
                        options,
                        Thread.currentThread(),
                        proxyConnectionFactory,
                        dataProviderServer);

        FakeDataProvider provider = new FakeDataProvider();
        client.start(provider);

        assertThat(proxyConnection.closedInvoked).isFalse();
        assertThat(dataProviderServer.closedInvoked).isFalse();

        client.stop();

        assertThat(proxyConnection.closedInvoked).isTrue();
        assertThat(dataProviderServer.closedInvoked).isTrue();
        assertThat(client.closingException()).isEmpty();
    }

    static Stream<Throwable> exceptions() {
        return Stream.of(
                new RemotingException("A fake RemotingException"),
                new IOException("A fake IOException"));
    }

    @ParameterizedTest
    @MethodSource("exceptions")
    void shouldHandleExceptions(Throwable fakeException) throws RemotingException {
        ProxyAdapterClientOptions options =
                new ProxyAdapterClientOptions.Builder("host", 6661).build();

        FakeProxyConnection proxyConnection = new FakeProxyConnection();
        RemoteDataProviderServer dataProviderServer =
                new FakeRemoteDataProviderServer(fakeException);

        // Necessary to satisfy the required arguments of the ProxyAdapterClient constructor.
        Function<ProxyAdapterClientOptions, ProxyAdapterConnection> proxyConnectionFactory =
                opts -> proxyConnection;
        AtomicBoolean interrupted = new AtomicBoolean();
        Supplier<Optional<Throwable>> connectTask =
                () -> {
                    ProxyAdapterClient client =
                            new ProxyAdapterClient(
                                    options,
                                    Thread.currentThread(),
                                    proxyConnectionFactory,
                                    dataProviderServer);

                    client.start(new FakeDataProvider());

                    try {
                        TimeUnit.MILLISECONDS.sleep(1000);
                    } catch (InterruptedException e) {
                        interrupted.set(true);
                    }
                    return client.closingException();
                };

        // Run the task asynchronously.
        Optional<Throwable> closingException = CompletableFuture.supplyAsync(connectTask).join();

        // The Sync task thread should have been interrupted.
        assertThat(interrupted.get()).isTrue();
        // The triggering closing exception should be the one expected.
        assertThat(closingException).hasValue(fakeException);
    }
}
