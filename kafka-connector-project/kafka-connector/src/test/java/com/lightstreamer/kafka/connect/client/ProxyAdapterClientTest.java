
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

package com.lightstreamer.kafka.connect.client;

import static com.google.common.truth.Truth.assertThat;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.adapters.remote.RemotingException;
import com.lightstreamer.kafka.connect.Fakes.FakeDataProviderWrapper;
import com.lightstreamer.kafka.connect.Fakes.FakeProxyConnection;
import com.lightstreamer.kafka.connect.common.DataProviderWrapper;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class ProxyAdapterClientTest {

    @Test
    void shouldStartWithCredentials() throws RemotingException, IOException {
        ProxyAdapterClientOptions options =
                new ProxyAdapterClientOptions.Builder("host", 6661)
                        .username("username")
                        .password("password")
                        .build();

        FakeProxyConnection proxyConnection = new FakeProxyConnection();
        assertThat(proxyConnection.openInvoked).isFalse();

        FakeDataProviderWrapper wrapper = new FakeDataProviderWrapper();
        assertThat(wrapper.startInvoked).isFalse();

        ProxyAdapterClient client =
                new ProxyAdapterClient(options, Thread.currentThread(), opts -> proxyConnection);
        client.start((() -> wrapper));

        assertThat(proxyConnection.openInvoked).isTrue();
        assertThat(proxyConnection.retries).isEqualTo(0);
        assertThat(wrapper.username).isEqualTo("username");
        assertThat(wrapper.password).isEqualTo("password");
        assertThat(wrapper.startInvoked).isTrue();
        assertThat(wrapper.ioStreams).isSameInstanceAs(proxyConnection.io);
        assertThat(client.closingException()).isEmpty();
    }

    static Stream<Arguments> configuredRetriesAbsorbFailures() {
        return Stream.of(
                Arguments.of(0, 0), Arguments.of(1, 1), Arguments.of(2, 1), Arguments.of(5, 3));
    }

    @ParameterizedTest
    @MethodSource("configuredRetriesAbsorbFailures")
    void shouldStartAfterFailures(int maxRetries, int scheduledFailures)
            throws RemotingException, IOException {
        ProxyAdapterClientOptions options =
                new ProxyAdapterClientOptions.Builder("host", 6661)
                        .username("username")
                        .password("password")
                        .connectionMaxRetries(maxRetries)
                        .build();

        FakeProxyConnection proxyConnection = new FakeProxyConnection(scheduledFailures);
        FakeDataProviderWrapper wrapper = new FakeDataProviderWrapper();

        ProxyAdapterClient client =
                new ProxyAdapterClient(options, Thread.currentThread(), opts -> proxyConnection);
        client.start((() -> wrapper));

        assertThat(proxyConnection.openInvoked).isTrue();
        assertThat(proxyConnection.retries).isEqualTo(scheduledFailures);
        assertThat(wrapper.username).isEqualTo("username");
        assertThat(wrapper.password).isEqualTo("password");
        assertThat(wrapper.startInvoked).isTrue();
        assertThat(wrapper.ioStreams).isSameInstanceAs(proxyConnection.io);
        assertThat(client.closingException()).isEmpty();
    }

    @Test
    void shouldNotStartWithNegativeMaxRetries() throws RemotingException, IOException {
        ProxyAdapterClientOptions options =
                new ProxyAdapterClientOptions.Builder("host", 6661)
                        .connectionMaxRetries(-1)
                        .build();

        FakeProxyConnection proxyConnection = new FakeProxyConnection();
        FakeDataProviderWrapper wrapper = new FakeDataProviderWrapper();

        ProxyAdapterClient client =
                new ProxyAdapterClient(options, Thread.currentThread(), opts -> proxyConnection);
        client.start((() -> wrapper));

        assertThat(proxyConnection.openInvoked).isFalse();
        assertThat(proxyConnection.retries).isEqualTo(0);
        assertThat(wrapper.startInvoked).isFalse();
        assertThat(wrapper.ioStreams).isNull();
        assertThat(client.closingException()).isEmpty();
    }

    static Stream<Arguments> configuredRetriesNotAbsorbFailures() {
        return Stream.of(Arguments.of(0, 1), Arguments.of(1, 2), Arguments.of(2, 6));
    }

    @ParameterizedTest
    @MethodSource("configuredRetriesNotAbsorbFailures")
    void shouldNotStartAfterNoMoreRetries(int maxRetries, int scheduledFailures)
            throws RemotingException, IOException {
        ProxyAdapterClientOptions options =
                new ProxyAdapterClientOptions.Builder("host", 6661)
                        .username("username")
                        .password("password")
                        .connectionMaxRetries(maxRetries)
                        .build();

        FakeProxyConnection proxyConnection = new FakeProxyConnection(scheduledFailures);
        FakeDataProviderWrapper wrapper = new FakeDataProviderWrapper();

        ProxyAdapterClient client =
                new ProxyAdapterClient(options, Thread.currentThread(), opts -> proxyConnection);

        assertThrows(ConnectException.class, () -> client.start(() -> wrapper));

        assertThat(proxyConnection.openInvoked).isFalse();
        assertThat(proxyConnection.retries).isEqualTo(maxRetries);
        assertThat(wrapper.username).isNull();
        assertThat(wrapper.password).isNull();
        assertThat(wrapper.startInvoked).isFalse();
        assertThat(wrapper.closedInvoked).isFalse();
        assertThat(wrapper.ioStreams).isNull();
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
        FakeDataProviderWrapper wrapper = new FakeDataProviderWrapper();

        ProxyAdapterClient client =
                new ProxyAdapterClient(options, Thread.currentThread(), opts -> proxyConnection);
        client.start(() -> wrapper);

        assertThat(proxyConnection.openInvoked).isTrue();
        assertThat(wrapper.username).isNull();
        assertThat(wrapper.password).isNull();
        assertThat(wrapper.startInvoked).isTrue();
        assertThat(client.closingException()).isEmpty();

        // Simulate sending records to the Proxy Adapter.
        SinkRecord sinkRecord = new SinkRecord("topic", 1, null, null, Schema.INT8_SCHEMA, 1, 1);
        Set<SinkRecord> sinkRecords = Collections.singleton(sinkRecord);
        client.sendRecords(sinkRecords);
        assertThat(wrapper.records).isSameInstanceAs(sinkRecords);
    }

    @Test
    void shouldStop() throws RemotingException {
        ProxyAdapterClientOptions options =
                new ProxyAdapterClientOptions.Builder("host", 6661).build();

        FakeProxyConnection proxyConnection = new FakeProxyConnection();
        FakeDataProviderWrapper wrapper = new FakeDataProviderWrapper();

        ProxyAdapterClient client =
                new ProxyAdapterClient(options, Thread.currentThread(), opts -> proxyConnection);
        client.start(() -> wrapper);

        assertThat(proxyConnection.closedInvoked).isFalse();
        assertThat(wrapper.closedInvoked).isFalse();

        client.stop();

        assertThat(proxyConnection.closedInvoked).isTrue();
        assertThat(wrapper.closedInvoked).isTrue();
        assertThat(client.closingException()).isEmpty();
    }

    static Stream<Throwable> exceptions() {
        return Stream.of(
                new RemotingException("A fake RemotingException"),
                new IOException("A fake IOException"));
    }

    @ParameterizedTest
    @MethodSource("exceptions")
    void shouldHandleExceptions(Throwable fakeException)
            throws RemotingException, InterruptedException {
        ProxyAdapterClientOptions options =
                new ProxyAdapterClientOptions.Builder("host", 6661).build();

        FakeProxyConnection proxyConnection = new FakeProxyConnection();
        DataProviderWrapper wrapper = new FakeDataProviderWrapper(fakeException);

        Thread syncConnectorThread = new Thread();
        ProxyAdapterClient client =
                new ProxyAdapterClient(options, syncConnectorThread, opts -> proxyConnection);
        client.start(() -> wrapper);

        // Just wait a bit to ensure the delayed exception has been processed.
        TimeUnit.MILLISECONDS.sleep(800);

        // The Sync task thread should have been interrupted.
        assertThat(syncConnectorThread.isInterrupted()).isTrue();

        // The triggering closing exception should be the one expected.
        assertThat(client.closingException()).isPresent();
        assertThat(client.closingException()).hasValue(fakeException);
    }
}
