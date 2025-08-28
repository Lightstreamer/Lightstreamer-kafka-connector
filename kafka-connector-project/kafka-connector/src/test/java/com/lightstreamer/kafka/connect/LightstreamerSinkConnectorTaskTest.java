
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

import static com.google.common.truth.Truth.assertThat;

import com.lightstreamer.kafka.connect.Fakes.FakeDataProviderWrapper;
import com.lightstreamer.kafka.connect.Fakes.FakeProviderConnection;
import com.lightstreamer.kafka.connect.Fakes.FakeProxyConnection;
import com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig;
import com.lightstreamer.kafka.test_utils.VersionUtils;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class LightstreamerSinkConnectorTaskTest {

    static Map<String, String> basicConfig(boolean enableConnectionInversion) {
        Map<String, String> config = new HashMap<>();
        config.put(
                LightstreamerConnectorConfig.CONNECTION_INVERSION_ENABLE,
                Boolean.toString(enableConnectionInversion));
        config.put(LightstreamerConnectorConfig.LIGHTSTREAMER_PROXY_ADAPTER_ADDRESS, "host:6661");
        config.put(LightstreamerConnectorConfig.TOPIC_MAPPINGS, "topic:item1");
        config.put(LightstreamerConnectorConfig.RECORD_MAPPINGS, "field1:#{VALUE}");
        return config;
    }

    LightstreamerSinkConnectorTask createTask() {
        return new LightstreamerSinkConnectorTask();
    }

    @Test
    void shouldGetVersion() {
        LightstreamerSinkConnectorTask connector = createTask();
        assertThat(connector.version()).isEqualTo(VersionUtils.currentVersion());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldStartAndPutRecords(boolean enableConnectionInversion) throws InterruptedException {
        FakeProxyConnection clientConnection = new FakeProxyConnection();
        FakeProviderConnection serverConnection = new FakeProviderConnection();
        FakeDataProviderWrapper wrapper = new FakeDataProviderWrapper();
        LightstreamerSinkConnectorTask task =
                new LightstreamerSinkConnectorTask(
                        opts -> clientConnection, opts -> serverConnection, config -> wrapper);
        task.start(basicConfig(enableConnectionInversion));

        if (enableConnectionInversion) {
            // Simulate the connection loop accepting a connection.
            serverConnection.triggerAcceptConnections(1);
            TimeUnit.MILLISECONDS.sleep(100);
            assertThat(serverConnection.acceptInvoked.get()).isEqualTo(1);
            assertThat(clientConnection.openInvoked).isFalse();

        } else {
            assertThat(clientConnection.openInvoked).isTrue();
            assertThat(clientConnection.closedInvoked).isFalse();
            assertThat(serverConnection.acceptInvoked.get()).isEqualTo(0);
        }

        SinkRecord sinkRecord = new SinkRecord("topic", 1, null, null, Schema.INT8_SCHEMA, 1, 1);
        Set<SinkRecord> sinkRecords = Collections.singleton(sinkRecord);
        task.put(sinkRecords);

        assertThat(wrapper.records).isSameInstanceAs(sinkRecords);
    }
}
