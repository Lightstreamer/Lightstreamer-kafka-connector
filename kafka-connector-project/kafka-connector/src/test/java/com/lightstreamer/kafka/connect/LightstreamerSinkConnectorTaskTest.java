
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

import com.lightstreamer.kafka.connect.Fakes.FakeProxyConnection;
import com.lightstreamer.kafka.connect.Fakes.FakeRecordSender;
import com.lightstreamer.kafka.connect.Fakes.FakeRemoteDataProviderServer;
import com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig;
import com.lightstreamer.kafka.test_utils.VersionUtils;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class LightstreamerSinkConnectorTaskTest {

    static Map<String, String> basicConfig() {
        Map<String, String> config = new HashMap<>();
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

    @Test
    void shouldStartAndPutRecords() {
        FakeProxyConnection connection = new FakeProxyConnection();
        FakeRecordSender sender = new FakeRecordSender();
        LightstreamerSinkConnectorTask task =
                new LightstreamerSinkConnectorTask(
                        opts -> connection,
                        (config, context) -> sender,
                        new FakeRemoteDataProviderServer());
        Map<String, String> configs = basicConfig();
        task.start(configs);

        assertThat(connection.openInvoked).isTrue();
        assertThat(connection.closedInvoked).isFalse();

        SinkRecord sinkRecord = new SinkRecord("topic", 1, null, null, Schema.INT8_SCHEMA, 1, 1);
        Set<SinkRecord> sinkRecords = Collections.singleton(sinkRecord);
        task.put(sinkRecords);

        assertThat(sender.records).isSameInstanceAs(sinkRecords);
    }
}
