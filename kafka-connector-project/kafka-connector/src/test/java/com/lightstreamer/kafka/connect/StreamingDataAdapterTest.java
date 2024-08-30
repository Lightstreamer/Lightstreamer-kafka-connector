
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
import static org.junit.Assert.assertThrows;

import com.lightstreamer.adapters.remote.FailureException;
import com.lightstreamer.adapters.remote.SubscriptionException;
import com.lightstreamer.kafka.common.mapping.Items.Item;
import com.lightstreamer.kafka.connect.Fakes.FakeItemEventListener;
import com.lightstreamer.kafka.connect.Fakes.FakeSinkContext;
import com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class StreamingDataAdapterTest {

    static Map<String, String> basicConfig() {
        Map<String, String> config = new HashMap<>();
        config.put(LightstreamerConnectorConfig.LIGHTSTREAMER_PROXY_ADAPTER_ADDRESS, "host:6661");
        config.put(LightstreamerConnectorConfig.TOPIC_MAPPINGS, "topic:item1,item2");
        config.put(LightstreamerConnectorConfig.RECORD_MAPPING, "field1:#{VALUE}");
        return config;
    }

    static LightstreamerConnectorConfig basicConnectorConfig() {
        return new LightstreamerConnectorConfig(basicConfig());
    }

    static StreamingDataAdapter newAdapter() {
        return new StreamingDataAdapter(
                DataAdapterConfigurator.configure(basicConnectorConfig()), null);
    }

    @Test
    void shouldCreateWithoutContext() throws SubscriptionException {
        StreamingDataAdapter adapter = newAdapter();
        assertThat(adapter.reporter).isNull();
        assertThat(adapter.isSnapshotAvailable("anItem")).isFalse();
    }

    @Test
    void shouldCreateWithContextNoErrantReporter() {
        StreamingDataAdapter adapter =
                new StreamingDataAdapter(
                        DataAdapterConfigurator.configure(basicConnectorConfig()),
                        new FakeSinkContext());
        assertThat(adapter.reporter).isNull();
    }

    @Test
    void shouldCreateWithContext() {
        StreamingDataAdapter adapter =
                new StreamingDataAdapter(
                        DataAdapterConfigurator.configure(basicConnectorConfig()),
                        new FakeSinkContext(true));
        assertThat(adapter.reporter).isNotNull();
    }

    @Test
    void shouldSetItemEventListener() {
        StreamingDataAdapter adapter = newAdapter();
        FakeItemEventListener eventListener = new FakeItemEventListener();
        adapter.setListener(eventListener);
        assertThat(adapter.listener).isSameInstanceAs(eventListener);
    }

    @Test
    void shouldSubscribeAndUnsubscribe() throws SubscriptionException, FailureException {
        StreamingDataAdapter adapter = newAdapter();

        adapter.subscribe("item1");
        Item subscribedItem = adapter.getSubscribedItem("item1");
        assertThat(subscribedItem.schema().name()).isEqualTo("item1");
        assertThat(adapter.getCurrentItemsCount()).isEqualTo(1);

        adapter.subscribe("item2");
        Item subscribedItem2 = adapter.getSubscribedItem("item2");
        assertThat(subscribedItem2.schema().name()).isEqualTo("item2");
        assertThat(adapter.getCurrentItemsCount()).isEqualTo(2);

        adapter.unsubscribe("item1");
        assertThat(adapter.getSubscribedItem("item1")).isNull();
        assertThat(adapter.getCurrentItemsCount()).isEqualTo(1);

        adapter.unsubscribe("item2");
        assertThat(adapter.getSubscribedItem("item2")).isNull();
        assertThat(adapter.getCurrentItemsCount()).isEqualTo(0);
    }

    @Test
    public void shouldNotSubscribeToNotAllowedItems() {
        StreamingDataAdapter adapter = newAdapter();
        SubscriptionException se1 = assertThrows(SubscriptionException.class,() ->adapter.subscribe("anItem"));
        assertThat(se1.getMessage()).isEqualTo("Item does not match any defined item templates");

        SubscriptionException se2 = assertThrows(SubscriptionException.class,() ->adapter.subscribe("@"));
        assertThat(se2.getMessage()).isEqualTo("Invalid Item");
    }

    @Test
    public void shouldNotUnsubscribeFromNotExistingItem() {
        StreamingDataAdapter adapter = newAdapter();
        
        assertThrows(SubscriptionException.class,() ->adapter.unsubscribe("item"));
    }
}
