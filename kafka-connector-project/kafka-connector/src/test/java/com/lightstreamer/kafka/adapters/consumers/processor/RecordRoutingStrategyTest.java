
/*
 * Copyright (C) 2025 Lightstreamer Srl
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

package com.lightstreamer.kafka.adapters.consumers.processor;

import static com.google.common.truth.Truth.assertThat;

import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.DefaultRoutingStrategy;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.RecordRoutingStrategy;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.RouteAllStrategy;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.test_utils.Mocks;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Set;

public class RecordRoutingStrategyTest {

    private static final SubscribedItem item1 = Items.subscribedFrom("item1");
    private static final SubscribedItem item2 = Items.subscribedFrom("item2");
    private static final SubscribedItem item3 = Items.subscribedFrom("item3");

    private Set<SubscribedItem> routable = Set.of(item1);
    private Set<SubscribedItem> allRoutable = Set.of(item2, item3);

    private Mocks.MockMappedRecord record = new Mocks.MockMappedRecord(routable, allRoutable);

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldCreateRecordRoutingStrategy(boolean allowImplicitItems) {
        RecordRoutingStrategy strategy =
                RecordRoutingStrategy.fromSubscribedItems(
                        allowImplicitItems ? SubscribedItems.nop() : SubscribedItems.create());

        if (allowImplicitItems) {
            assertThat(strategy).isInstanceOf(RouteAllStrategy.class);
        } else {
            assertThat(strategy).isInstanceOf(DefaultRoutingStrategy.class);
        }
    }

    @Test
    public void shouldDefaultStrategyRouteToSubscribedItems() {
        assertThat(record.routeInvoked()).isFalse();
        assertThat(record.routeAllInvoked()).isFalse();

        Set<SubscribedItem> routed = defaultStrategy().route(record);

        assertThat(record.routeInvoked()).isTrue();
        assertThat(record.routeAllInvoked()).isFalse();
        assertThat(routed).containsExactly(item1);
    }

    @Test
    public void shouldDefaultStrategyNotRouteToImplicitItems() {
        assertThat(defaultStrategy().canRouteImplicitItems()).isFalse();
    }

    @Test
    public void shouldRouteAllStrategyRouteToAllItems() {
        assertThat(record.routeInvoked()).isFalse();
        assertThat(record.routeAllInvoked()).isFalse();

        Set<SubscribedItem> routed = routeAllStrategy().route(record);

        assertThat(record.routeInvoked()).isFalse();
        assertThat(record.routeAllInvoked()).isTrue();
        assertThat(routed).containsExactly(item2, item3);
    }

    @Test
    public void shouldRouteAllStrategyRouteToImplicitItems() {
        assertThat(routeAllStrategy().canRouteImplicitItems()).isTrue();
    }

    private static RecordRoutingStrategy defaultStrategy() {
        SubscribedItems subscribed = SubscribedItems.create();
        subscribed.addItem("item", Items.subscribedFrom("item", new Object()));
        return new DefaultRoutingStrategy(subscribed);
    }

    private static RecordRoutingStrategy routeAllStrategy() {
        return new RouteAllStrategy();
    }
}
