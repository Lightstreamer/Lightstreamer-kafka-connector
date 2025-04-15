
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

package com.lightstreamer.kafka.examples.quick_start.producer.json;

import static com.google.common.truth.Truth.assertThat;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.lightstreamer.kafka.examples.quick_start.producer.test_utils.StockEvents;

import org.junit.jupiter.api.Test;

import java.util.Map;

public class JsonStockTest {

    @Test
    public void shouldCreateProtobufStockFromEvent() {
        // Create test stock event
        Map<String, String> stockEvent = StockEvents.createEvent();

        Stock stock = JsonStock.fromEvent(stockEvent);
        assertNotNull(stock);
        assertThat(stock.name()).isEqualTo("AAPL");
        assertThat(stock.time()).isEqualTo("12:34:56");
        assertThat(stock.timestamp()).isEqualTo("2023-10-01T12:34:56Z");
        assertThat(stock.ask()).isEqualTo("150.50");
        assertThat(stock.bid()).isEqualTo("149.75");
        assertThat(stock.ask_quantity()).isEqualTo("50");
        assertThat(stock.bid_quantity()).isEqualTo("100");
        assertThat(stock.pct_change()).isEqualTo("0.5");
        assertThat(stock.min()).isEqualTo("149.00");
        assertThat(stock.max()).isEqualTo("151.00");
        assertThat(stock.ref_price()).isEqualTo("150.00");
        assertThat(stock.open_price()).isEqualTo("149.50");
        assertThat(stock.item_status()).isEqualTo("active");
    }
}
