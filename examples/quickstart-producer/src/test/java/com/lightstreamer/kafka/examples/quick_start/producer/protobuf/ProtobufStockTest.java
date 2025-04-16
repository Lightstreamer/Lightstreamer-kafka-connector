
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

package com.lightstreamer.kafka.examples.quick_start.producer.protobuf;

import static com.google.common.truth.Truth.assertThat;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.lightstreamer.kafka.examples.quick_start.producer.test_utils.StockEvents;

import org.junit.jupiter.api.Test;

import java.util.Map;

public class ProtobufStockTest {

    @Test
    public void shouldCreateProtobufStockFromEvent() {
        // Create test stock event
        Map<String, String> stockEvent = StockEvents.createEvent();

        Stock stock = ProtobufStock.fromEvent(stockEvent);
        assertNotNull(stock);
        assertThat(stock.getName()).isEqualTo("AAPL");
        assertThat(stock.getTime()).isEqualTo("12:34:56");
        assertThat(stock.getTimestamp()).isEqualTo("2023-10-01T12:34:56Z");
        assertThat(stock.getAsk()).isEqualTo("150.50");
        assertThat(stock.getBid()).isEqualTo("149.75");
        assertThat(stock.getAskQuantity()).isEqualTo("50");
        assertThat(stock.getBidQuantity()).isEqualTo("100");
        assertThat(stock.getPctChange()).isEqualTo("0.5");
        assertThat(stock.getMin()).isEqualTo("149.00");
        assertThat(stock.getMax()).isEqualTo("151.00");
        assertThat(stock.getRefPrice()).isEqualTo("150.00");
        assertThat(stock.getOpenPrice()).isEqualTo("149.50");
        assertThat(stock.getItemStatus()).isEqualTo("active");
    }
}
