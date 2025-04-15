
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

package com.lightstreamer.kafka.examples.quick_start.producer.test_utils;

import java.util.HashMap;
import java.util.Map;

public class StockEvents {

    public static Map<String, String> createEvent() {
        Map<String, String> stockEvent = new HashMap<>();
        stockEvent.put("name", "AAPL");
        stockEvent.put("time", "12:34:56");
        stockEvent.put("timestamp", "2023-10-01T12:34:56Z");
        stockEvent.put("last_price", "150.5");
        stockEvent.put("ask", "150.50");
        stockEvent.put("bid", "149.75");
        stockEvent.put("ask_quantity", "50");
        stockEvent.put("bid_quantity", "100");
        stockEvent.put("pct_change", "0.5");
        stockEvent.put("min", "149.00");
        stockEvent.put("max", "151.00");
        stockEvent.put("ref_price", "150.00");
        stockEvent.put("open_price", "149.50");
        stockEvent.put("item_status", "active");
        return stockEvent;
    }
}
