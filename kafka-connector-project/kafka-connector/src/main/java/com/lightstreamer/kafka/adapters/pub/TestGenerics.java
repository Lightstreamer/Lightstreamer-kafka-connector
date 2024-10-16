
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

package com.lightstreamer.kafka.adapters.pub;

import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;

import java.util.HashMap;
import java.util.Map;

public class TestGenerics {
    static <K, V> void foo(Map<? extends K, ? extends V> map1, Map<? extends K, ? extends V> map2) {
        // Implementazione
    }

    public static void main(String[] args) throws ExtractionException {
        Map<?, ?> map = new HashMap<String, String>();
        foo(map, map);
    }
}
