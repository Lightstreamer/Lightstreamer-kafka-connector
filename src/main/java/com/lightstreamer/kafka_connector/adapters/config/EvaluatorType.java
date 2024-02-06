
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

package com.lightstreamer.kafka_connector.adapters.config;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public enum EvaluatorType {
    AVRO,
    JSON,
    STRING;

    private static Set<String> NAMES;

    static {
        NAMES =
                Arrays.stream(EvaluatorType.values())
                        .map(EvaluatorType::toString)
                        .collect(Collectors.toUnmodifiableSet());
    }

    public static Set<String> names() {
        return NAMES;
    }
}
