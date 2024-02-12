
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

public class ConfigTypes {

    public enum SecurityProtocol {
        SASL_PLAINTEXT,
        SASL_SSL,
        SSL;

        public static Set<String> names() {
            return ConfigTypes.names(values());
        }
    }

    public enum SslProtocol {
        TLSv12 {
            public String toString() {
                return "TLSv1.2";
            }
        },
        TLSv13 {
            public String toString() {
                return "TLSv1.3";
            }
        };

        public static String valueStr() {
            return names().stream().collect(Collectors.joining(","));
        }

        public static Set<String> names() {
            return ConfigTypes.names(values());
        }
    }

    public enum KeystoreType {
        JKS,
        PKCS12;

        public static Set<String> names() {
            return ConfigTypes.names(values());
        }
    }

    public enum EvaluatorType {
        AVRO,
        JSON,
        STRING;

        public static Set<String> names() {
            return ConfigTypes.names(values());
        }
    }

    public enum RecordErrorHandlingStrategy {
        IGNORE_AND_CONTINUE,
        FORCE_UNSUBSCRIPTION;

        public static Set<String> names() {
            return ConfigTypes.names(values());
        }
    }

    private static Set<String> names(Enum<?>[] e) {
        return Arrays.stream(e).map(Enum::toString).collect(Collectors.toUnmodifiableSet());
    }
}
