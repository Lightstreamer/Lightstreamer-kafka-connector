
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConfigTypes {

    public enum SecurityProtocol {
        PLAINTEXT,
        SASL_PLAINTEXT,
        SASL_SSL,
        SSL;

        public static Set<String> names() {
            return enumNames(values());
        }

        static SecurityProtocol retrieve(boolean encrypted, boolean authenticated) {
            SecurityProtocol channel = encrypted ? SSL : PLAINTEXT;
            String protocol =
                    authenticated
                            ? String.join("_", "SASL", channel.toString())
                            : channel.toString();
            return SecurityProtocol.valueOf(protocol);
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

        static Map<String, SslProtocol> NAME_CACHE = new HashMap<>();

        static {
            NAME_CACHE =
                    Stream.of(values())
                            .collect(Collectors.toMap(SslProtocol::toString, Function.identity()));
        }

        public static SslProtocol fromName(String name) {
            SslProtocol protocol = NAME_CACHE.get(name);
            if (protocol == null) {
                throw new IllegalArgumentException(
                        "No SslProtocol found with name [%s]".formatted(name));
            }
            return protocol;
        }

        public static String toValuesStr() {
            return names().stream().sorted().collect(Collectors.joining(","));
        }

        public static List<SslProtocol> fromValueStr(String text) {
            String[] elements = text.split(",");
            if (elements.length == 1 && elements[0].isBlank()) {
                return Collections.emptyList();
            }
            return Stream.of(elements)
                    .map(element -> NAME_CACHE.get(element))
                    .filter(Objects::nonNull)
                    .toList();
        }

        public static Set<String> names() {
            return enumNames(values());
        }
    }

    public enum SaslMechanism {
        PLAIN {
            @Override
            public String loginModule() {
                return "org.apache.kafka.common.security.plain.PlainLoginModule";
            }
        },
        SCRAM_256 {
            @Override
            public String loginModule() {
                return "org.apache.kafka.common.security.scram.ScramLoginModule";
            }
        },

        SCRAM_512 {
            @Override
            public String loginModule() {
                return "org.apache.kafka.common.security.scram.ScramLoginModule";
            }
        },

        GSSAPI {
            @Override
            public String loginModule() {
                return "com.sun.security.auth.module.Krb5LoginModule";
            }
        };

        public static Set<String> names() {
            return enumNames(values());
        }

        public String loginModule() {
            return "";
        }
    }

    public enum KeystoreType {
        JKS,
        PKCS12;

        public static Set<String> names() {
            return enumNames(values());
        }
    }

    public enum EvaluatorType {
        AVRO,
        JSON,
        STRING;

        public static Set<String> names() {
            return enumNames(values());
        }
    }

    public enum RecordErrorHandlingStrategy {
        IGNORE_AND_CONTINUE,
        FORCE_UNSUBSCRIPTION;

        public static Set<String> names() {
            return enumNames(values());
        }
    }

    private static Set<String> enumNames(Enum<?>[] e) {
        return Arrays.stream(e).map(Enum::toString).collect(Collectors.toUnmodifiableSet());
    }
}
