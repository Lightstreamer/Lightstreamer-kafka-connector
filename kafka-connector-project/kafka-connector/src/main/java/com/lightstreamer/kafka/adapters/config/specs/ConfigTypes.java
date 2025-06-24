
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

package com.lightstreamer.kafka.adapters.config.specs;

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

public interface ConfigTypes {

    public enum SecurityProtocol {
        PLAINTEXT,
        SASL_PLAINTEXT,
        SASL_SSL,
        SSL;

        public static Set<String> names() {
            return enumNames(values());
        }

        public static SecurityProtocol retrieve(boolean encrypted, boolean authenticated) {
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
        PLAIN,
        SCRAM_256 {
            @Override
            public String toString() {
                return "SCRAM-SHA-256";
            }
        },

        SCRAM_512 {
            @Override
            public String toString() {
                return "SCRAM-SHA-512";
            }
        },

        GSSAPI,

        AWS_MSK_IAM;

        static Map<String, SaslMechanism> NAME_CACHE = new HashMap<>();

        static {
            NAME_CACHE =
                    Stream.of(values())
                            .collect(
                                    Collectors.toMap(SaslMechanism::toString, Function.identity()));
        }

        public static SaslMechanism fromName(String name) {
            SaslMechanism mechanism = NAME_CACHE.get(name);
            if (mechanism == null) {
                throw new IllegalArgumentException(
                        "No SaslMechanism found with name [%s]".formatted(name));
            }
            return mechanism;
        }

        public static Set<String> names() {
            return Stream.of(values())
                    .map(SaslMechanism::toString)
                    .collect(Collectors.toUnmodifiableSet());
        }

        public String loginModule() {
            return switch (this) {
                case PLAIN -> "org.apache.kafka.common.security.plain.PlainLoginModule";
                case SCRAM_256, SCRAM_512 ->
                        "org.apache.kafka.common.security.scram.ScramLoginModule";
                case GSSAPI -> "com.sun.security.auth.module.Krb5LoginModule";
                case AWS_MSK_IAM -> "software.amazon.msk.auth.iam.IAMLoginModule";
            };
        }

        public String toString() {
            return switch (this) {
                case SCRAM_256 -> "SCRAM-SHA-256";
                case SCRAM_512 -> "SCRAM-SHA-512";
                default -> super.toString();
            };
        }
    }

    public enum RecordConsumeFrom {
        LATEST,
        EARLIEST;

        public String toPropertyValue() {
            return toString().toLowerCase();
        }

        public static Set<String> names() {
            return enumNames(values());
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
        PROTOBUF,
        KVP,
        STRING,
        INTEGER,
        BOOLEAN,
        BYTE_ARRAY,
        BYTE_BUFFER,
        BYTES,
        DOUBLE,
        FLOAT,
        LONG,
        SHORT,
        UUID;

        public boolean is(EvaluatorType type) {
            return this == type;
        }

        public static Set<String> names() {
            return enumNames(values());
        }

        public static EvaluatorType fromClass(Class<?> klass) {
            return switch (klass.getSimpleName()) {
                case "String" -> EvaluatorType.STRING;
                case "Integer" -> EvaluatorType.INTEGER;
                case "Boolean" -> EvaluatorType.BOOLEAN;
                case "byte[]" -> EvaluatorType.BYTE_ARRAY;
                case "ByteBuffer" -> EvaluatorType.BYTE_BUFFER;
                case "Bytes" -> EvaluatorType.BYTES;
                case "Double" -> EvaluatorType.DOUBLE;
                case "Float" -> EvaluatorType.FLOAT;
                case "SHORT" -> EvaluatorType.SHORT;
                case "UUID" -> EvaluatorType.UUID;
                default -> throw new IllegalArgumentException();
            };
        }
    }

    public enum RecordErrorHandlingStrategy {
        IGNORE_AND_CONTINUE,
        FORCE_UNSUBSCRIPTION;

        public static Set<String> names() {
            return enumNames(values());
        }
    }

    public enum RecordConsumeWithOrderStrategy {
        ORDER_BY_KEY,
        ORDER_BY_PARTITION,
        UNORDERED;

        public static Set<String> names() {
            return enumNames(values());
        }
    }

    enum CommandModeStrategy {
        NONE,
        ENFORCE,
        AUTO;

        public static CommandModeStrategy from(boolean auto, boolean enforce) {
            if (auto) {
                return AUTO;
            }
            if (enforce) {
                return ENFORCE;
            }
            return NONE;
        }

        public boolean manageSnapshot() {
            return this == ENFORCE;
        }
    }

    private static Set<String> enumNames(Enum<?>[] e) {
        return Arrays.stream(e).map(Enum::toString).collect(Collectors.toUnmodifiableSet());
    }
}
