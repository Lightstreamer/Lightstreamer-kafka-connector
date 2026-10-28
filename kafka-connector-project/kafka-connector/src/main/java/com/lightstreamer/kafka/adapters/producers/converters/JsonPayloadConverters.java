
/*
 * Copyright (C) 2026 Lightstreamer Srl
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

package com.lightstreamer.kafka.adapters.producers.converters;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.UUIDSerializer;

import java.util.UUID;

class JsonPayloadConverters {

    private static InvalidMessageException unexpected(JsonNode json, String expected) {
        return new InvalidMessageException(
                "expected %s but found %s".formatted(expected, json.getNodeType()));
    }

    public static JsonPayloadConverter<String> String() {
        return new JsonPayloadConverter<String>() {
            @Override
            public String fromJson(JsonNode json) throws InvalidMessageException {
                if (!json.isTextual()) {
                    throw unexpected(json, "string");
                }
                return json.asText();
            }

            @Override
            public Serializer<String> serializer() {
                return org.apache.kafka.common.serialization.Serdes.String().serializer();
            }
        };
    }

    public static JsonPayloadConverter<Integer> Integer() {
        return new JsonPayloadConverter<Integer>() {
            @Override
            public Integer fromJson(JsonNode json) throws InvalidMessageException {
                if (!json.isIntegralNumber() || !json.canConvertToInt()) {
                    throw unexpected(json, "integer");
                }
                return json.asInt();
            }

            @Override
            public Serializer<Integer> serializer() {
                return org.apache.kafka.common.serialization.Serdes.Integer().serializer();
            }
        };
    }

    public static JsonPayloadConverter<Boolean> Boolean() {
        return new JsonPayloadConverter<Boolean>() {
            @Override
            public Boolean fromJson(JsonNode json) throws InvalidMessageException {
                if (!json.isBoolean()) {
                    throw unexpected(json, "boolean");
                }
                return json.asBoolean();
            }

            @Override
            public Serializer<Boolean> serializer() {
                return org.apache.kafka.common.serialization.Serdes.Boolean().serializer();
            }
        };
    }

    public static JsonPayloadConverter<Double> Double() {
        return new JsonPayloadConverter<Double>() {
            @Override
            public Double fromJson(JsonNode json) throws InvalidMessageException {
                if (!json.isNumber()) {
                    throw unexpected(json, "a number");
                }
                return json.asDouble();
            }

            @Override
            public Serializer<Double> serializer() {
                return org.apache.kafka.common.serialization.Serdes.Double().serializer();
            }
        };
    }

    public static JsonPayloadConverter<Float> Float() {
        return new JsonPayloadConverter<Float>() {
            @Override
            public Float fromJson(JsonNode json) throws InvalidMessageException {
                if (!json.isNumber()) {
                    throw unexpected(json, "a number");
                }
                return json.floatValue();
            }

            @Override
            public Serializer<Float> serializer() {
                return org.apache.kafka.common.serialization.Serdes.Float().serializer();
            }
        };
    }

    public static JsonPayloadConverter<Long> Long() {
        return new JsonPayloadConverter<Long>() {
            @Override
            public Long fromJson(JsonNode json) throws InvalidMessageException {
                if (json.isIntegralNumber() && json.canConvertToLong()) {
                    return json.asLong();
                }
                if (json.isTextual()) {
                    try {
                        return Long.parseLong(json.asText());
                    } catch (NumberFormatException e) {
                        throw unexpected(json, "long");
                    }
                }
                throw unexpected(json, "a long (number or numeric string)");
            }

            @Override
            public Serializer<Long> serializer() {
                return org.apache.kafka.common.serialization.Serdes.Long().serializer();
            }
        };
    }

    public static JsonPayloadConverter<Short> Short() {
        return new JsonPayloadConverter<Short>() {
            @Override
            public Short fromJson(JsonNode json) throws InvalidMessageException {
                if (!json.isIntegralNumber()
                        || !json.canConvertToInt()
                        || json.intValue() < Short.MIN_VALUE
                        || json.intValue() > Short.MAX_VALUE) {
                    throw unexpected(json, "short");
                }
                return json.shortValue();
            }

            @Override
            public Serializer<Short> serializer() {
                return org.apache.kafka.common.serialization.Serdes.Short().serializer();
            }
        };
    }

    public static JsonPayloadConverter<UUID> UUID() {
        return new JsonPayloadConverter<UUID>() {
            @Override
            public UUID fromJson(JsonNode json) throws InvalidMessageException {
                if (!json.isTextual()) {
                    throw unexpected(json, "a UUID string");
                }
                String text = json.asText();
                try {
                    return UUID.fromString(text);
                } catch (IllegalArgumentException e) {
                    throw new InvalidMessageException(
                            "expected a valid UUID but found \"%s\"".formatted(json.textValue()));
                }
            }

            @Override
            public Serializer<UUID> serializer() {
                return new UUIDSerializer();
            }
        };
    }
}
