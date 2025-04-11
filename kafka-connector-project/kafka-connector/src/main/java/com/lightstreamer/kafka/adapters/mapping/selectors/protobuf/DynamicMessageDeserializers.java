
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

package com.lightstreamer.kafka.adapters.mapping.selectors.protobuf;

import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.AVRO;

import com.google.protobuf.DynamicMessage;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;

public class DynamicMessageDeserializers {

    private static Deserializer<DynamicMessage> configuredDeserializer(
            ConnectorConfig config, boolean isKey) {
        checkEvaluator(config, isKey);
        Deserializer<DynamicMessage> deserializer = newDeserializer(config, isKey);
        deserializer.configure(Utils.propsToMap(config.baseConsumerProps()), isKey);
        return deserializer;
    }

    private static Deserializer<DynamicMessage> newDeserializer(
            ConnectorConfig config, boolean isKey) {
        return new KafkaProtobufDeserializer<>();
    }

    private static void checkEvaluator(ConnectorConfig config, boolean isKey) {
        if (isProtobufKeyEvaluator(config, isKey)) {
            return;
        }
        if (isProtobufValueEvaluator(config, isKey)) {
            return;
        }
    }

    private static boolean isProtobufKeyEvaluator(ConnectorConfig config, boolean isKey) {
        if (!isKey) {
            return false;
        }
        return config.getKeyEvaluator().is(AVRO);
    }

    private static boolean isProtobufValueEvaluator(ConnectorConfig config, boolean isKey) {
        if (isKey) {
            return false;
        }
        return config.getValueEvaluator().is(AVRO);
    }

    static Deserializer<DynamicMessage> ValueDeserializer(ConnectorConfig config) {
        return configuredDeserializer(config, false);
    }

    static Deserializer<DynamicMessage> KeyDeserializer(ConnectorConfig config) {
        return configuredDeserializer(config, true);
    }
}
