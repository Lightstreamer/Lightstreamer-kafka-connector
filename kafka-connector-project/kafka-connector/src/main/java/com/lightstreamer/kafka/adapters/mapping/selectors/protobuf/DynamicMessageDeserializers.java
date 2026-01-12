
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

import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.PROTOBUF;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.mapping.selectors.AbstractLocalSchemaDeserializer;

import io.confluent.kafka.schemaregistry.protobuf.dynamic.DynamicSchema;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

public class DynamicMessageDeserializers {

    public static class DynamicMessageLocalDeserializer
            extends AbstractLocalSchemaDeserializer<DynamicMessage> {

        private DynamicSchema dynamicSchema;
        private String messageTypeName;
        private Descriptor messageDescriptor;

        public void preConfigure(ConnectorConfig config, boolean isKey) {
            super.preConfigure(config, isKey);
            this.messageTypeName =
                    isKey
                            ? config.getProtobufKeyMessageType()
                            : config.getProtobufValueMessageType();
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            try {
                File schemaFile = getSchemaFile(isKey);
                dynamicSchema = DynamicSchema.parseFrom(Files.newInputStream(schemaFile.toPath()));
                messageDescriptor = dynamicSchema.getMessageDescriptor(messageTypeName);
                if (messageDescriptor == null) {
                    throw new IllegalArgumentException(
                            "Message type "
                                    + "["
                                    + messageTypeName
                                    + "]"
                                    + " not found in schema "
                                    + schemaFile.getAbsolutePath());
                }
            } catch (IOException | DescriptorValidationException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public DynamicMessage deserialize(String topic, byte[] data) {
            try {
                return DynamicMessage.parseFrom(messageDescriptor, data);
            } catch (InvalidProtocolBufferException e) {
                throw new SerializationException(e.getMessage());
            }
        }
    }

    private static Deserializer<DynamicMessage> configuredDeserializer(
            ConnectorConfig config, boolean isKey) {
        checkEvaluator(config, isKey);
        Deserializer<DynamicMessage> deserializer = newDeserializer(config, isKey);
        deserializer.configure(Utils.propsToMap(config.baseConsumerProps()), isKey);
        return deserializer;
    }

    private static Deserializer<DynamicMessage> newDeserializer(
            ConnectorConfig config, boolean isKey) {
        if ((isKey && config.hasKeySchemaFile() && config.getProtobufKeyMessageType() != null)
                || (!isKey
                        && config.hasValueSchemaFile()
                        && config.getProtobufValueMessageType() != null)) {
            DynamicMessageLocalDeserializer localDeserializer =
                    new DynamicMessageLocalDeserializer();
            localDeserializer.preConfigure(config, isKey);
            return localDeserializer;
        }
        return new KafkaProtobufDeserializer<>();
    }

    private static void checkEvaluator(ConnectorConfig config, boolean isKey) {
        if (isProtobufKeyEvaluator(config, isKey)) {
            return;
        }
        if (isProtobufValueEvaluator(config, isKey)) {
            return;
        }
        throw new IllegalArgumentException("Evaluator type is not PROTOBUF");
    }

    private static boolean isProtobufKeyEvaluator(ConnectorConfig config, boolean isKey) {
        if (!isKey) {
            return false;
        }
        return config.getKeyEvaluator().is(PROTOBUF);
    }

    private static boolean isProtobufValueEvaluator(ConnectorConfig config, boolean isKey) {
        if (isKey) {
            return false;
        }
        return config.getValueEvaluator().is(PROTOBUF);
    }

    public static Deserializer<DynamicMessage> ValueDeserializer(ConnectorConfig config) {
        return configuredDeserializer(config, false);
    }

    public static Deserializer<DynamicMessage> KeyDeserializer(ConnectorConfig config) {
        return configuredDeserializer(config, true);
    }
}
