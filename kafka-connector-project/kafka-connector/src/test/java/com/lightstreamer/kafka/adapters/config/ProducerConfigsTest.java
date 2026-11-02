
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

package com.lightstreamer.kafka.adapters.config;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.adapters.config.ProducerConfigs.BOOTSTRAP_SERVERS;
import static com.lightstreamer.kafka.adapters.config.ProducerConfigs.DELIVERY_TIMEOUT_MS;
import static com.lightstreamer.kafka.adapters.config.ProducerConfigs.IDEMPOTENCE_ENABLE;
import static com.lightstreamer.kafka.adapters.config.ProducerConfigs.LINGER_MS;
import static com.lightstreamer.kafka.adapters.config.ProducerConfigs.MAX_BLOCK_MS;
import static com.lightstreamer.kafka.adapters.config.ProducerConfigs.RECORD_ACKS;
import static com.lightstreamer.kafka.adapters.config.ProducerConfigs.RECORD_KEY_EVALUATOR_TYPE;
import static com.lightstreamer.kafka.adapters.config.ProducerConfigs.RECORD_VALUE_EVALUATOR_TYPE;
import static com.lightstreamer.kafka.adapters.config.ProducerConfigs.REQUEST_TIMEOUT_MS;
import static com.lightstreamer.kafka.adapters.config.ProducerConfigs.RETRIES;
import static com.lightstreamer.kafka.adapters.config.ProducerConfigs.SEND_CONFIRMATION_MODE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.CONFIRMATION_MODE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.EVALUATOR;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.HOST_LIST;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.NON_NEGATIVE_INT;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.NON_NEGATIVE_LONG;

import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfParameter;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType;

import org.junit.jupiter.api.Test;

class ProducerConfigsTest {

    @Test
    void shouldReturnConfigSpec() {
        ConfigsSpec configSpec = ProducerConfigs.spec();

        ConfParameter bootStrapServers = configSpec.findParameter(BOOTSTRAP_SERVERS);
        assertThat(bootStrapServers.name()).isEqualTo(BOOTSTRAP_SERVERS);
        assertThat(bootStrapServers.required()).isTrue();
        assertThat(bootStrapServers.multiple()).isFalse();
        assertThat(bootStrapServers.mutable()).isTrue();
        assertThat(bootStrapServers.defaultValue()).isNull();
        assertThat(bootStrapServers.type()).isEqualTo(HOST_LIST);

        ConfParameter keyEvaluatorType = configSpec.findParameter(RECORD_KEY_EVALUATOR_TYPE);
        assertThat(keyEvaluatorType.name()).isEqualTo(RECORD_KEY_EVALUATOR_TYPE);
        assertThat(keyEvaluatorType.required()).isFalse();
        assertThat(keyEvaluatorType.multiple()).isFalse();
        assertThat(keyEvaluatorType.mutable()).isTrue();
        assertThat(keyEvaluatorType.defaultValue()).isEqualTo("STRING");
        assertThat(keyEvaluatorType.type()).isEqualTo(EVALUATOR);

        ConfParameter valueEvaluatorType = configSpec.findParameter(RECORD_VALUE_EVALUATOR_TYPE);
        assertThat(valueEvaluatorType.name()).isEqualTo(RECORD_VALUE_EVALUATOR_TYPE);
        assertThat(valueEvaluatorType.required()).isFalse();
        assertThat(valueEvaluatorType.multiple()).isFalse();
        assertThat(valueEvaluatorType.mutable()).isTrue();
        assertThat(valueEvaluatorType.defaultValue()).isEqualTo("STRING");
        assertThat(valueEvaluatorType.type()).isEqualTo(EVALUATOR);

        ConfParameter recordAcks = configSpec.findParameter(RECORD_ACKS);
        assertThat(recordAcks.name()).isEqualTo(RECORD_ACKS);
        assertThat(recordAcks.required()).isFalse();
        assertThat(recordAcks.multiple()).isFalse();
        assertThat(recordAcks.mutable()).isTrue();
        assertThat(recordAcks.defaultValue()).isEqualTo("all");
        assertThat(recordAcks.type()).isEqualTo(ConfType.ACKS);

        ConfParameter sendConfirmationMode = configSpec.findParameter(SEND_CONFIRMATION_MODE);
        assertThat(sendConfirmationMode.name()).isEqualTo(SEND_CONFIRMATION_MODE);
        assertThat(sendConfirmationMode.required()).isFalse();
        assertThat(sendConfirmationMode.multiple()).isFalse();
        assertThat(sendConfirmationMode.mutable()).isTrue();
        assertThat(sendConfirmationMode.defaultValue()).isEqualTo("CONFIRMED");
        assertThat(sendConfirmationMode.type()).isEqualTo(CONFIRMATION_MODE);

        ConfParameter enableIdempotence = configSpec.findParameter(IDEMPOTENCE_ENABLE);
        assertThat(enableIdempotence.name()).isEqualTo(IDEMPOTENCE_ENABLE);
        assertThat(enableIdempotence.required()).isFalse();
        assertThat(enableIdempotence.multiple()).isFalse();
        assertThat(enableIdempotence.mutable()).isTrue();
        assertThat(enableIdempotence.defaultValue()).isEqualTo("true");
        assertThat(enableIdempotence.type()).isEqualTo(ConfType.BOOL);

        ConfParameter retries = configSpec.findParameter(RETRIES);
        assertThat(retries.name()).isEqualTo(RETRIES);
        assertThat(retries.required()).isFalse();
        assertThat(retries.multiple()).isFalse();
        assertThat(retries.mutable()).isTrue();
        assertThat(retries.defaultValue()).isEqualTo(Integer.toString(Integer.MAX_VALUE));
        assertThat(retries.type()).isEqualTo(NON_NEGATIVE_INT);

        ConfParameter lingerMs = configSpec.findParameter(LINGER_MS);
        assertThat(lingerMs.name()).isEqualTo(LINGER_MS);
        assertThat(lingerMs.required()).isFalse();
        assertThat(lingerMs.multiple()).isFalse();
        assertThat(lingerMs.mutable()).isTrue();
        assertThat(lingerMs.defaultValue()).isEqualTo("0");
        assertThat(lingerMs.type()).isEqualTo(NON_NEGATIVE_LONG);

        ConfParameter deliverTimeoutMs = configSpec.findParameter(DELIVERY_TIMEOUT_MS);
        assertThat(deliverTimeoutMs.name()).isEqualTo(DELIVERY_TIMEOUT_MS);
        assertThat(deliverTimeoutMs.required()).isFalse();
        assertThat(deliverTimeoutMs.multiple()).isFalse();
        assertThat(deliverTimeoutMs.mutable()).isTrue();
        assertThat(deliverTimeoutMs.defaultValue()).isEqualTo("120000");
        assertThat(deliverTimeoutMs.type()).isEqualTo(NON_NEGATIVE_LONG);

        ConfParameter requestTimeoutMs = configSpec.findParameter(REQUEST_TIMEOUT_MS);
        assertThat(requestTimeoutMs.name()).isEqualTo(REQUEST_TIMEOUT_MS);
        assertThat(requestTimeoutMs.required()).isFalse();
        assertThat(requestTimeoutMs.multiple()).isFalse();
        assertThat(requestTimeoutMs.mutable()).isTrue();
        assertThat(requestTimeoutMs.defaultValue()).isEqualTo("30000");
        assertThat(requestTimeoutMs.type()).isEqualTo(NON_NEGATIVE_INT);

        ConfParameter maxBlockMs = configSpec.findParameter(MAX_BLOCK_MS);
        assertThat(maxBlockMs.name()).isEqualTo(MAX_BLOCK_MS);
        assertThat(maxBlockMs.required()).isFalse();
        assertThat(maxBlockMs.multiple()).isFalse();
        assertThat(maxBlockMs.mutable()).isTrue();
        assertThat(maxBlockMs.defaultValue()).isEqualTo("1000");
        assertThat(maxBlockMs.type()).isEqualTo(NON_NEGATIVE_LONG);
    }
}
