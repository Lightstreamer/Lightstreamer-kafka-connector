
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
import static com.lightstreamer.kafka.adapters.config.PublishConfigs.BOOTSTRAP_SERVERS;
import static com.lightstreamer.kafka.adapters.config.PublishConfigs.RECORD_KEY_EVALUATOR_TYPE;
import static com.lightstreamer.kafka.adapters.config.PublishConfigs.RECORD_VALUE_EVALUATOR_TYPE;

import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfParameter;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType;

import org.junit.jupiter.api.Test;

class PublishConfigTest {

    @Test
    void shouldReturnConfigSpec() {
        ConfigsSpec configSpec = PublishConfigs.spec();

        ConfParameter bootStrapServers = configSpec.findParameter(BOOTSTRAP_SERVERS);
        assertThat(bootStrapServers.name()).isEqualTo(BOOTSTRAP_SERVERS);
        assertThat(bootStrapServers.required()).isTrue();
        assertThat(bootStrapServers.multiple()).isFalse();
        assertThat(bootStrapServers.mutable()).isTrue();
        assertThat(bootStrapServers.defaultValue()).isNull();
        assertThat(bootStrapServers.type()).isEqualTo(ConfType.HOST_LIST);

        ConfParameter keyEvaluatorType = configSpec.findParameter(RECORD_KEY_EVALUATOR_TYPE);
        assertThat(keyEvaluatorType.name()).isEqualTo(RECORD_KEY_EVALUATOR_TYPE);
        assertThat(keyEvaluatorType.required()).isFalse();
        assertThat(keyEvaluatorType.multiple()).isFalse();
        assertThat(keyEvaluatorType.mutable()).isTrue();
        assertThat(keyEvaluatorType.defaultValue()).isEqualTo("STRING");
        assertThat(keyEvaluatorType.type()).isEqualTo(ConfType.EVALUATOR);

        ConfParameter valueEvaluatorType = configSpec.findParameter(RECORD_VALUE_EVALUATOR_TYPE);
        assertThat(valueEvaluatorType.name()).isEqualTo(RECORD_VALUE_EVALUATOR_TYPE);
        assertThat(valueEvaluatorType.required()).isFalse();
        assertThat(valueEvaluatorType.multiple()).isFalse();
        assertThat(valueEvaluatorType.mutable()).isTrue();
        assertThat(valueEvaluatorType.defaultValue()).isEqualTo("STRING");
        assertThat(valueEvaluatorType.type()).isEqualTo(ConfType.EVALUATOR);
    }
}
