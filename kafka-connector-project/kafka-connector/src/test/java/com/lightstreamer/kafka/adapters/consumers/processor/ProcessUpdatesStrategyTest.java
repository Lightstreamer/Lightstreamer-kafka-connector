
/*
 * Copyright (C) 2025 Lightstreamer Srl
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

package com.lightstreamer.kafka.adapters.consumers.processor;

import static com.google.common.truth.Truth.assertThat;

import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluateCommandMode;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordProcessor.ProcessUpdatesType;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.AutoCommandModeProcessUpdatesStrategy;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.DefaultUpdatesStrategy;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.ExplicitCommandModeProcessUpdatesStrategy;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.ProcessUpdatesStrategy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class ProcessUpdatesStrategyTest {

    @ParameterizedTest
    @EnumSource(ProcessUpdatesType.class)
    public void shouldProcessUpdatesTypeAllowConcurrentProcessing(ProcessUpdatesType type) {
        if (type == ProcessUpdatesType.EXPLICIT_COMMAND_MODE) {
            assertThat(type.allowConcurrentProcessing()).isFalse();
        } else {
            assertThat(type.allowConcurrentProcessing()).isTrue();
        }
    }

    @Test
    public void shouldCreateDefaultStrategy() {
        ProcessUpdatesStrategy strategy = ProcessUpdatesStrategy.defaultStrategy();
        assertThat(strategy).isInstanceOf(DefaultUpdatesStrategy.class);
        assertThat(strategy.type()).isEqualTo(ProcessUpdatesType.DEFAULT);
    }

    @Test
    public void shouldCreateExplicitCommandModeStrategy() {
        ProcessUpdatesStrategy strategy = ProcessUpdatesStrategy.explicitCommandModeStrategy();
        assertThat(strategy).isInstanceOf(ExplicitCommandModeProcessUpdatesStrategy.class);
        assertThat(strategy.type()).isEqualTo(ProcessUpdatesType.EXPLICIT_COMMAND_MODE);
    }

    @Test
    public void shouldCreateAutoCommandModeStrategy() {
        ProcessUpdatesStrategy strategy = ProcessUpdatesStrategy.autoCommandModeStrategy();
        assertThat(strategy).isInstanceOf(AutoCommandModeProcessUpdatesStrategy.class);
        assertThat(strategy.type()).isEqualTo(ProcessUpdatesType.AUTO_COMMAND_MODE);
    }

    @ParameterizedTest
    @EnumSource(EvaluateCommandMode.class)
    public void shouldCreateFromEvaluateCommandMode(EvaluateCommandMode commandMode) {
        ProcessUpdatesStrategy strategy1 =
                ProcessUpdatesStrategy.fromEvaluateCommandMode(commandMode);
        switch (commandMode) {
            case AUTO:
                assertThat(strategy1).isInstanceOf(AutoCommandModeProcessUpdatesStrategy.class);
                assertThat(strategy1.type()).isEqualTo(ProcessUpdatesType.AUTO_COMMAND_MODE);
                break;
            case EXPLICIT:
                assertThat(strategy1).isInstanceOf(ExplicitCommandModeProcessUpdatesStrategy.class);
                assertThat(strategy1.type()).isEqualTo(ProcessUpdatesType.EXPLICIT_COMMAND_MODE);
                break;
            case DISABLED:
                assertThat(strategy1).isInstanceOf(DefaultUpdatesStrategy.class);
                assertThat(strategy1.type()).isEqualTo(ProcessUpdatesType.DEFAULT);
                break;
        }
    }
}
