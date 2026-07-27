
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

import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordProcessor.ProcessUpdatesType;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.CommandModeProcessUpdatesStrategy;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.DefaultUpdatesStrategy;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.ProcessUpdatesStrategy;

import org.junit.jupiter.api.Test;

class ProcessUpdatesStrategyTest {

    @Test
    void shouldCreateDefaultStrategy() {
        ProcessUpdatesStrategy strategy = ProcessUpdatesStrategy.defaultStrategy();
        assertThat(strategy).isInstanceOf(DefaultUpdatesStrategy.class);
        assertThat(strategy.type()).isEqualTo(ProcessUpdatesType.DEFAULT);
    }

    @Test
    void shouldCreateCommandModeStrategy() {
        ProcessUpdatesStrategy strategy = ProcessUpdatesStrategy.commandModeStrategy();
        assertThat(strategy).isInstanceOf(CommandModeProcessUpdatesStrategy.class);
        assertThat(strategy.type()).isEqualTo(ProcessUpdatesType.COMMAND_MODE);
    }
}
