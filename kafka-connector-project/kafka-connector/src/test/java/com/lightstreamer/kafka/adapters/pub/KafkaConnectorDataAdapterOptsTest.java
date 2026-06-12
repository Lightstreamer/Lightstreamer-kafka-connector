
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

package com.lightstreamer.kafka.adapters.pub;

import static com.google.common.truth.Truth.assertThat;

import com.lightstreamer.interfaces.metadata.Mode;
import com.lightstreamer.kafka.adapters.pub.KafkaConnectorMetadataAdapter.KafkaConnectorDataAdapterOpts;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public class KafkaConnectorDataAdapterOptsTest {

    static Stream<Arguments> supportedModes() {
        return Stream.of(
                Arguments.of(
                        Optional.empty(),
                        Set.of(Mode.COMMAND, Mode.MERGE, Mode.DISTINCT, Mode.RAW),
                        true),
                Arguments.of(Optional.of(Mode.COMMAND), Set.of(Mode.COMMAND), true),
                Arguments.of(
                        Optional.of(Mode.COMMAND),
                        Set.of(Mode.MERGE, Mode.DISTINCT, Mode.RAW),
                        false),
                Arguments.of(Optional.of(Mode.MERGE), Set.of(Mode.MERGE), true),
                Arguments.of(
                        Optional.of(Mode.MERGE),
                        Set.of(Mode.COMMAND, Mode.DISTINCT, Mode.RAW),
                        false),
                Arguments.of(Optional.of(Mode.DISTINCT), Set.of(Mode.DISTINCT), true),
                Arguments.of(
                        Optional.of(Mode.DISTINCT),
                        Set.of(Mode.MERGE, Mode.COMMAND, Mode.RAW),
                        false));
    }

    @ParameterizedTest
    @MethodSource("supportedModes")
    public void shouldCreateKafkaConnectorDataAdapterOpts(
            Optional<Mode> subscriptionMode, Set<Mode> modesToTest, boolean expectedSupport) {
        KafkaConnectorDataAdapterOpts opts =
                new KafkaConnectorDataAdapterOpts("CONNECTOR", true, subscriptionMode, 10);
        assertThat(opts.dataAdapterName()).isEqualTo("CONNECTOR");
        assertThat(opts.enabled()).isTrue();

        assertThat(opts.itemSnapshotDistinctLength()).isEqualTo(10);
        for (Mode mode : modesToTest) {
            assertThat(opts.supportMode(mode)).isEqualTo(expectedSupport);
        }
    }

    @Test
    public void shouldNotCreateKafkaConnectorDataAdapterOptsWithRawSubscriptionMode() {
        try {
            new KafkaConnectorDataAdapterOpts("CONNECTOR", true, Optional.of(Mode.RAW), 10);
        } catch (IllegalArgumentException e) {
            assertThat(e)
                    .hasMessageThat()
                    .contains("RAW is not a valid configurable subscription mode");
        }
    }
}
