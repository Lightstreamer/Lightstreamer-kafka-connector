
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

package com.lightstreamer.kafka.adapters.consumers.offsets;

import static com.google.common.truth.Truth.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class FixedThresholdCommitStrategyTest {

    private Offsets.CommitStrategy strategy;

    @BeforeEach
    public void setUp() {
        this.strategy = Offsets.CommitStrategy.fixedCommitStrategy(5000, 100_000);
    }

    @ParameterizedTest
    @CsvSource({
        // 6 seconds ago (> 5s), 5k messages (< 100k) -> exceeds time threshold
        "10000, 4000, 5000, true",
        // 6 seconds ago (> 5s), 10k messages (< 100k) -> exceeds time threshold
        "10000, 4000, 10000, true",
        // 6 seconds ago (> 5s), 20k messages (< 100k) -> exceeds message threshold
        "10000, 4000, 20000, true",
        // 6 seconds ago (> 5s), 0 messages (< 100k) -> exceeds time threshold
        "10000, 4000, 0, true",
        // 1 second ago (< 5s), 100k messages (> 100k) -> exactly at message threshold
        "5000, 4000, 100000, true",
        // 1 second ago (< 5s), 150k messages (> 100k) -> exceeds message threshold
        "5000, 4000, 150000, true",
        // 3 seconds ago (< 5s), 50k messages (< 100k) -> below both thresholds
        "8000, 5000, 50000, false",
        // Exactly 5 seconds ago, 100 messages (< 100k)-> exactly at time threshold
        "10000, 5000, 100, true",
        // 10 seconds ago (> 5s), 200k messages (> 100k) -> exceeds both thresholds
        "20000, 10000, 200000, true",
        // No time elapsed, 50k messages (< 100k) -> below both thresholds
        "1000, 1000, 50000, false",
        // 3 seconds ago (< 5s), 0 messages (< 100k) -> below both thresholds
        "5000, 2000, 0, false"
    })
    public void shouldCommitWhenThresholdsExceeded(
            long now, long lastCommitTimeMs, int messagesSinceLastCommit, boolean expected) {
        assertThat(strategy.canCommit(now, lastCommitTimeMs, messagesSinceLastCommit))
                .isEqualTo(expected);
    }
}
