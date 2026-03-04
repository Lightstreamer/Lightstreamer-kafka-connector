
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

package com.lightstreamer.kafka.adapters.consumers.offsets;

import static com.google.common.truth.Truth.assertThat;

import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.CommitStrategy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class AdaptiveThresholdCommitStrategyTest {

    private CommitStrategy strategy;

    @BeforeEach
    void setUp() {
        strategy = CommitStrategy.adaptiveCommitStrategy(5); // max 5 commits/sec
    }

    // ==================== Hard limits ====================

    @Test
    @DisplayName("Should not commit when no messages")
    void shouldNotCommitWhenNoMessages() {
        long now = 1000L;
        long lastCommit = 0L;
        int messagesSinceLastCommit = 0;

        assertThat(strategy.canCommit(now, lastCommit, messagesSinceLastCommit)).isFalse();
    }

    @Test
    @DisplayName("Should not commit before minCommitInterval (200ms for 5 commits/sec)")
    void shouldNotCommitBeforeMinInterval() {
        long lastCommit = 0L;
        long now = 100L; // Only 100ms elapsed
        int messagesSinceLastCommit = 10000;

        assertThat(strategy.canCommit(now, lastCommit, messagesSinceLastCommit)).isFalse();
    }

    @Test
    @DisplayName("Should commit when maxCommitInterval (10s) reached regardless of lag")
    void shouldCommitWhenMaxIntervalReached() {
        long lastCommit = 0L;
        long now = 10_001L; // 10+ seconds elapsed
        int messagesSinceLastCommit = 1; // Very low lag

        assertThat(strategy.canCommit(now, lastCommit, messagesSinceLastCommit)).isTrue();
    }

    // ==================== Low throughput behavior (≤10K msg/sec) ====================

    @Test
    @DisplayName("Should allow up to 5 commits/sec at low rate")
    void shouldAllowMaxCommitsPerSecondAtLowRate() {
        // Simulate 10K msg/sec: 2000 messages in 200ms
        long lastCommit = 0L;
        long now = 200L;
        int messagesSinceLastCommit = 2000;

        // First call establishes the rate and should commit
        assertThat(strategy.canCommit(now, lastCommit, messagesSinceLastCommit)).isTrue();
    }

    @Test
    @DisplayName("Should not commit if lag threshold not reached at low rate")
    void shouldNotCommitIfLagNotReached() {
        // First call to establish rate (10K msg/sec)
        strategy.canCommit(1000L, 0L, 10_000);

        // Second call: 200ms elapsed but only 500 messages (below threshold of ~2000)
        assertThat(strategy.canCommit(1200L, 1000L, 500)).isFalse();
    }

    // ==================== High throughput behavior (≥500K msg/sec) ====================

    @Test
    @DisplayName("Should throttle to 1 commit/sec at high rate")
    void shouldThrottleToOneCommitPerSecAtHighRate() {
        // Simulate 500K msg/sec: 500K messages in 1000ms
        long lastCommit = 0L;
        long now = 1000L;
        int messagesSinceLastCommit = 500_000;

        assertThat(strategy.canCommit(now, lastCommit, messagesSinceLastCommit)).isTrue();
    }

    @Test
    @DisplayName("Should not commit at 500ms even with high message count")
    void shouldNotCommitTooEarlyAtHighRate() {
        // First call to establish high rate
        strategy.canCommit(1000L, 0L, 500_000);

        // At 500ms with 250K messages - should wait for full 1 second worth
        // Rate is 500K/sec, so lag threshold is 500K
        assertThat(strategy.canCommit(1500L, 1000L, 250_000)).isFalse();
    }

    @Test
    @DisplayName("Should commit when lag reaches message rate at high throughput")
    void shouldCommitWhenLagReachesRate() {
        // First call to establish rate
        strategy.canCommit(1000L, 0L, 500_000);

        // Second call: 1000ms elapsed, 500K messages (equals rate)
        assertThat(strategy.canCommit(2000L, 1000L, 500_000)).isTrue();
    }

    // ==================== Intermediate throughput (gradual throttling) ====================

    @Test
    @DisplayName("Should use intermediate commit frequency at 200K msg/sec")
    void shouldUseIntermediateFrequencyAt200K() {
        // First call to establish rate: 200K messages in 1000ms = 200K msg/sec
        strategy.canCommit(1000L, 0L, 200_000);

        // At 200K msg/sec, effective commits/sec should be ~3.4
        // Lag threshold = 200K / 3.4 ≈ 58,824
        // With ~60K messages, should trigger commit
        assertThat(strategy.canCommit(1300L, 1000L, 60_000)).isTrue();
    }

    @Test
    @DisplayName("Should gradually reduce commit frequency as rate increases")
    void shouldGraduallyReduceFrequency() {
        CommitStrategy strategy1 = CommitStrategy.adaptiveCommitStrategy(5);
        CommitStrategy strategy2 = CommitStrategy.adaptiveCommitStrategy(5);
        CommitStrategy strategy3 = CommitStrategy.adaptiveCommitStrategy(5);

        // Establish different rates (first call sets avgMessageRate)
        strategy1.canCommit(1000L, 0L, 100_000); // 100K msg/sec
        strategy2.canCommit(1000L, 0L, 300_000); // 300K msg/sec
        strategy3.canCommit(1000L, 0L, 500_000); // 500K msg/sec

        // At 500ms with proportional messages:
        // 100K rate: threshold ~23K, 50K messages -> should commit
        // 300K rate: threshold ~115K, 50K messages -> should not commit
        // 500K rate: threshold 500K, 50K messages -> should not commit

        assertThat(strategy1.canCommit(1500L, 1000L, 50_000)).isTrue();
        assertThat(strategy2.canCommit(1500L, 1000L, 50_000)).isFalse();
        assertThat(strategy3.canCommit(1500L, 1000L, 50_000)).isFalse();
    }

    // ==================== EMA smoothing ====================

    @Test
    @DisplayName("Should smooth rate fluctuations with EMA")
    void shouldSmoothRateFluctuations() {
        // First call: high rate (500K msg/sec)
        strategy.canCommit(1000L, 0L, 500_000);

        // Second call: suddenly low rate (10K msg/sec)
        // EMA should smooth this: new_avg = 0.3 * 10K + 0.7 * 500K = 353K
        // So threshold should still be relatively high
        strategy.canCommit(2000L, 1000L, 10_000);

        // Third call: low messages should not trigger commit due to smoothed high threshold
        assertThat(strategy.canCommit(2200L, 2000L, 5_000)).isFalse();
    }

    // ==================== Different maxCommitsPerSecond values ====================

    @Test
    @DisplayName("With maxCommitsPerSecond=1, minInterval should be 1000ms")
    void shouldHave1SecIntervalWithMaxCommits1() {
        CommitStrategy strategy1 = CommitStrategy.adaptiveCommitStrategy(1);

        // Should not commit before 1000ms
        assertThat(strategy1.canCommit(500L, 0L, 10_000)).isFalse();

        // Should commit after 1000ms
        assertThat(strategy1.canCommit(1000L, 0L, 10_000)).isTrue();
    }

    @Test
    @DisplayName("With maxCommitsPerSecond=2, minInterval should be 500ms")
    void shouldHave500msIntervalWithMaxCommits2() {
        CommitStrategy strategy2 = CommitStrategy.adaptiveCommitStrategy(2);

        // Should not commit before 500ms
        assertThat(strategy2.canCommit(400L, 0L, 10_000)).isFalse();

        // Should commit after 500ms with sufficient lag
        // At 500ms with 5000 messages: rate = 10K msg/sec (at LOW_RATE threshold)
        // effectiveCommitsPerSec = 2, maxLag = 10K / 2 = 5000
        // So 5000 messages should trigger commit
        assertThat(strategy2.canCommit(500L, 0L, 5_000)).isTrue();
    }
}
