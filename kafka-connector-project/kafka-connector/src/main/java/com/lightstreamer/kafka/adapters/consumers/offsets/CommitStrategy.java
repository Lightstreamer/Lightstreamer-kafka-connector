
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

/**
 * Strategy that determines when accumulated offsets should be committed to Kafka.
 *
 * <p>Implementations decide based on elapsed time, message count, or message rate whether a commit
 * should be triggered. Two built-in strategies are available via factory methods:
 *
 * <ul>
 *   <li>{@link #fixedCommitStrategy(long, int)} — commits at fixed time or count thresholds
 *   <li>{@link #adaptiveCommitStrategy(int)} — dynamically adjusts commit frequency based on
 *       message rate
 * </ul>
 */
public sealed interface CommitStrategy {

    /**
     * Returns whether a commit should be performed now.
     *
     * @param now the current time in milliseconds
     * @param lastCommitTimeMs the time of the last commit in milliseconds
     * @param messagesSinceLastCommit the number of messages received since the last commit
     * @return {@code true} if a commit should be triggered, {@code false} otherwise
     */
    boolean canCommit(long now, long lastCommitTimeMs, int messagesSinceLastCommit);

    /**
     * Creates a strategy that commits when a fixed time interval or message count threshold is
     * reached.
     *
     * @param commitIntervalMs the minimum interval between commits in milliseconds
     * @param commitEveryNRecords the maximum number of records before a commit is triggered
     * @return a new {@link Fixed} strategy
     */
    static CommitStrategy fixedCommitStrategy(long commitIntervalMs, int commitEveryNRecords) {
        return new Fixed(commitIntervalMs, commitEveryNRecords);
    }

    /**
     * Creates a strategy that dynamically adjusts commit frequency based on message throughput.
     *
     * @param maxCommitsPerSecond the upper bound on commits per second at low throughput
     * @return a new {@link Adaptive} strategy
     */
    static CommitStrategy adaptiveCommitStrategy(int maxCommitsPerSecond) {
        return new Adaptive(maxCommitsPerSecond);
    }

    /**
     * Commit strategy that triggers when a fixed time interval or message count threshold is
     * reached, whichever comes first.
     */
    final class Fixed implements CommitStrategy {

        private final long commitIntervalMs;
        private final int commitEveryNRecords;

        Fixed(long commitIntervalMs, int commitEveryNRecords) {
            this.commitIntervalMs = commitIntervalMs;
            this.commitEveryNRecords = commitEveryNRecords;
        }

        @Override
        public boolean canCommit(long now, long lastCommitTimeMs, int messagesSinceLastCommit) {
            if (messagesSinceLastCommit == 0) {
                return false;
            }
            boolean byTime = now - lastCommitTimeMs >= this.commitIntervalMs;
            boolean byCount = messagesSinceLastCommit >= this.commitEveryNRecords;

            return byTime || byCount;
        }
    }

    /**
     * Commit strategy that dynamically adjusts commit frequency based on message rate.
     *
     * <p>Behavior:
     *
     * <ul>
     *   <li>Commits when lag reaches the current message rate (max 1 second worth)
     *   <li>At low throughput: commits more frequently (up to maxCommitsPerSecond)
     *   <li>At high throughput: gradually throttles down to 1 commit/sec
     *   <li>Never waits longer than 10 seconds between commits (safety)
     * </ul>
     *
     * <p>Example with maxCommitsPerSecond=5:
     *
     * <pre>
     * | Message Rate | Effective Commits/sec | Max Lag   | Interval |
     * |--------------|-----------------------|-----------|----------|
     * | 10K msg/sec  | 5.0                   | 2,000     | 200ms    |
     * | 50K msg/sec  | 4.7                   | 10,638    | 213ms    |
     * | 100K msg/sec | 4.3                   | 23,256    | 233ms    |
     * | 200K msg/sec | 3.4                   | 58,824    | 294ms    |
     * | 300K msg/sec | 2.6                   | 115,385   | 385ms    |
     * | 400K msg/sec | 1.8                   | 222,222   | 556ms    |
     * | 500K msg/sec | 1.0                   | 500,000   | 1,000ms  |
     * | 1M msg/sec   | 1.0                   | 1,000,000 | 1,000ms  |
     * </pre>
     */
    final class Adaptive implements CommitStrategy {

        private static final double EMA_ALPHA = 0.3;
        private static final double LOW_RATE = 10_000;
        private static final double HIGH_RATE = 500_000;

        private final int maxCommitsPerSecond;
        private final long minCommitIntervalMs;
        private final long maxCommitIntervalMs = 10_000L;

        private volatile double avgMessageRate = 0.0;

        Adaptive(int maxCommitsPerSecond) {
            this.maxCommitsPerSecond = maxCommitsPerSecond;
            this.minCommitIntervalMs = 1000L / maxCommitsPerSecond;
        }

        @Override
        public boolean canCommit(long now, long lastCommitTimeMs, int messagesSinceLastCommit) {
            if (messagesSinceLastCommit == 0) {
                return false;
            }

            long elapsed = now - lastCommitTimeMs;

            // Hard limit: never exceed maxCommitsPerSecond
            if (elapsed < minCommitIntervalMs) {
                return false;
            }

            // Calculate and update average message rate
            double currentRate = elapsed > 0 ? (messagesSinceLastCommit * 1000.0) / elapsed : 0;
            avgMessageRate =
                    avgMessageRate == 0.0
                            ? currentRate
                            : EMA_ALPHA * currentRate + (1 - EMA_ALPHA) * avgMessageRate;

            // Calculate effective commits/sec that scales with message rate:
            // - At LOW_RATE or below: use maxCommitsPerSecond
            // - At HIGH_RATE or above: use 1 commit/sec
            // - In between: linear interpolation
            double effectiveCommitsPerSec;
            if (avgMessageRate <= LOW_RATE) {
                effectiveCommitsPerSec = maxCommitsPerSecond;
            } else if (avgMessageRate >= HIGH_RATE) {
                effectiveCommitsPerSec = 1.0;
            } else {
                // Linear interpolation between maxCommitsPerSecond and 1
                double scale = (avgMessageRate - LOW_RATE) / (HIGH_RATE - LOW_RATE);
                effectiveCommitsPerSec = maxCommitsPerSecond - scale * (maxCommitsPerSecond - 1);
            }

            // Lag threshold = rate / effectiveCommitsPerSec
            int maxLag = Math.max(1, (int) (avgMessageRate / effectiveCommitsPerSec));

            // Commit when lag exceeds threshold OR max interval reached (safety)
            return messagesSinceLastCommit >= maxLag || elapsed >= maxCommitIntervalMs;
        }
    }
}
