
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

package com.lightstreamer.kafka.adapters.consumers.processor;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Alternative implementation of the {@code TaskExecutor} interface. */
class HashedKeyedEventProcessor<S> implements TaskExecutor<S> {

    private final ExecutorService[] executors;
    private final int numBuckets;

    public HashedKeyedEventProcessor(int numBuckets) {
        this.numBuckets = numBuckets;
        this.executors = new ExecutorService[numBuckets];

        for (int i = 0; i < numBuckets; i++) {
            final int s = i;
            executors[i] =
                    Executors.newSingleThreadExecutor(
                            r -> new Thread(r, "HashedKeyedEventProcessor-" + s));
        }
    }

    @Override
    public void execute(S sequence, Runnable task) {
        int bucket =
                sequence != null
                        ? Math.abs(sequence.hashCode() % numBuckets)
                        : new Random().nextInt(numBuckets);
        executors[bucket].submit(task);
    }

    @Override
    public void shutdown() {
        for (ExecutorService executor : executors) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                    if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                        throw new RuntimeException("Executor not terminated!");
                    }
                } else {
                    System.out.println("Executor shut down gracefully!");
                }

            } catch (InterruptedException e) {
                // (Re-)Cancel if current thread also interrupted
                executor.shutdownNow();
                // Preserve interrupt status
                Thread.currentThread().interrupt();
            }
        }
    }
}
