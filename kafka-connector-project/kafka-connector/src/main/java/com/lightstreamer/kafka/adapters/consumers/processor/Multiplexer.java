
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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class Multiplexer<S> implements TaskExecutor<S> {

    private final ExecutorService executor;

    /*
     * The Map associates each sequence to the queue of tasks to be run.
     * The queue is implemented through the "next" element of each task.
     *
     * Associated invariant:
     * - As long as a sequence is present in the Map, its queue is being processed by a dequeuing activity.
     *   When the dequeueing activity consumes the queue, it removes the sequence from the Map.
     *   Hence, at most one dequeueing activity can be active at any time.
     * - The element currently mapped corresponds to the last task of the queue if its "next" element is null;
     *   otherwise, it is a transient state MOD in which the queue is currently being modified.
     *
     * Associated constraints:
     * - Only the task that can add its sequence to the Map can also schedule a dequeueing activity.
     *   If the sequence is already present in the Map, other tasks can only add themselves to the queue.
     *   The dequeueing activity can take care of added tasks either immediately or by rescheduling;
     *   the latter is done by delegating the new scheduling to the next task left in the queue.
     *   If the end of the queue is reached, the dequeueing activity should remove the sequence from the Map.
     *
     * - Only one task at a time can add itself to an existing queue.
     *   In fact, the addition is a two-step process:
     *   first, the task sets itself as the next of the task associated in the Map, thus bringing state MOD;
     *   then, the task sets itself as the associated element in the Map, thus terminating state MOD.
     *   During state MOD, other tasks willing to add themselves must wait.
     *   On the other hand, the first step is enough for the dequeueing activity to process the task;
     *   hence the task may even be mapped after it has already been processed.
     *
     * - Moreover, during sequence removal by the dequeueing activity, new task additions cannot be run.
     *   In fact, sequence removal is also a two-step process:
     *   first, the last task of the queue is set as the next of itself;
     *   then, the sequence is removed from the Map.
     *   The first step either brings state MOD, or occurs while already in state MOD.
     *   In the latter case, the last task is still to be associated in the Map;
     *   anyway, after association, the state MOD persists, thus new additions are still prevented;
     *   on the other hand, the second step (the removal) may occur before the reassociation;
     *   in this case, the reassociation is just redundant and should be prevented;
     *   hence, an adding task must always check this possibility when trying to associate itself.
     *   Obviously, the second step terminates state MOD and enables new additions;
     *   but now, pending task additions will also readd the sequence.
     */
    private final ConcurrentMap<S, LinkableTask> queues;

    private final MyCountDownLatch emptinessChecker;

    private static final int CPU_BOUND_BATCH = 1000; // experimental

    public Multiplexer(ExecutorService executor, boolean batchSupport) {
        this.executor = executor;

        if (batchSupport) {
            emptinessChecker = new MyCountDownLatch();
        } else {
            emptinessChecker = null;
        }

        queues = new ConcurrentHashMap<>();
    }

    public boolean supportsBatching() {
        return emptinessChecker != null;
    }

    private void runTask(Runnable task) {
        try {
            task.run();
        } finally {
            if (emptinessChecker != null) {
                emptinessChecker.decrement();
            }
        }
    }

    @Override
    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    throw new RuntimeException("Executor not terminated!");
                }
            }
        } catch (InterruptedException e) {
            // (Re-)Cancel if current thread also interrupted
            executor.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }

    private class LinkableTask implements Runnable {
        private final S sequence;
        private final Runnable deferredTask;
        private final AtomicReference<LinkableTask> next;

        public LinkableTask(S sequence, Runnable deferredTask) {
            this.sequence = sequence;
            this.deferredTask = deferredTask;
            next = new AtomicReference<>(null);
        }

        public void attach() {
            while (true) {
                LinkableTask last = queues.putIfAbsent(sequence, this);
                if (last == null) {
                    // there were no tasks in queue for the sequence;
                    // I can safely schedule the dequeuing activity:
                    // other requests will work on this.next
                    executor.execute(this);
                    break;
                } else {
                    // there was a task queue in place and last is the last task,
                    // but only as long as last.next is null
                    if (last.next.compareAndSet(null, this)) {
                        // [A]
                        // there was no next and now state MOD is established;
                        // I can safely notify myself as the new last:
                        // other requests must get back to the map and wait there;
                        // the only contention is with my own getNext in [B],
                        // which may have already removed the element

                        if (!queues.replace(sequence, last, this)) {
                            // if a removal came first, this was not necessary
                        }
                        break;
                    } else {
                        // there is another addition in place (state MOD);
                        // I could follow the queue, but it is better to retry from the beginning,
                        // because in the meantime the queue may also be executed and removed
                        continue;
                    }
                }
            }
        }

        @Override
        public void run() {
            // dequeuing activity
            LinkableTask last = this;

            try {
                runTask(deferredTask);

                // Check if there are other waiting tasks for this sequence;
                // assuming CPU-bound tasks, we can execute them immediately,
                // provided that we avoid starvation
                for (int i = 0; i < CPU_BOUND_BATCH; i++) {
                    last = last.getNext();
                    if (last != null) {
                        runTask(last.deferredTask);
                    } else {
                        // the task queue has been completed and the sequence already removed
                        // (a new queue may be concurrently being initiated)
                        last = null;
                        break;
                    }
                }

            } catch (RuntimeException | Error e) {
                // this is unexpected; the Runnable received is supposed to check its own exceptions
                throw e;

            } finally {
                if (last != null) {
                    // We haven't finished the dequeuing for this sequence;
                    // to continue, we resort to the executor, for fairness;
                    // in case of exception executing last, we will skip it
                    last.launchNext();
                }
            }
        }

        protected LinkableTask getNext() {
            if (next.compareAndSet(null, this)) {
                // [B]
                // there was no next and further additions are now blocked;
                // I can safely remove the queue as completed:
                // other requests must get back to the map and wait there (state MOD);
                // the only contention is with my own attach in [A],
                // which may haven't set the element yet in the map;
                // in this case, that mapping will no longer be needed

                queues.remove(sequence);
                return null;
            } else {
                // there was a next and it is now immutable
                return next.get();
            }
        }

        protected void launchNext() {
            LinkableTask nextTask = getNext();
            if (nextTask != null) {
                // the next task will take care of itself and the rest of the task queue
                executor.execute(nextTask);
            } else {
                // the task queue was completed and the sequence already removed
                // (a new queue may be concurrently being initiated)
            }
        }
    }

    @Override
    public void execute(S sequence, Runnable task) {
        if (emptinessChecker != null) {
            emptinessChecker.increment();
        }
        if (sequence == null) {
            // No requirement to sequentialize tasks that have no key set
            if (emptinessChecker != null) {
                executor.execute(() -> runTask(task));
            } else {
                executor.execute(task);
            }

        } else {
            // Create the task and attach it to the task queue for the sequence;
            // this will also take care of ensuring that the task will be dequeued
            LinkableTask linkableTask = new LinkableTask(sequence, task);
            linkableTask.attach();
        }
    }

    private class MyCountDownLatch {
        // couldn't find this functionality in the JDK

        private final AtomicLong count = new AtomicLong(0);

        private final Semaphore semaphore = new Semaphore(0);

        private final AtomicBoolean waiting = new AtomicBoolean(false);

        private void increment() {
            count.incrementAndGet();
        }

        private void decrement() {
            long val = count.decrementAndGet();
            if (val == 0) {
                if (waiting.compareAndSet(true, false)) {
                    semaphore.release();
                }
            }
        }

        private void waitEmpty() {
            if (!waiting.compareAndSet(false, true)) {
                throw new IllegalStateException();
            }
            if (count.get() == 0) {
                if (waiting.compareAndSet(true, false)) {
                    assert (semaphore.availablePermits() == 0);
                    return;
                } else {
                    // race condition: semaphore being released
                }
            }
            try {
                semaphore.acquire();
                // assert (count.get() == 0); unless other producers are still running
                assert (semaphore.availablePermits() == 0);
                assert (waiting.get() == false);
            } catch (InterruptedException e) {
                if (waiting.compareAndSet(true, false)) {
                    assert (semaphore.availablePermits() == 0);
                    throw new IllegalStateException();
                } else {
                    // race condition
                    while (semaphore.drainPermits() == 0)
                        ;
                }
            }
        }
    }

    @Override
    public void waitBatch() {
        if (emptinessChecker == null) {
            throw new UnsupportedOperationException();
        }
        synchronized (emptinessChecker) {
            emptinessChecker.waitEmpty();
        }
    }
}
