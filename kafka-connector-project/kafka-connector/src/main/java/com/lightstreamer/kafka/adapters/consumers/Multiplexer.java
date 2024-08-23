package com.lightstreamer.kafka.adapters.consumers;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

public class Multiplexer {

    private final ExecutorService executor;

    /*
     * The Map associates each sequence to the queue of tasks to be run.
     * The queue is implemented through the "next" member of each task.
     * 
     * Associated invariant:
     * - As long as a sequence is present in the Map, its queue is being processed by a dequeuing activity.
     *   When the dequeuing activity consumes the queue, it removes the sequence from the Map.
     *   Hence, at most one dequeuing activity can be active at any time.
     * - The element currently associated in the Map corresponds to the last task of the queue;
     *   more precisely, this holds only if its "next" member is null;
     *   otherwise, we are in a transient state MOD in which the queue is currently being modified.
     * 
     * Associated constraints:
     * - Only the task that can add its sequence to the Map can also schedule a dequeuing activity.
     *   If the sequence is already present in the Map, other tasks can only add themselves to the queue.
     *   The dequeuing activity can take care of added tasks either immediately or by rescheduling;
     *   the latter is done by delegating the new scheduling to the next task left in the queue. 
     *   If the end of the queue is reached, the dequeuing activity should remove the sequence from the Map.
     * 
     * - Only one task at a time can add itself to an existing queue.
     *   In fact, the addition is a two-step process:
     *   - first, the task sets itself as the next of the task associated in the Map, thus bringing state MOD;
     *   - then, the task sets itself as the associated element in the Map, thus terminating state MOD.
     *   During state MOD, other tasks willing to add themselves must wait.
     *   On the other hand, the first step is enough for the dequeuing activity to process the task;
     *   hence the task may even be mapped after it has already been processed.
     *
     * - Moreover, during sequence removal by the dequeuing activity, new task additions have to wait. 
     *   In fact, sequence removal is also a two-step process:
     *   - first, the last task of the queue is set as the next of itself;
     *   - then, the sequence is removed from the Map.
     *   The first step either brings state MOD, or occurs while already in state MOD.
     *   In the latter case, the last task is still to be associated in the Map;
     *   anyway, after association, the state MOD persists, thus new additions are still blocked;
     *   on the other hand, the second step (the removal) may occur before the new association;
     *   in this case, the new association would be just redundant and should be prevented;
     *   hence, an adding task must always check this possibility when trying to associate itself.
     *   Obviously, the second step terminates state MOD and enables new additions;
     *   but now, the first pending task addition will also readd the sequence to the Map.
     */
    private final ConcurrentMap<String, LinkableTask> queues;

    private static final int CPU_BOUND_BATCH = 1000; // experimental

    public Multiplexer(ExecutorService executor) {
        this.executor = executor;

        queues = new ConcurrentHashMap<>();
    }

    private class LinkableTask implements Runnable {
        private final String sequence;
        private final Runnable deferredTask;
        private final AtomicReference<LinkableTask> next;

        public LinkableTask(String sequence, Runnable deferredTask) {
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
                        // I can safely associate myself in the Map as the new last:
                        // other requests must get back to the Map and wait there;
                        // the only contention is with my own getNext in [B],
                        // which may have already removed the element

                        if (! queues.replace(sequence, last, this)) {
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
                deferredTask.run();

                // Check if there are other waiting tasks for this sequence;
                // assuming CPU-bound tasks, we can execute them immediately,
                // provided that we avoid starvation
                for (int i = 0; i < CPU_BOUND_BATCH; i++) {
                    last = last.getNext();
                    if (last != null) {
                        last.deferredTask.run();
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
                // I can safely remove the sequence from the Map as completed:
                // other requests must get back to the Map and wait there (state MOD);
                // the only contention is with my own attach in [A],
                // which may haven't set the element yet in the Map;
                // in this case, that mapping will no longer be needed

                queues.remove(sequence);
                return null;
            } else {
                // there is a next in the queue, so we can immediately process it,
                // even if it were still going to be associated in the Map as the last task,
                // as that is only of interest for addition operations
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

    public void execute(String sequence, Runnable task) {
        if (sequence == null) {
            // No requirement to sequentialize tasks that have no key set
            executor.execute(task);

        } else {
            // Create the task and attach it to the task queue for the sequence;
            // this will also take care of ensuring that the task will be dequeued
            LinkableTask linkableTask = new LinkableTask(sequence, task);
            linkableTask.attach();
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // Local test

    public static class Test {
        private static int threads = 50;
        private static int sequences = 10;
        private static int run = 1000;
        private static int work = 100;
        private static int pause = 100;

        private static class SequenceData {
            public final String name;
            public volatile boolean active = false;
            public volatile long count = 0;
            public volatile double data = 0;
            public SequenceData(String name) {
                this.name = name;
            }
        }

        public static void main(String[] args) {
            Multiplexer multi1 = new Multiplexer(java.util.concurrent.Executors.newFixedThreadPool(4));
            Multiplexer multi2 = new Multiplexer(java.util.concurrent.Executors.newFixedThreadPool(4));

            startMulti(multi1, "Test1");
            startMulti(multi2, "Test2");
        }

        private static void startMulti(Multiplexer multi, String name) {

            SequenceData[] sequenceData = new SequenceData[sequences];
            for (int n = 0; n < sequences; n++) {
                sequenceData[n] = new SequenceData(name + ".seq" + (n + 1));
            }

            for (int i = 0; i < threads; i++) {
                new Thread() {
                    @Override
                    public void run() {
                        submitLoop(multi, sequenceData);
                    }
                }.start();
            }
        }

        private static void submitLoop(Multiplexer multi, SequenceData[] sequenceData) {
            java.util.Random rnd = new java.util.Random();
            while (true) {
                for (int i = 0; i < run; i++) {
                    SequenceData currSequence = sequenceData[rnd.nextInt(sequences)];
                    multi.execute(currSequence.name, () -> {
                        try {
                            assert (! currSequence.active); // this is what we are testing (enable asserts on launch)
                            currSequence.active = true;
                            double tot = 0;
                            for (int n = 0; n < work; n++) {
                                tot += rnd.nextDouble();
                            }
                            currSequence.data += tot;
                            currSequence.count++;
                            if (currSequence.count % 100000 == 0) {
                                System.out.println(currSequence.name + " currently at " + currSequence.count + " with " + currSequence.data);
                            }
                            assert (currSequence.active); // this is what we are testing (enable asserts on launch)
                            currSequence.active = false;
                        } catch (RuntimeException | Error e) {
                            System.out.println("Error on " + currSequence.name + ": " + e.toString());
                        }
                    });
                }
                try {
                    Thread.sleep(pause);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }
}
