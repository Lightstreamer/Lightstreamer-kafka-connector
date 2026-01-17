
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

import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;

public interface TaskExecutor<S> {

    void shutdown();

    void execute(S sequence, Runnable task);

    default <E> void execute(E event, S sequence, BiConsumer<? super S, ? super E> task) {
        execute(sequence, () -> task.accept(sequence, event));
    }

    static <S> TaskExecutor<S> create(ExecutorService pool) {
        return new Multiplexer<>(pool, false);
    }
}
