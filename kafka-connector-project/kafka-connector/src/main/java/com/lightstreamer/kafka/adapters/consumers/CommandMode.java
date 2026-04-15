
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

package com.lightstreamer.kafka.adapters.consumers;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility interface for COMMAND mode event decoration. Provides methods and constants for adding
 * command semantics ({@code ADD}, {@code DELETE}, {@code UPDATE}, {@code CS}, {@code EOS}) to
 * Lightstreamer events derived from Kafka records.
 *
 * <p>Used by both the real-time record processing pipeline ({@code RecordConsumerSupport}) and the
 * snapshot consumer ({@code TopicSnapshotConsumer}) to ensure consistent event schema across
 * delivery paths.
 */
public interface CommandMode {

    static final String SNAPSHOT = "snapshot";

    static Map<String, String> decorate(Map<String, String> event, Command command) {
        event.put(Key.COMMAND.key(), command.toString());
        return event;
    }

    static Map<String, String> deleteEvent(Map<String, String> event) {
        // Creates a new event with only the key field: all other fields are discarded because
        // they are not relevant for the deletion operation.
        Map<String, String> deleteEvent = new HashMap<>();
        deleteEvent.put(Key.KEY.key(), Key.KEY.lookUp(event));

        // Decorate the event with DELETE command
        return decorate(deleteEvent, Command.DELETE);
    }

    enum Command {
        ADD,
        DELETE,
        UPDATE,
        CS,
        EOS;

        static Map<String, Command> CACHE =
                Stream.of(values())
                        .collect(Collectors.toMap(Command::toString, Function.identity()));

        public static Optional<Command> lookUp(Map<String, String> input) {
            String command = input.get(Key.COMMAND.key());
            return Optional.ofNullable(CACHE.get(command));
        }

        public boolean isSnapshot() {
            return this.equals(CS) || this.equals(EOS);
        }
    }

    enum Key {
        KEY("key"),
        COMMAND("command");

        private final String key;

        Key(String key) {
            this.key = key;
        }

        public String lookUp(Map<String, String> input) {
            return input.get(key);
        }

        public String key() {
            return key;
        }
    }
}
