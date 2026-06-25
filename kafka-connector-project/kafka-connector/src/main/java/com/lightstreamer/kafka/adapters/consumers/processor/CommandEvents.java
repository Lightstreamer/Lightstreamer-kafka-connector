
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

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility interface for COMMAND mode event decoration. Provides methods and constants for adding
 * command semantics ({@link Command#ADD}, {@link Command#DELETE}, {@link Command#UPDATE}) to
 * Lightstreamer events derived from Kafka records.
 *
 * <p>Used by both the real-time record processing pipeline ({@link RecordConsumerSupport}) and the
 * snapshot strategies to ensure consistent event schema across delivery paths.
 */
public interface CommandEvents {

    /**
     * Decorates an event map with the given {@link Command}.
     *
     * @param event the event map to decorate
     * @param command the command to attach
     * @return the same event map, now decorated with the command
     */
    static Map<String, String> decorate(Map<String, String> event, Command command) {
        event.put(Key.COMMAND.key(), command.toString());
        return event;
    }

    /** Commands that can be attached to Lightstreamer events for COMMAND mode subscriptions. */
    enum Command {
        ADD,
        DELETE,
        UPDATE;

        private static final Map<String, Command> CACHE =
                Stream.of(values())
                        .collect(Collectors.toMap(Command::toString, Function.identity()));

        /**
         * Looks up the {@code Command} from the command field in the given map.
         *
         * @param input the field map to inspect
         * @return the matching {@code Command}, or an empty {@link Optional} if not found
         */
        public static Optional<Command> lookUp(Map<String, String> input) {
            String command = input.get(Key.COMMAND.key());
            return Optional.ofNullable(CACHE.get(command));
        }
    }

    /** Keys used to locate command mode fields within an event map. */
    enum Key {
        KEY("key"),
        COMMAND("command");

        private final String key;

        Key(String key) {
            this.key = key;
        }

        /**
         * Retrieves the value associated with this key from the given map.
         *
         * @param input the field map to look up
         * @return the value, or {@code null} if not present
         */
        public String lookUp(Map<String, String> input) {
            return input.get(key);
        }

        /**
         * Returns the string key used for map lookups.
         *
         * @return the key string
         */
        public String key() {
            return key;
        }
    }
}
