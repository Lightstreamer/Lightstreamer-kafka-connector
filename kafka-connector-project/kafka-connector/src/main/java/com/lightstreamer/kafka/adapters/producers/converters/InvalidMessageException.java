
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

package com.lightstreamer.kafka.adapters.producers.converters;

/**
 * Signals that a JSON message sent by a client cannot be published because it is malformed or
 * violates the expected structure or field types.
 *
 * <p>Instances are created only through the static factories, which own the wording of every
 * client-facing message so that all rejections share a consistent shape.
 */
public class InvalidMessageException extends Exception {

    private InvalidMessageException(String message) {
        super(message, null, false, false); // No stack trace: the message is the diagnosis.
    }

    /**
     * Creates an exception for a message that is not syntactically valid JSON.
     *
     * @param reason the parser's description of the problem
     * @return a new {@code InvalidMessageException}
     */
    public static InvalidMessageException malformedJson(String reason) {
        return new InvalidMessageException("Malformed JSON: " + reason);
    }

    public static InvalidMessageException missingValue() {
        return new InvalidMessageException("Invalid message: \"value\" is missing");
    }

    public static InvalidMessageException invalidDestination(String destination) {
        return new InvalidMessageException(
                "Invalid message: no topics found for destination: " + destination);
    }
}
