
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

import com.fasterxml.jackson.databind.JsonNode;

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

    /**
     * Creates an exception for a named field that is missing, blank or of the wrong type.
     *
     * @param field the JSON field name
     * @param reason what was expected and, where useful, what was found
     * @return a new {@code InvalidMessageException}
     */
    public static InvalidMessageException invalidField(String field, String reason) {
        return new InvalidMessageException("Invalid \"%s\" field: %s".formatted(field, reason));
    }

    /**
     * Creates an exception for a message whose {@code key} and {@code value} are both absent.
     *
     * @return a new {@code InvalidMessageException}
     */
    public static InvalidMessageException missingKeyAndValue() {
        return new InvalidMessageException(
                "Invalid message: both \"key\" and \"value\" are missing");
    }

    /**
     * Creates an exception for a JSON node whose type does not match the configured evaluator type.
     * The field name is not known at this level and is added by the caller through {@link
     * #invalidField(String, String)}.
     *
     * @param node the offending node
     * @param expected a short description of the expected type
     * @return a new {@code InvalidMessageException}
     */
    public static InvalidMessageException unexpectedType(JsonNode node, String expected) {
        return new InvalidMessageException(
                "expected %s but found %s".formatted(expected, node.getNodeType()));
    }

    /**
     * Creates an exception for a textual node that is not a valid {@link java.util.UUID}.
     *
     * @param text the offending text
     * @return a new {@code InvalidMessageException}
     */
    public static InvalidMessageException invalidUuid(String text) {
        return new InvalidMessageException(
                "expected a valid UUID but found \"%s\"".formatted(text));
    }
}
