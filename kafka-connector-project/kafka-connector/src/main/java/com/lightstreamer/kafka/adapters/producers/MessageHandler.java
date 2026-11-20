
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

package com.lightstreamer.kafka.adapters.producers;

import com.lightstreamer.interfaces.metadata.CreditsException;
import com.lightstreamer.kafka.adapters.config.PublisherConfig;
import com.lightstreamer.kafka.adapters.pub.messages.MessageHook;

import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;

/** A handler for sending messages to the Kafka producer. */
public interface MessageHandler {

    /**
     * Error codes and their client-facing messages, reported to the client through {@link
     * CreditsException}.
     *
     * <p>Each constant carries a fixed message. Some failures also attach a detail message, which
     * is exposed to the client together with the fixed one (see {@link
     * #toCreditsException(String)}).
     */
    enum ClientError {

        /** Message publishing is disabled in the configuration. */
        PUBLISHING_DISABLED(-1, "Message publishing disabled"),

        /** The client message could not be parsed. */
        MALFORMED_MESSAGE(-2, "Malformed message"),

        /**
         * The message hook rejected the message. The detail is the message of the exception thrown
         * by the hook.
         */
        MESSAGE_HOOK_FAILED(-3, "Message hook failed"),

        /** The client message has no destination, or the destination is blank. */
        MISSING_DESTINATION(-4, "Missing destination"),

        /** The destination is not mapped to any topic. */
        UNMAPPED_DESTINATION(-5, "Destination not mapped"),

        /** The client message has no value. */
        MISSING_PAYLOAD_VALUE(-6, "Missing payload value"),

        /**
         * The key or the value cannot be serialized. The detail is the message of the serialization
         * error.
         */
        INVALID_PAYLOAD_FORMAT(-7, "Invalid payload format"),

        /**
         * The producer reported the failure asynchronously, through the send callback, after the
         * message was accepted for sending. It is most likely caused by the broker or the network.
         */
        BROKER_DELIVERY_FAILED(-8, "Broker delivery failed"),

        /**
         * The failure was raised synchronously while sending the message (for example, a closed
         * producer or an interrupted thread), and is not attributable to the broker.
         */
        SEND_FAILED(-9, "Message send failed");

        private final int code;
        private final String message;

        ClientError(int code, String message) {
            this.code = code;
            this.message = message;
        }

        /**
         * Returns the numeric error code.
         *
         * @return the error code, always negative
         */
        public int code() {
            return code;
        }

        /**
         * Returns the fixed client-facing message.
         *
         * @return the error message
         */
        public String message() {
            return message;
        }

        /**
         * Creates a {@link CreditsException} with this error's code and fixed message.
         *
         * @return a new {@code CreditsException}
         */
        public CreditsException toCreditsException() {
            return new CreditsException(code, message);
        }

        /**
         * Creates a {@link CreditsException} with this error's code and fixed message, plus a
         * detail message to be shown to the client.
         *
         * @param userMessage the detail message for the client
         * @return a new {@code CreditsException}
         */
        public CreditsException toCreditsException(String userMessage) {
            return new CreditsException(code, message, userMessage);
        }
    }

    CompletableFuture<String> sendMessage(String user, String sessionID, String message);

    void close();

    static MessageHandler create(Logger logger, PublisherConfig config, MessageHook hook) {
        return new MessagePublisher(logger, config, hook);
    }

    static MessageHandler disabled() {
        return new DisabledMessageHandler();
    }
}
