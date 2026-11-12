
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
import com.lightstreamer.kafka.adapters.config.GlobalConfig;
import com.lightstreamer.kafka.adapters.pub.messages.MessageHook;

import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;

/** A handler for sending messages to the Kafka producer. */
public interface MessageHandler {

    /** Error codes reported to the client through {@link CreditsException}. */
    enum ErrorCode {
        PRODUCER_DISABLED(-1),
        MESSAGE_REJECTED(-2);

        private final int code;

        ErrorCode(int code) {
            this.code = code;
        }

        public int code() {
            return code;
        }
    }

    CompletableFuture<String> sendMessage(String user, String sessionID, String message);

    void close();

    static MessageHandler create(Logger logger, GlobalConfig globalConfig, MessageHook hook) {
        return new MessagePublisher(logger, globalConfig, hook);
    }

    static MessageHandler disabled() {
        return new DisabledMessageHandler();
    }
}
