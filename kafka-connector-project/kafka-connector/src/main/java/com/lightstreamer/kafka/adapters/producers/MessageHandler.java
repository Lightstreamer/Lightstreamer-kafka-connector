
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

import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;

public interface MessageHandler {

    enum ErrorMessages {
        DISABLED_PRODUCER(-1, "Disabled producer"),
        MALFORMED_JSON_MESSAGE(-2, "Malformed JSON message: {}"),
        REJECTED_MESSAGE(-3, "Rejected message: {}");

        private final int code;
        private final String message;

        ErrorMessages(int code, String message) {
            this.code = code;
            this.message = message;
        }

        public String getMessage() {
            return message;
        }

        public int getCode() {
            return code;
        }
    }

    CompletableFuture<String> sendMessage(String message) throws CreditsException;

    void close();

    static MessageHandler create(Logger logger, ProducerSettings settings) {
        return new DefaultMessageHandler(logger, settings);
    }

    MessageHandler DISABLED = new DisabledMessageHandler();
}
