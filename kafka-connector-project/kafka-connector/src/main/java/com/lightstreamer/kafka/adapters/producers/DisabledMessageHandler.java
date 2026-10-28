
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

import java.util.concurrent.CompletableFuture;

class DisabledMessageHandler implements MessageHandler {

    @Override
    public CompletableFuture<String> sendMessage(String message) throws CreditsException {
        throw new CreditsException(
                ErrorMessages.DISABLED_PRODUCER.getCode(),
                ErrorMessages.DISABLED_PRODUCER.getMessage());
    }

    @Override
    public void close() {
        // No-op
    }
}
