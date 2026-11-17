
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

import static com.google.common.truth.Truth.assertThat;

import com.lightstreamer.kafka.adapters.config.PublisherConfig;
import com.lightstreamer.kafka.adapters.pub.messages.ClientMessage;
import com.lightstreamer.kafka.adapters.pub.messages.ClientMessageInfo;
import com.lightstreamer.kafka.adapters.pub.messages.MessageHook;
import com.lightstreamer.kafka.adapters.pub.messages.PublishingException;
import com.lightstreamer.kafka.adapters.pub.messages.converters.PayloadConverter;

import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

public class MessageHandlerTest {

    @Test
    void shouldCreateDisabledMessageHandler() {
        MessageHandler handler = MessageHandler.disabled();
        assertThat(handler).isInstanceOf(DisabledMessageHandler.class);
    }

    @Test
    void shouldCreatedMessageHandler() {
        MessageHandler handler =
                MessageHandler.create(
                        LoggerFactory.getLogger("Publisher"),
                        PublisherConfig.defaultConfig(),
                        new MessageHook() {

                            @Override
                            public ClientMessage onMessagePublishing(ClientMessageInfo messageInfo)
                                    throws PublishingException {
                                throw new UnsupportedOperationException(
                                        "Unimplemented method 'onMessagePublishing'");
                            }

                            @Override
                            public PayloadConverter createKeyConverter() {
                                throw new UnsupportedOperationException(
                                        "Unimplemented method 'createKeyConverter'");
                            }

                            @Override
                            public PayloadConverter createValueConverter() {
                                throw new UnsupportedOperationException(
                                        "Unimplemented method 'createValueConverter'");
                            }
                        });
        assertThat(handler).isInstanceOf(MessagePublisher.class);
    }
}
