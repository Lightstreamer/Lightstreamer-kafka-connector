
/*
 * Copyright (C) 2025 Lightstreamer Srl
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

package com.lightstreamer.kafka.common.listeners;

import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;

import java.util.Map;

public interface EventListener {

    void update(SubscribedItem item, Map<String, String> updates, boolean isSnapshot);

    void clearSnapshot(SubscribedItem item);

    void endOfSnapshot(SubscribedItem item);

    void failure(Exception exception);

    static EventListener smartEventListener(
            com.lightstreamer.interfaces.data.ItemEventListener listener) {
        return new SmartEventListener(listener);
    }

    static EventListener legacyEventListener(
            com.lightstreamer.interfaces.data.ItemEventListener listener) {
        return new LegacyEventListener(listener);
    }

    static EventListener remoteEventListener(
            com.lightstreamer.adapters.remote.ItemEventListener listener) {
        return new RemoteEventListener(listener);
    }
}
