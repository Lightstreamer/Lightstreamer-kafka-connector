
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

import com.lightstreamer.adapters.remote.ItemEventListener;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;

class RemoteEventListener implements EventListener {

    private final ItemEventListener listener;

    RemoteEventListener(ItemEventListener listener) {
        this.listener = listener;
    }

    @Override
    public void update(
            SubscribedItem item, java.util.Map<String, String> updates, boolean isSnapshot) {
        listener.update(item.asCanonicalItemName(), updates, isSnapshot);
    }

    @Override
    public void clearSnapshot(SubscribedItem sub) {
        listener.clearSnapshot(sub.asCanonicalItemName());
    }

    @Override
    public void endOfSnapshot(SubscribedItem sub) {
        listener.endOfSnapshot(sub.asCanonicalItemName());
    }

    @Override
    public void failure(Exception exception) {
        listener.failure(exception);
    }
}
