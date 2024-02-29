
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

package com.lightstreamer.kafka_connector.samples.consumer;

import com.lightstreamer.client.ClientListener;
import com.lightstreamer.client.ItemUpdate;
import com.lightstreamer.client.SubscriptionListener;

import java.util.function.Consumer;

public class Listeners {

    private static class SystemOutClientListener implements ClientListener {

        @Override
        public void onListenEnd() {
            System.out.println("Stops listening to client events");
        }

        @Override
        public void onListenStart() {
            System.out.println("Start listening to client events");
        }

        @Override
        public void onPropertyChange(String property) {
            System.out.println("Client property changed: " + property);
        }

        @Override
        public void onServerError(int code, String message) {
            System.out.println("Server error: " + code + ": " + message);
        }

        @Override
        public void onStatusChange(String newStatus) {
            System.out.println("Connection status changed to " + newStatus);
        }
    }

    public static ClientListener clientListener() {
        return new SystemOutClientListener();
    }

    private static class MySubscriptiontListener implements SubscriptionListener {

        private final Consumer<ItemUpdate> consumer;

        MySubscriptiontListener(Consumer<ItemUpdate> itemUpdateConsumer) {
            this.consumer = itemUpdateConsumer;
        }

        @Override
        public void onClearSnapshot(String itemName, int itemPos) {
            System.out.println("Server has cleared the current status of the chat");
        }

        @Override
        public void onCommandSecondLevelItemLostUpdates(int lostUpdates, String key) {}

        @Override
        public void onCommandSecondLevelSubscriptionError(int code, String message, String key) {}

        @Override
        public void onEndOfSnapshot(String arg0, int arg1) {
            System.out.println(
                    "Snapshot is now fully received, from now on only real-time messages will be"
                            + " received");
        }

        @Override
        public void onItemLostUpdates(String itemName, int itemPos, int lostUpdates) {
            System.out.println(lostUpdates + " messages were lost");
        }

        @Override
        public void onItemUpdate(ItemUpdate update) {
            consumer.accept(update);
        }

        @Override
        public void onListenEnd() {
            System.out.println("Stop listeneing to subscription events");
        }

        @Override
        public void onListenStart() {
            System.out.println("Start listeneing to subscription events");
        }

        @Override
        public void onSubscription() {
            System.out.println("Now subscribed, messages will now start coming in");
        }

        @Override
        public void onSubscriptionError(int code, String message) {
            System.out.println("Cannot subscribe because of error " + code + ": " + message);
        }

        @Override
        public void onUnsubscription() {
            System.out.println(
                    "Now unsubscribed from demo item, no more messages will be received");
        }

        @Override
        public void onRealMaxFrequency(String frequency) {
            System.out.println("Frequency is " + frequency);
        }
    }

    public static SubscriptionListener subscriptionListener(Consumer<ItemUpdate> consumer) {
        return new MySubscriptiontListener(consumer);
    }
}
