
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

import com.lightstreamer.client.ItemUpdate;
import com.lightstreamer.client.LightstreamerClient;
import com.lightstreamer.client.Subscription;
import com.lightstreamer.client.SubscriptionListener;

import picocli.CommandLine.Option;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class LsClient implements Runnable {

    @Option(
            names = "--address",
            description = "The Lightstreamer server address",
            defaultValue = "http://localhost:8080")
    private String host;

    @Option(
            names = "--adapter-set",
            description = "The name of the target Adapter Set",
            defaultValue = "KAFKA")
    private String adapterSet;

    @Option(
            names = "--data-adapter",
            description = "The name of the target Data Adapter",
            defaultValue = "DEFAULT")
    private String dataAdapter;

    @Option(
            names = "--subscription-mode",
            description = "The subcription mode",
            defaultValue = "RAW")
    private String subscriptionMode;

    @Option(
            names = "--items",
            split = ",",
            description = "The items to subscribe to ",
            required = true)
    private String[] items;

    @Option(
            names = "--fields",
            split = ",",
            description = "The fields to subscribe to ",
            required = true)
    private String[] fields;

    @Override
    public void run() {
        LightstreamerClient client = new LightstreamerClient(this.host, this.adapterSet);
        client.addListener(Listeners.clientListener());
        client.connect();

        Consumer<ItemUpdate> cons =
                obj -> {
                    String update =
                            Arrays.stream(fields)
                                    .map(
                                            field ->
                                                    String.format(
                                                            "[%s] = <%s>",
                                                            field, obj.getValue(field)))
                                    .collect(Collectors.joining(","));
                    System.out.printf("Received: %s%n", obj.getFields());
                };
        SubscriptionListener listener = Listeners.subscriptionListener(cons);
        for (String item : this.items) {
            Subscription sub = new Subscription(this.subscriptionMode, item, this.fields);
            sub.setDataAdapter(dataAdapter);
            sub.addListener(listener);
            client.subscribe(sub);
        }
    }
}
