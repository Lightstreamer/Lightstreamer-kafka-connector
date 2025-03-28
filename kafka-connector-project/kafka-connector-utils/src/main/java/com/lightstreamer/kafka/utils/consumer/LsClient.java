
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

package com.lightstreamer.kafka.utils.consumer;

import com.lightstreamer.client.ItemUpdate;
import com.lightstreamer.client.LightstreamerClient;
import com.lightstreamer.client.Subscription;
import com.lightstreamer.client.SubscriptionListener;

import picocli.CommandLine.Option;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;

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
            description = "The subscription mode",
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

    private String prepareEventRow(int firstColumnWidth, int otherColumnWidth) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < fields.length; i++) {
            int width = i == 0 ? firstColumnWidth : otherColumnWidth;
            sb.append("%s = %" + width + "s");
            if (i < fields.length - 1) {
                sb.append(", ");
            }
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public void run() {
        String eventRow = prepareEventRow(17, 7);
        LightstreamerClient client = new LightstreamerClient(this.host, this.adapterSet);

        Consumer<ItemUpdate> cons =
                obj -> {
                    try {
                        Map<Integer, String> f = obj.getFieldsByPosition();
                        List<Object> args = new ArrayList<>();
                        for (Entry<Integer, String> entry : f.entrySet()) {
                            args.add(fields[entry.getKey() - 1]);
                            args.add(entry.getValue());
                        }

                        String update = eventRow.formatted(args.toArray());
                        System.out.printf("Update: %s%n", update);
                    } catch (RuntimeException e) {
                        e.printStackTrace();
                    }
                };
        SubscriptionListener listener = Listeners.subscriptionListener(cons);
        for (String item : this.items) {
            Subscription sub = new Subscription(this.subscriptionMode, item, this.fields);
            sub.setDataAdapter(dataAdapter);
            sub.addListener(listener);
            client.subscribe(sub);
        }

        client.addListener(Listeners.clientListener());
        client.connect();
    }
}
