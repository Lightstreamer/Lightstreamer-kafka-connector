
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

package com.lightstreamer.kafka.utils.producer;

import com.lightstreamer.client.ClientMessageListener;
import com.lightstreamer.client.LightstreamerClient;
import com.lightstreamer.kafka.utils.common.Listeners;

import picocli.CommandLine.Option;

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

    @Option(names = "--message", description = "The message to send")
    private String message;

    @Override
    public void run() {
        LightstreamerClient client = new LightstreamerClient(this.host, this.adapterSet);

        client.addListener(Listeners.clientListener());
        client.connect();

        client.sendMessage(
                message,
                "seq",
                0,
                new ClientMessageListener() {

                    @Override
                    public void onAbort(String originalMessage, boolean sentOnNetwork) {
                        System.out.println(
                                "Message aborted: "
                                        + originalMessage
                                        + ", sent on network: "
                                        + sentOnNetwork);
                    }

                    @Override
                    public void onDeny(String originalMessage, int code, String error) {
                        System.out.println(
                                "Message denied: "
                                        + originalMessage
                                        + ", code: "
                                        + code
                                        + ", error: "
                                        + error);
                        System.exit(code);
                    }

                    @Override
                    public void onDiscarded(String originalMessage) {
                        System.out.println("Message discarded: " + originalMessage);
                    }

                    @Override
                    public void onError(String originalMessage) {
                        System.out.println("Message error: " + originalMessage);
                    }

                    @Override
                    public void onProcessed(String originalMessage, String response) {
                        System.out.println(
                                "Message processed: "
                                        + originalMessage
                                        + ", response: "
                                        + response);
                    }
                },
                false);
    }
}
