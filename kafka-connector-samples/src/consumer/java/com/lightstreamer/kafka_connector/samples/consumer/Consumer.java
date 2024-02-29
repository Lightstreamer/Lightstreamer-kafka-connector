package com.lightstreamer.kafka_connector.samples.consumer;

import picocli.CommandLine;

public class Consumer {
    public static void main(String... args) {
        new CommandLine(new LsClient()).execute(args);
    }
}
