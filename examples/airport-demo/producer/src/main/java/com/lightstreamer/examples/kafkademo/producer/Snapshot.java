package com.lightstreamer.examples.kafkademo.producer;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Snapshot {
    @JsonProperty
    public String command;

    public Snapshot(String command) {
        this.command = command;
    }
}
