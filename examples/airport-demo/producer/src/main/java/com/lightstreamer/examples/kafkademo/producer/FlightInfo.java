package com.lightstreamer.examples.kafkademo.producer;

import com.fasterxml.jackson.annotation.JsonProperty;

public class FlightInfo {

    @JsonProperty
    public String destination;

    @JsonProperty
    public String departure;

    @JsonProperty
    public String flightNo;

    @JsonProperty
    public int terminal;

    @JsonProperty
    public String status;

    @JsonProperty
    public String airline;

    @JsonProperty
    public String currentTime;

    @JsonProperty
    public String command;

    public FlightInfo() {
    }

    public FlightInfo(String destination, String departure, String flightNo, int terminal, String status,
            String airline) {
        this.destination = destination;
        this.departure = departure;
        this.flightNo = flightNo;
        this.terminal = terminal;
        this.status = status;
        this.airline = airline;

        this.currentTime = "";
        this.command = "UPDTAE";
    }

}
