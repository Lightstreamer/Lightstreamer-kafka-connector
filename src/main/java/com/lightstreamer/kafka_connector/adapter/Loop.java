package com.lightstreamer.kafka_connector.adapter;

public interface Loop {

    void subscribe(String topic);

    void unsubscribe(String topic);

}