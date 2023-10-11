package com.lightstreamer.kafka_connector.adapter;

public interface Loop {

    boolean maySubscribe(String item);

    void subscribe(String item);

    void unsubscribe(String topic);

}