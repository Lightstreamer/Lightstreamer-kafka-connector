package com.lightstreamer.kafka_connector.adapter;

public interface Loop {

    void trySubscribe(String item, Object itemHandle);

    void unsubscribe(String topic);

}