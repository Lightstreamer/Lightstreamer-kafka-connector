package com.lightstreamer.kafka_connector.adapter.commons;

public interface MetadataListener {

    void disableAdapter();

    void forceUnsubscription(String item);
    
}
