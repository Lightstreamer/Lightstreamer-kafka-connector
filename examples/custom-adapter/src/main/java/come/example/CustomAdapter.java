package com.example;

import java.io.File;
import java.util.Map;

import com.lightstreamer.kafka_connector.adapters.pub.KafkaConnectorMetadataAdapter;

public class CustomAdapter extends KafkaConnectorMetadataAdapter {

    @Override
    public void doInit(Map parameters, File adapterDir) {
        System.out.println("Hello from CustomAdapter");
    }
}
