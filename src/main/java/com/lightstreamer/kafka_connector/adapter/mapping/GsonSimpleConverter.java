package com.lightstreamer.kafka_connector.adapter.mapping;

import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class GsonSimpleConverter implements DataConverter {

    private Gson gson;

    public GsonSimpleConverter() {
        this.gson = new Gson();
    }

    @Override
    public Map<String, String> convert(byte[] data) {
        TypeToken<Map<String, String>> mapType = new TypeToken<Map<String, String>>() {
        };
        return gson.fromJson(new String(data), mapType);

    }
}
