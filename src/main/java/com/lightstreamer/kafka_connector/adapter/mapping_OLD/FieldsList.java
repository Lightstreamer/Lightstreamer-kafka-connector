package com.lightstreamer.kafka_connector.adapter.mapping_OLD;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class FieldsList {

    private Map<String, String> fields = new HashMap<>();

    public FieldsList(Map<String, String> fields) {
        this.fields = fields;
    }

    public Map<String, String> map() {
        return Collections.unmodifiableMap(fields);
    }

}
