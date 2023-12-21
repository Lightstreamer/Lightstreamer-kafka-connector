package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import java.util.Map;

public interface Value extends ValueSchema {

    String text();

    default boolean isContainer() {
        return false;
    }

    static Value of(String name, String text) {
        return new SimpleValue(name, text);
    }

    static Value of(Map.Entry<String,String> entry) {
        return of(entry.getKey(), entry.getValue());
    }

    default boolean matches(Value other) {
        return name().equals(other.name()) && text().equals(other.text());
    }

}

class SimpleValue implements Value {

    private final String value;

    private final String name;

    public SimpleValue(String name, String value) {
        this.name = name;
        this.value = value;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String text() {
        return value;
    }

    public String toString() {
        return String.format("name=<%s>,value=<%s>", name, value);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SimpleValue other = (SimpleValue) obj;
        if (value == null) {
            if (other.value != null)
                return false;
        } else if (!value.equals(other.value))
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        return true;
    }

}
