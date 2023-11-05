package com.lightstreamer.kafka_connector.adapter.config;

interface A1<T> {

}

public abstract sealed class A<T> implements A1<T> permits B {
    
}
