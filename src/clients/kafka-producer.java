///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS org.apache.kafka:kafka-clients:3.3.1
//DEPS org.slf4j:slf4j-api:2.0.10
//DEPS org.slf4j:slf4j-reload4j:2.0.10
//DEPS info.picocli:picocli:4.7.5
//SOURCES producer/Producer.java

import picocli.CommandLine;
import producer.Producer;

class kafka_producer  {

    public static void main(String... args) {
        new CommandLine(new Producer()).execute(args);
    }
}
