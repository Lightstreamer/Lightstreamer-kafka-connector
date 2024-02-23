///usr/bin/env jbang "$0" "$@" ; exit $?
//REPOS central,confluent=https://packages.confluent.io/maven
//DEPS org.apache.kafka:kafka-clients:7.5.3-ccs
//DEPS io.confluent:kafka-avro-serializer:7.5.3
//DEPS org.slf4j:slf4j-api:2.0.10
//DEPS org.slf4j:slf4j-reload4j:2.0.10
//DEPS info.picocli:picocli:4.7.5
//SOURCES producer/Producer.java


import producer.Producer;
import picocli.CommandLine;

class kafka_producer  {

    public static void main(String... args) {
        new CommandLine(new Producer()).execute(args);
    }
}
