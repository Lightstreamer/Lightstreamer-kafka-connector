
///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS com.lightstreamer:ls-javase-client:5.0.0
//DEPS info.picocli:picocli:4.7.5
//SOURCES consumer/


import consumer.LsClient;
import picocli.CommandLine;

class lsclient {
    public static void main(String... args) {
        new CommandLine(new LsClient()).execute(args);
    }
}
