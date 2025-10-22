#!/bin/bash

echo "Building project and compiling JMH benchmarks..."

# Compile JMH benchmarks
./gradlew compileJmhJava

# Expression evaluation benchmark - Tests expression parsing and evaluation performance
# ./gradlew jmh --args="ExpressionsBenchmark -wi 3 -w 3 -i 10 -r 3 -f 1 -t 1 -p params=1,3,5"

# Single item name building benchmark - Measures performance of building individual item names
# ./gradlew jmh --args="DataBenchmarks.buildItemNameSingle -wi 3 -w 3 -i 5 -r 10 -f 1 -t 1 -p params=1"

# Array item name building benchmark - Tests performance of building multiple item names in arrays
# ./gradlew jmh --args="DataBenchmarks.buildItemNameArray -wi 3 -w 3 -i 10 -r 3 -f 1 -t 1 -p params=1,3,5"

# Data extraction benchmark - Measures Protobuf data extraction performance with varying template parameters
# ./gradlew jmh --args="DataExtractorBenchmarks.extractAsCanonicalItemProtoBuf* -wi 3 -w 5 -i 10 -r 3 -f 2 -t 1 -p numOfTemplateParams=1,2,3"

# Record mapping benchmark - Tests Protobuf record mapping performance
# ./gradlew jmh --args="RecordMapperBenchmarks.measureMapWithProtobuf -wi 3 -w5 -i 10 -f 1 -t 1"

# Record routing benchmark - Measures Protobuf record routing performance
# ./gradlew jmh --args="RecordMapperBenchmarks.measureRouteWithProtobuf -wi 3 -w5 -i 10 -f 1 -t 1"

# Consumer processing benchmark - Tests end-to-end Protobuf consumption with configurable load parameters
# ./gradlew jmh --args="RecordConsumerBenchmark.consumeWithProtobuf -wi 3 -w 3  -i 10 -r 3 -f 1 -t 1 -p numOfRecords=1000,10000 -p numOfSubscriptions=1000,2000 -p partitions=2,4 -p threads=1,2,4"
