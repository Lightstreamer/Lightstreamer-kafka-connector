#!/bin/bash
echo "Building project and compiling JMH benchmarks..."
echo "Profile options: $PROFILE_OPTS"

# Compile JMH benchmarks
./gradlew compileJmhJava

# Expression evaluation benchmark - Tests expression parsing and evaluation performance
# ./gradlew jmh --args="ExpressionsBenchmark -wi 3 -w 3 -i 10 -r 3 -f 1 -t 1 -p params=1,3,5 ${PROFILE_OPTS}""

# Item name construction from single element - Benchmarks building item names from a single data element and schema name
# ./gradlew jmh --args="DataBenchmarks.buildItemNameSingle -wi 3 -w 3 -i 5 -r 10 -f 1 -t 1 -p params=1 ${PROFILE_OPTS}"

# Item name construction from array - Benchmarks building item names from multiple data elements in an array
# ./gradlew jmh --args="DataBenchmarks.buildItemNameArray -wi 3 -w 3 -i 10 -r 3 -f 1 -t 1 -p params=1,3,5 ${PROFILE_OPTS}"

# Canonical item extraction from Protobuf - Benchmarks extracting canonical item names from Protobuf-formatted Kafka records
# ./gradlew jmh --args="DataExtractorBenchmarks.extractAsCanonicalItemProtoBuf* -wi 3 -w 5 -i 10 -r 3 -f 2 -t 1 -p numOfTemplateParams=1,2,3 ${PROFILE_OPTS}"

# Map creation from Protobuf - Benchmarks extraction of Protobuf records into new Map structures
# ./gradlew jmh --args="DataExtractorBenchmarks.extractAsMapProtoBuf* -wi 5 -w 5 -i 10 -r 3 -f 2 -t 1 -p numOfTemplateParams=1 ${PROFILE_OPTS}"

# Map population from Protobuf - Benchmarks extraction of Protobuf records into pre-existing Map structures
# ./gradlew jmh --args="DataExtractorBenchmarks.extractIntoMapProtoBuf* -wi 5 -w 5 -i 10 -r 3 -f 2 -t 1 -p numOfTemplateParams=1 ${PROFILE_OPTS}"

# Map creation from JSON - Benchmarks extraction of JSON records into new Map structures
# ./gradlew jmh --args="DataExtractorBenchmarks.extractAsMapJson* -wi 5 -w 5 -i 10 -r 3 -f 2 -t 1 -p numOfTemplateParams=1 ${PROFILE_OPTS}"

# Map population from JSON - Benchmarks extraction of JSON records into pre-existing Map structures
# ./gradlew jmh --args="DataExtractorBenchmarks.extractIntoMapJson* -wi 5 -w 5 -i 10 -r 3 -f 2 -t 1 -p numOfTemplateParams=1 ${PROFILE_OPTS}"

# Record mapping from Protobuf - Benchmarks the complete mapping of Protobuf-formatted Kafka records to MappedRecord objects
# ./gradlew jmh --args="RecordMapperBenchmarks.measureMapWithProtobuf -wi 3 -w5 -i 10 -f 1 -t 1 ${PROFILE_OPTS}"

# Record mapping and field extraction from Protobuf - Benchmarks the complete mapping workflow including field extraction into Maps
# ./gradlew jmh --args="RecordMapperBenchmarks.measureRouteWithProtobuf -wi 3 -w5 -i 10 -f 1 -t 1 ${PROFILE_OPTS}"

# End-to-end record consumption from Protobuf - Benchmarks complete consumption workflow from records to subscriber notifications
# ./gradlew jmh --args="RecordConsumerBenchmark.consumeWithProtobuf -wi 3 -w 3  -i 10 -r 3 -f 1 -t 1 -p numOfRecords=1000,10000 -p numOfSubscriptions=1000,2000 -p partitions=2,4 -p threads=1,2,4 ${PROFILE_OPTS}"
