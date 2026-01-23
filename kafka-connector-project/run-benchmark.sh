#!/bin/bash
echo "Building project and compiling JMH benchmarks..."
echo "Profile options: $PROFILE_OPTS"

# Compile JMH benchmarks
./gradlew clean compileJmhJava

# # Expression parsing and item name conversion - Evaluates performance of subscription expression parsing and canonical item name generation
./gradlew jmh --args="ExpressionsBenchmark -wi 3 -w 3 -i 10 -r 3 -f 1 -t 1 -p params=1,3,5 ${PROFILE_OPTS}"

# Item name construction - Tests performance of building item names from single and multiple data elements
./gradlew jmh --args="DataBenchmarks -wi 3 -w 3 -i 5 -r 10 -f 1 -t 1 -p params=1 ${PROFILE_OPTS}"

# Data extraction and canonical item names - Measures field extraction and canonical item name extraction from records
./gradlew jmh --args="DataExtractorBenchmarks -wi 3 -w 5 -i 10 -r 3 -f 1 -t 1 -p numOfTemplateParams=1,2,3 ${PROFILE_OPTS}"

# Record mapping and field routing - Tests complete mapping workflow including field extraction and subscription routing
./gradlew jmh --args="RecordMapperBenchmarks -wi 3 -w5 -i 10 -r 5 -f 1 -t 1 ${PROFILE_OPTS}"

# End-to-end record consumption - Benchmarks complete workflow from record consumption through mapping to subscriber notification
./gradlew jmh --args="RecordConsumerBenchmark -wi 5 -w 5 -i 10 -r 5 -f 1 -t 1 -p numOfRecords=500 -p numOfSubscriptions=20 -p partitions=2 -p threads=1,2,4 ${PROFILE_OPTS}"

