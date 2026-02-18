#!/bin/bash
echo "Building project and compiling JMH benchmarks..."
echo "Profile options: $PROFILE_OPTS"

# Compile JMH benchmarks
./gradlew clean compileJmhJava

# Expression parsing and item name conversion - Benchmarks subscription expression parsing and canonical item name generation
# ./gradlew jmh --args="ExpressionsBenchmark -wi 3 -w 3 -i 10 -r 3 -f 1 -t 1 -p params=1,3,5 ${PROFILE_OPTS}"

# Item name construction - Benchmarks building item names from single and multiple data elements
# ./gradlew jmh --args="DataBenchmarks -wi 3 -w 3 -i 5 -r 10 -f 1 -t 1 -p params=1 ${PROFILE_OPTS}"

# Data extraction and canonical item names - Benchmarks field extraction and canonical item name extraction from records
# ./gradlew jmh --args="DataExtractorBenchmarks -wi 3 -w 5 -i 10 -r 3 -f 1 -t 1 -p numOfTemplateParams=1,2,3 ${PROFILE_OPTS}"

# Record mapping and field routing - Benchmarks complete mapping workflow including field extraction and subscription routing
# ./gradlew jmh --args="RecordMapperBenchmarks -wi 3 -w 5 -i 10 -r 5 -f 1 -t 1 -p type=JSON ${PROFILE_OPTS}"  

# End-to-end record consumption - Benchmarks complete workflow from record consumption through mapping to subscriber notification
# ./gradlew jmh --args="RecordConsumerBenchmark.pollPriceInfo -wi 5 -w 5 -i 10 -r 5 -f 1 -t 1 -p numOfRecords=500 -p numOfSubscriptions=500 -p partitions=1 -p ordering=ORDER_BY_KEY -p threads=2,4,6 ${PROFILE_OPTS}"
