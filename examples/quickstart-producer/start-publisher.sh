#!/bin/bash
set -ex
java -jar build/libs/quickstart-producer-all.jar --bootstrap-servers pkc-z9doz.eu-west-1.aws.confluent.cloud:9092 --topic stocks --config-file producer.properties
