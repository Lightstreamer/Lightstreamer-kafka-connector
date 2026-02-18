# Changelog

## [1.4.0] (2026-02-17)

**Improvements**

- **Polling Lifecycle Optimization**: Significantly enhanced the polling lifecycle to improve throughput and minimize record lag. Introduced an adaptive commit strategy that dynamically adjusts commit frequency based on message rate. This optimization also removes previous constraints that prevented concurrent processing when using compacted topics. ([#76](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/76))

- **Monitor Logger**: Added a dedicated logger to track essential performance metrics including records received, records processed, and internal ring buffer utilization rates. ([#76](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/76))

- **Configurable Max Poll Records**: Exposed the [`record.consume.max.poll.records`](README.md#recordconsumemaxpollrecords) parameter to configure the maximum number of records fetched in each polling cycle of the internal Kafka consumer. ([#76](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/76))

- **Microbenchmarks Consolidation**: Refactored and consolidated the internal microbenchmarking suite to improve performance testing capabilities. ([#76](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/76))

**Examples and Documentation**

- Significantly reorganized the structure of examples as follows:
  - Split Confluent QuickStart into separate `quickstart-confluent-cloud/` and `quickstart-confluent-platform/` directories. ([#74](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/74))
  - Renamed `quickstart-redpanda-self-hosted` to `quickstart-redpanda-self-managed`. ([#74](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/74))
  - Removed unused web client assets and `QuickStartConfluentCloud` configuration from factory files. ([#74](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/74))
  - Updated internal links to reflect new folder locations. ([#74](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/74))

- Updated [`README.md`](README.md): Removed vendor-specific connection instructions (Confluent Cloud, Redpanda Cloud) and moved them to dedicated quickstarts. Expanded broker list with links to all vendor quickstarts. ([#74](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/74))

- Updated [`examples/vendors/confluent/README.md`](examples/vendors/confluent/README.md): Improved section organization and clarity. ([#74](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/74))

- Clarified `adapters.xml` descriptions in various QuickStart READMEs. ([#74](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/74))

**Third-Party Library Updates**

- Upgraded the _confluent-kafka_ dependency to version 8.1.1-ccs. ([#76](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/76))

**Bug Fixes**

- Fixed outdated links to official Kafka documentation in the [README.md](README.md), [examples/vendors/confluent/README.md](examples/vendors/confluent/README.md), and factory [`adapters.xml`](kafka-connector-project/kafka-connector/src/adapter/dist/adapters.xml) files. ([#76](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/76))

- Fixed heading hierarchy, TOC indentation, typos, and trailing spaces in the [README.md](README.md) and [examples/vendors/confluent/README.md](examples/vendors/confluent/README.md) files. ([#74](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/74))
 

## [1.3.2] (2026-01-26)

**Improvements**

- **Deferred Deserialization**: Refactored the deserialization pipeline to defer the conversion of raw bytes to deserialized objects. This architectural change makes the Kafka Connector open for further extension by allowing custom implementations to intercept and access raw bytes from Kafka records before or instead of automatic deserialization. ([#75](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/75))

- **Auto COMMAND Mode**: Added support for the [Auto COMMAND mode](README.md#auto-command-mode-fieldsautocommandmodeenable) feature that generates _command_ operations for Lightstreamer items without requiring explicit command fields in the Kafka records. ([#75](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/75))

- Consolidated the script for running microbenchmarks. ([#75](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/75))

- Upgraded _Gradle_ to version 9.3.0. ([#75](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/75))

**Bug Fixes**

- **Null Value Deserialization**: Fixed incorrect handling of null values during deserialization when using ProtoBuf, JSON, and Avro formats. The deserializers now properly process null payloads without triggering unexpected errors or data loss. ([#75](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/75))


## [1.3.1] (2025-12-19)

**Improvements**

- **Data Extraction Language Enhancement and Dynamic Field Discovery**: Extended the [_Data Extraction Language_](README.md#data-extraction-language) to support wildcard expressions (e.g., `#{VALUE.*}`, `#{KEY.*}`, `#{HEADERS.*}`), enabling the new [_Dynamic Field Discovery_](README.md#dynamic-field-discovery-field) mechanism with the `field.*` configuration parameter. This allows automatic mapping of Kafka record fields to Lightstreamer fields at runtime, eliminating the need to explicitly configure each field individually – especially useful for records with numerous or dynamically varying fields (also available for the Sink connector). ([#72](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/72))

- Upgraded _Gradle_ to version 9.2.1. ([#72](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/72))

**Third-Party Library Updates**

- Upgraded _JUnit_ to version 6.0.1. ([#72](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/72))
- Upgraded _Truth_ to version 1.4.5. ([#72](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/72))

**Bug Fixes**

- Fixed the [examples/quickstart-schema-registry/README.md](examples/quickstart-schema-registry/README.md) file to mention usage of Protobuf. ([#72](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/72))

- Corrected broken anchor links in the Table of Contents of the [README.md](README.md) file. ([#72](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/72))


## [1.3.0] (2025-10-21)

**Breaking Changes**

- **KafkaConnectorMetadataAdapter Class Hierarchy Change**: The [`com.lightstreamer.kafka.adapters.pub.KafkaConnectorMetadataAdapter`](https://lightstreamer.github.io/Lightstreamer-kafka-connector/javadoc/com/lightstreamer/kafka/adapters/pub/KafkaConnectorMetadataAdapter.html) class now extends [`com.lightstreamer.adapters.metadata.MetadataProviderAdapter`](https://sdk.lightstreamer.com/ls-adapter-inprocess/8.0.0/api/com/lightstreamer/interfaces/metadata/MetadataProviderAdapter.html) directly instead of [`com.lightstreamer.adapters.metadata.LiteralBasedProvider`](https://sdk.lightstreamer.com/ls-adapter-inprocess/8.0.0/api/com/lightstreamer/adapters/metadata/LiteralBasedProvider.html). This change affects any custom implementations that relied on `com.lightstreamer.adapters.metadata.LiteralBasedProvider`-specific functionality. Additionally, a new [`remapItems`](https://lightstreamer.github.io/Lightstreamer-kafka-connector/javadoc/com/lightstreamer/kafka/adapters/pub/KafkaConnectorMetadataAdapter.html#remapItems(java.lang.String,java.lang.String,java.lang.String,java.lang.String))  method has been introduced that provides a hook for custom item resolution logic. Custom metadata adapters extending `com.lightstreamer.kafka.adapters.pub.KafkaConnectorMetadataAdapter` should override the new `remapItems` method to provide custom item resolution logic, rather than overriding [`getItems`](https://sdk.lightstreamer.com/ls-adapter-inprocess/8.0.0/api/com/lightstreamer/interfaces/metadata/MetadataProvider.html#getItems(java.lang.String,java.lang.String,java.lang.String,java.lang.String)) directly. Other methods that were previously provided by [`com.lightstreamer.adapters.metadata.LiteralBasedProvider`](https://sdk.lightstreamer.com/ls-adapter-inprocess/8.0.0/api/com/lightstreamer/adapters/metadata/LiteralBasedProvider.html), such as [`getSchema`](https://sdk.lightstreamer.com/ls-adapter-inprocess/8.0.0/api/com/lightstreamer/interfaces/metadata/MetadataProvider.html#getSchema(java.lang.String,java.lang.String,java.lang.String,java.lang.String,java.lang.String)) and other metadata resolution methods, may also need to be implemented. ([#71](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/71))

**Improvements**

- **Core Algorithm Optimizations**: Enhanced overall system performance through optimizations in data processing, record mapping, and routing logic. These improvements result in faster record processing and reduced resource consumption during high-throughput scenarios. ([#71](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/71))

- **Data Access Pattern Optimization**: Optimized record routing algorithm to use more efficient data access patterns, improving throughput when delivering messages to multiple subscribed clients. ([#71](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/71))

- **Comprehensive Benchmarking Suite**: Added comprehensive performance benchmarking infrastructure to enable systematic performance monitoring and optimization of core components including data extraction, record mapping, and expression evaluation. ([#71](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/71))

- **Improved Logging**: Refined logging levels to reduce verbosity in production environments while maintaining debugging capabilities when needed. ([#71](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/71))

**Bug Fixes**

- **Consumer Shutdown Robustness**: Enhanced error handling during Kafka consumer shutdown to prevent hanging or incomplete disconnections. ([#71](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/71))

- **Configuration Cleanup**: Removed `group.id` assignment from the factory [`adapters.xml`](kafka-connector-project/kafka-connector/src/adapter/dist/adapters.xml) file. ([#71](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/71))

- **Protobuf Deserialization**: Improved robustness of Protobuf message deserialization by ensuring that the specified message type exists in the provided schema file. ([#70](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/70))

**Code Quality Improvements**

- **Import Organization**: Cleaned up unused imports across multiple files. ([#71](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/71))

- **Documentation Enhancement**: Improved inline documentation and method signatures. ([#71](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/71))

- **Code Structure**: Removed commented-out code and unnecessary blank lines for cleaner codebase. ([#71](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/71))


## [1.2.10] (2025-09-18)

**Improvements**

- Added the [Gradle-License-Report plugin](https://github.com/jk1/Gradle-License-Report) to the [project](./kafka-connector-project/buildSrc/src/main/groovy/lightstreamer-kafka-connector.gradle#L6) to automatically generate and include third-party license files in the distribution packages (including the Sink connector). ([#68](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/68))

**Bug Fixes**

- Fixed [`examples/compose-templates/log4j.properties`](examples/compose-templates/log4j.properties) file to explicitly set the logging level and output for the `com.lightstreamer.kafka.adapters.pub.KafkaConnectorMetadataAdapter` class. ([#67](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/67))

- Fixed regex pattern for semantic version matching in the [`com.lightstreamer.kafka.test_utils.VersionUtils`](kafka-connector-project/kafka-connector/src/test/java/com/lightstreamer/kafka/test_utils/VersionUtils.java) class to ensure accurate handling of all version formats in unit tests. ([#69](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/69))


## [1.2.9] (2025-08-25)

**Improvements**

- Added a [quickstart demo](examples/vendors/automq/quickstart-automq/) for [_AutoMQ_](https://www.automq.com/), a cloud-native, S3-based Kafka distribution. ([#62](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/62))

- Introduced support for [_connection inversion_](README.md#connectioninversionenable) in the Sink connector, allowing it to accept incoming connections from the Lightstreamer Broker. ([#65](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/65))

- Upgraded _Gradle_ to version 9.0.0. ([#63](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/63))

- Clarified the general format of `map.TOPIC_NAME.to` parameter in the factory [`adapters.xml`](kafka-connector-project/kafka-connector/src/adapter/dist/adapters.xml) file. ([#61](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/61))

**Bug Fixes**

- Corrected minor documentation issues in `README.md` files and improved code formatting in various source files. ([#66](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/66))

- Added missing documentation for trust store and key store types in [`README.md`](README.md) and the factory [`adapters.xml`](kafka-connector-project/kafka-connector/src/adapter/dist/adapters.xml) files. ([#64](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/64))

- Corrected the IAM role parameter examples in the factory [`adapters.xml`](kafka-connector-project/kafka-connector/src/adapter/dist/adapters.xml) file. ([#60](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/60))


## [1.2.8] (2025-07-10)

**Improvements**

- Extended the [_Data Extraction Language_](README.md#data-extraction-language) to support Headers. ([#57](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/57))

- Enhanced Protobuf support by allowing to specify [local binary descriptor files](README.md#recordkeyevaluatorschemapath-and-recordvalueevaluatorschemapath) and a [message type](README.md#recordkeyevaluatorprotobufmessagetype-and-recordvalueevaluatorprotobufmessagetype) as an alternative to using the Schema Registry. ([#59](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/59))

**Bug Fixes**

- Fixed unexpected behaviors that occurred when processing Kafka records with null payloads. ([#58](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/58))


## [1.2.7] (2025-06-05)

**Improvements**

- Added [IAM authentication support](README.md#aws_msk_iam) for Amazon MSK clusters, including updates to documentation, examples, and configuration files. ([#56](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/56))


## [1.2.6] (2025-05-27)

**Improvements**

- Updated the [Producer for the QuickStart App](examples/quickstart-producer/) example project to produce a Docker image which supports versioned JAR files. ([#52](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/52))

**Bug Fixes**

- Updated the [Airport demo](examples/airport-demo/) example to load the Lightstreamer library from _https_. ([#53](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/53))

- Fixed consumer loop blockage that occurred when errors happened during the commit stage while being notified about partition revocation. ([#54](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/54))

- Fixed indefinite growth of the committed metadata when using log compaction with the Connector configured for concurrent processing (also clarified in the [`README.md`](README.md) file). ([#54](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/54))

- Updated the comments in the factory [`adapters.xml`](kafka-connector-project/kafka-connector/src/adapter/dist/adapters.xml) file to clarify available deserialization formats. Corresponding changes also applied to the [`README.md`](README.md) file to maintain consistency in documentation. ([#55](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/55))


## [1.2.5] (2025-04-16)

**Improvements**

- Added support for [Protocol Buffer format](README.md#protocol-buffer-format) for message deserialization, with updates to the [Producer for the QuickStart App](examples/quickstart-producer/) and [Schema Registry QuickStart](examples/quickstart-schema-registry/) examples. ([#50](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/50))

**Bug Fixes**

- Fixed an issue with template parsing where expressions containing string keys were not handled correctly. ([#51](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/51))


## [1.2.4] (2025-04-08)

**Improvements**

- Added support for [COMMAND mode](README.md#evaluate-as-command-fieldsevaluateascommandenable). ([#49](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/49))
- Updated the [Airport demo](examples/airport-demo/) to use COMMAND mode. ([#49](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/49))


## [1.2.3] (2025-04-01)

**Improvements**

- Added support for the [KVP format](README.md#support-for-key-value-pairs-kvp). ([#45](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/45), [#46](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/46))

- Upgraded _Gradle_ to version 8.13. ([#47](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/47))

- Upgraded dependency on _Spotless plugin for Gradle_ to version 7.0.2. ([#48](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/48))


## [1.2.2] (2025-03-28)

**Improvements**

- Added support for [mapping non-scalar values](README.md#map-non-scalar-values-fieldsmapnonscalarvalues) to Lightstreamer fields (also available for the [Sink connector](README.md#recordmappingsmapnonscalarvaluesenable)). ([#43](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/43))

**Bug Fixes**

- Fixed an issue in concurrent processing where the topic name was not considered when using the ORDER_BY_PARTITION order strategy. ([#39](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/39))

- Corrected various typos in the code base and spelling issues across multiple files. ([#43](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/43), [#44](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/44))

- Fixed typos in the `README.md` file included in the distribution package. ([#43](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/43))


## [1.2.1] (2025-03-03)

**Bug Fixes**

- Fixing spelling issues in documentation, examples, and comments in the configuration file. ([#40](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/40)), ([#41](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/41)), ([#42](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/42))


## [1.2.0] (2025-01-02)

**Improvements**

- Upgraded _Gradle_ to version 8.12. ([#37](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/37))

- Added support for [skipping failures](README.md#skip-failed-mapping-fieldsskipfailedmappingenable) while mapping fields (also available for the [Sink connector](README.md#recordmappingsskipfailedenable)). ([#34](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/34))

- Added support for topic mappings declared as [regular expressions](README.md#enable-regular-expression-mapregexenable) (also available for the [Sink connector](README.md#topicmappingsregexenable)). ([#33](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/33), [#35](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/35))

- Optimize processing when there are no routable items. ([#30](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/30))

**Bug Fixes**

- Fixed unit tests to support version numbers that may include pre-release identifiers. ([#38](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/38))

- Fixed the Docker build in the Airport demo. ([#36](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/36))

- Fixed the name of the [`record.mappings`](README.md#recordmappings) parameter. ([#34](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/34))

- Fixed evaluation of template expressions. ([#32](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/32))

- Fixed exceptions management of single-threaded processing. ([#31](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/31))


## [1.1.0] (2024-12-11)

**Improvements**

- Added support for concurrent processing, along with deep refactoring of the code base for better performance. ([#29](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/29))

- Upgraded _Gradle_ to version 8.11.1. ([#29](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/29))

**Documentation**

- Added some style changes to the [README.md](README.md) and factory [adapters.xml](kafka-connector-project/kafka-connector/src/adapter/dist/adapters.xml) files. ([#28](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/28))

- Introduced the [Client Side Error Handling](README.md#client-side-error-handling) section in the [README.md](README.md) file. ([#17](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/17))


## [1.0.7] (2024-11-06)

**Improvements and Documentation**

- Revised some default settings for the internal Kafka consumer. ([#25](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/25))

- Improved the [README.md](README.md) file. ([#18](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/18), [#20](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/20),
[#21](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/21),
[#22](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/22))

- Significantly reorganized the [examples](examples) folder. Specifically:

  - Moved the quickstart examples for specific vendors (Confluent, Redpanda, Aiven, and Axual) into dedicated subfolders under [examples/vendors](examples/vendors/). ([#20](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/20), [#23](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/23), [#24](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/24))

  - Adjusted the script files to align with the updated layout of the examples folder. ([#20](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/20))

  - Improved the `docker-compose.yml` and `README.md` files within the quickstart folders. ([#20](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/20))

  - Improved the `README.md` files within [examples/docker](examples/docker/) and [examples/docker-kafka-connect](examples/docker-kafka-connect/) folders. ([#20](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/20))

  - Introduced a dedicated [README.md](examples/vendors/confluent/README.md) file for _Confluent Cloud_ along with further specific resources under [examples/vendors/confluent](examples/vendors/confluent/). ([#20](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/20), [#22](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/22), [#26](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/26))

  - Added new resources to the [pictures](pictures) folder. ([#18](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/18), [#20](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/20), [#22](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/22))

  - Added a new data adapter configuration named `QuickStartConfluentCloud` to the factory [adapters.xml](kafka-connector-project/kafka-connector/src/adapter/dist/adapters.xml) file. ([#22](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/22))

  - Added a new logger configuration named `QuickStartConfluentCloud` to the factory [log4j.properties](kafka-connector-project/kafka-connector/src/adapter/dist/log4j.properties) file. ([#22](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/22))

  - Improved the [QuickStart web client](examples/compose-templates/web/) by refactoring the JavaScript code and using the CDN version of the Lightstreamer client library. ([#22](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/22))

**Bug Fixes**

- Unhandled runtime exceptions during synchronous commits prevented the fulfillment of new subscription requests. ([#27](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/27))


## [1.0.6] (2024-10-10)

**Documentation**

- Updated the `manifest.json` file embedded in the Sink connector archive. ([#15](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/15))

- Updated the [README.md](README.md) and factory [adapters.xml](kafka-connector-project/kafka-connector/src/adapter/dist/adapters.xml) files to explain how to configure Basic HTTP authentication for the Schema Registry. ([#16](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/16))


## [1.0.5] (2024-09-25)

**Improvements**

- Added the `etc` folder to the Sink connector zip file. ([#14](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/14))


## [1.0.4] (2024-09-10)

**Documentation**

- Updated the [README.md](README.md) file to explain how to use either the configuration properties file or the configuration JSON file to run the Sink connector. In addition, examples of JSON files have been provided under the [kafka-connector-project/config/kafka-connect-config](kafka-connector-project/config/kafka-connect-config/) folder. ([#8](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/8))

- Consistently updated all references to Java as "JDK version 17" throughout the project. ([#9](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/9))

- Clarify the JDK version required to run the Sink connector. ([#10](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/10))

- Updated the `manifest.json` file embedded in the Sink connector archive. ([#13](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/13))

- Made some additional minor fixes. ([#12](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/12))

**Improvements**

- Introduced automatic configuration of the `client.id` consumer property when connected to Confluent Cloud. ([#11](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/11))


## [1.0.3] (2024-09-03)

**Improvements**

- Improved the offsets management and logging in the [ConsumerLoop](kafka-connector-project/kafka-connector/src/main/java/com/lightstreamer/kafka/adapters/consumers/ConsumerLoop.java) class.

- Modified the [`quickstart-producer`](examples/quickstart-producer/) sample client project to make it publishable to _GitHub Packages_.

- Deeply refactored the classes and interfaces of the `com.lightstreamer.kafka.connect` package to enhance loose coupling and facilitate testability.

- Improved the unit tests relative to the Sink connector.

**Documentation**

Modified the [README.md](README.md) file has follows:

- Added the [Supported Converters](README.md#supported-converters) section, clarifying the supported converters for the Sink connector.

- Stated the _Kafka Connect_ framework version for which the Sink connector has been developed.

- Mentioned the _Confluent Platform_ version to which the Sink connector can be deployed.

- Added the [tasks.max](README.md#tasksmax) section, clarifying the impact of the `tasks.max` property on the Sink connector configuration.

- Removed incomplete statements.

**Bug Fixes**

- The Sink connector enabled the creation of multiple tasks, despite the one-to-one relationship between ProxyAdapter and task.

- A _Converter_ configured without schema support triggered an exception, leading to an unexpected interruption of the Sink connector.


## [1.0.2] (2024-08-26)

**Improvements**

- Improved the [README.md](README.md) file by restructuring the [Topic Mapping](README.md#topic-mapping) section.

**Bug Fixes**

- Fixed reference to the `com.lightstreamer.kafka.adapters.pub.KafkaConnectorMetadataAdapter` class in several resource files. ([#4](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/4))


## [1.0.1] (2024-08-23)

**Improvements**

- Added the [Project Report Plugin](https://docs.gradle.org/current/userguide/project_report_plugin.html) to the [project](./kafka-connector-project/buildSrc/src/main/groovy/lightstreamer-kafka-connector.gradle#L5) to generate reports containing information about the build.

- Added dependency on _javadoc_ task to the _build_ task in the [build.gradle](./kafka-connector-project/kafka-connector/build.gradle) file.

**Bug Fixes**

- The following configuration parameters for the Sink connector:
  - `lightstreamer.server.proxy_adapter.username`
  - `lightstreamer.server.proxy_adapter.password`

  were being ignored.

- The [com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfigTest](./kafka-connector-project/kafka-connector/src/test/java/com/lightstreamer/kafka/connect/config/LightstreamerConnectorConfigTest.java) class contained a test producing unwanted output files.

- The [examples/docker-kafka-connect/README.md](./examples/docker-kafka-connect/README.md) file contained a wrong reference to the Docker image name.

- The [gradle.properties](./kafka-connector-project/gradle.properties) file contained an unused property.

- The [README.md](README.md) file contained broken links.


## [1.0.0] (2024-08-20)

- First official public release


## [0.1.0] (2024-03-17)

- First public pre-release
