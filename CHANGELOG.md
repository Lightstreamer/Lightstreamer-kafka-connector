# Changelog

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

- Updated the [Producer for the Quickstart App](examples/quickstart-producer/) example project to produce a Docker image which supports versioned JAR files. ([#52](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/52))

**Bug Fixes**

- Updated the [Airport demo](examples/airport-demo/) example to load the Lightstreamer library from _https_. ([#53](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/53))

- Fixed consumer loop blockage that occurred when errors happened during the commit stage while being notified about partition revocation. ([#54](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/54))

- Fixed indefinite growth of the committed metadata when using log compaction with the Connector configured for concurrent processing (also clarified in the [`README.md`](README.md) file). ([#54](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/54))

- Updated the comments in the factory [`adapters.xml`](kafka-connector-project/kafka-connector/src/adapter/dist/adapters.xml) file to clarify available deserialization formats. Corresponding changes also applied to the [`README.md`](README.md) file to maintain consistency in documentation. ([#55](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/55))


## [1.2.5] (2025-04-16)

**Improvements**

- Added support for [Protocol Buffer format](README.md#protocol-buffer-format) for message deserialization, with updates to the [Producer for the Quickstart App](examples/quickstart-producer/) and [Quickstart Schema Registry](examples/quickstart-schema-registry/) examples. ([#50](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/50))

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

  - Moved the quick start examples for specific vendors (Confluent, Redpanda, Aiven, and Axual) into dedicated subfolders under [examples/vendors](examples/vendors/). ([#20](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/20), [#23](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/23), [#24](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/24))

  - Adjusted the script files to align with the updated layout of the examples folder. ([#20](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/20))

  - Improved the `docker-compose.yml` and `README.md` files within the quick start folders. ([#20](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/20))

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
