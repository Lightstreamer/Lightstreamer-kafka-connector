# Producer for the Quick Start App

This folder contains the Gradle project of the Kafka native producer used for the _Quick Start_ App. See the [Quick Start](../../README.md#quick-start-set-up-in-5-minutes) section for more details.

To build the producer:

 ```sh
./gradlew distribuite 
```

which generates the `quickstart-producer-all` under the `deploy` folder.

To run it:

```sh
java -jar deploy/quickstart-producer-all.jar --bootstrap-servers <kafka.connection.string> --topic stocks
```
