# Quick Start Redpanda Self-hosted

This folder contains a variant of the [_Quick Start_](../../README.md#quick-start-set-up-in-5-minutes) app configured to use _Redpanda_ as the target broker.

The [docker-compose.yml](docker-compose.yml) file has been revised to realize the integration with [Redpanda Self-hosted](https://docs.redpanda.com/current/get-started/quick-start/) as follows:

- _redpanda-0_: replacement of the previous `broker` service, based on the definition included in the [`docker-compose.yml`](https://docs.redpanda.com/redpanda-labs/docker-compose/_attachments/single-broker/docker-compose.yml) file provided by the [_Redpanda Self-hosted Quick Start_](https://docs.redpanda.com/current/get-started/quick-start/)

- _kafka-connector_:

  adaption of [`adapters.xml`](./adapters.xml) to include:
  - new endpoint (`broker:9092`):
    ```xml
    <param name="bootstrap.servers">broker:9092</param>
    ```
- _producer_:
  - parameter `--bootstrap-servers` set to the new endpoint (`broker:9092`)


## Run

From this directory, follow the same instructions you can find in the [Quick Start](../../README.md#run) section of the main README file.
