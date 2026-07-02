# AutoMQ quickstart

This folder contains a variant of the [_Quickstart_](../../../quickstart/README.md#quick-start-set-up-in-5-minutes) app configured to use [_AutoMQ_](https://www.automq.com/) as the target Kafka cluster. AutoMQ is a cloud-native Kafka distribution that separates compute and storage, offering elastic scaling and cost-effective S3-based storage.

The [docker-compose.yml](docker-compose.yml) file has been revised to realize the integration with _AutoMQ_ as follows:

- **AutoMQ broker configuration**: Uses the official AutoMQ Docker image with S3 storage configuration
- **S3 storage backend**: Integration with MinIO as S3-compatible storage for data and operational metadata
- **Kafka UI integration**: Added Kafka UI for cluster monitoring and management
- **Network configuration**: Custom network setup for service communication

## Key features

- **Cloud-native architecture**: AutoMQ separates compute and storage, enabling elastic scaling
- **S3-based storage**: Cost-effective storage with automatic tiering and compression
- **Kafka compatibility**: 100% compatible with Apache Kafka APIs
- **Built-in monitoring**: Integrated Kafka UI for cluster management
- **Easy setup**: Complete stack with single command deployment

## Prerequisites

- Docker and Docker Compose installed
- At least 4GB of available memory for optimal performance
- Network access for downloading Docker images

## Run

From this directory, run the following command:

```sh
$ ./start.sh
```

This will start all services including:
- AutoMQ Kafka cluster with S3 storage
- MinIO S3-compatible storage
- Lightstreamer Kafka Connector
- Sample data producer
- Kafka UI for monitoring

Once all containers are ready:

1. **Access the Lightstreamer demo**: Point your browser to [http://localhost:8080/QuickStart](http://localhost:8080/QuickStart) to see real-time stock data streaming.

2. **Monitor the cluster**: Access Kafka UI at [http://localhost:12000](http://localhost:12000) to monitor topics, messages, and cluster health.

3. **Manage S3 storage**: Access MinIO Console at [http://localhost:9001](http://localhost:9001) (credentials: minioadmin/minioadmin) to view stored data.

## Architecture overview

This setup demonstrates AutoMQ's cloud-native architecture:

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Web Client    │◄───│  Lightstreamer   │◄───│   AutoMQ        │
│   (Browser)     │    │  Kafka Connector │    │   Cluster       │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
                                                         ▼
                                                ┌─────────────────┐
                                                │   MinIO S3      │
                                                │   Storage       │
                                                └─────────────────┘
```

## Stopping the demo

To stop all services and clean up resources:

```sh
$ ./stop.sh
```

## Configuration details

### AutoMQ-specific settings

- **S3 data buckets**: `s3://automq-data` - Primary data storage
- **S3 ops buckets**: `s3://automq-ops` - Operational metadata storage  
- **S3 WAL path**: Write-Ahead Log storage in S3
- **Cluster ID**: Unique identifier for the AutoMQ cluster
- **Node configuration**: Single-node setup with combined controller and broker roles

### Network ports

- `8080`: Lightstreamer Kafka Connector web interface
- `9092`: AutoMQ Kafka broker (external access)
- `29092`: AutoMQ Kafka broker (internal access)
- `9000`: MinIO S3 API
- `9001`: MinIO Console
- `12000`: Kafka UI

### Storage configuration

AutoMQ leverages S3 storage for:
- **Data tiering**: Automatic movement of data to cost-effective storage
- **Infinite retention**: Store data indefinitely without local disk constraints
- **Elastic scaling**: Scale compute independently from storage
- **Disaster recovery**: Built-in data replication and backup

## Troubleshooting

### Common issues

1. **Memory issues**: Ensure at least 4GB RAM is available
2. **Port conflicts**: Check that ports 8080, 9000, 9001, 9092, and 12000 are not in use
3. **S3 connection**: Verify MinIO is healthy before AutoMQ starts
4. **Network issues**: Ensure Docker network `automq_net` is created properly

### Logs

View service logs using:
```sh
$ docker-compose logs -f [service-name]
```

Where `[service-name]` can be: `broker`, `minio`, `kafka-connector`, `producer`, or `kafka-ui`.

## Next steps

- Explore AutoMQ's [official documentation](https://www.automq.com/docs/automq/deployment/deploy-multi-nodes-cluster-on-linux)
- Learn about [AutoMQ architecture](https://www.automq.com/docs/automq/architecture)
- Try [AutoMQ Cloud](https://console.automq.cloud/) for production deployments
- Integrate with your existing Kafka applications
