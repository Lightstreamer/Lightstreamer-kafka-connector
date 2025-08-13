# Quick Start with AutoMQ

This folder contains a variant of the [_Quick Start_](../../../quickstart/README.md#quick-start-set-up-in-5-minutes) app configured to use [_AutoMQ_](https://www.automq.com/) as the target Kafka cluster. AutoMQ is a cloud-native Kafka distribution that separates compute and storage, offering elastic scaling and cost-effective S3-based storage.

The [docker-compose.yml](docker-compose.yml) file has been revised to realize the integration with _AutoMQ_ as follows:

- **AutoMQ Broker Configuration**: Uses the official AutoMQ Docker image with S3 storage configuration
- **S3 Storage Backend**: Integration with MinIO as S3-compatible storage for data and operational metadata
- **Kafka UI Integration**: Added Kafka UI for cluster monitoring and management
- **Network Configuration**: Custom network setup for service communication

## Key Features

- **Cloud-Native Architecture**: AutoMQ separates compute and storage, enabling elastic scaling
- **S3-Based Storage**: Cost-effective storage with automatic tiering and compression
- **Kafka Compatibility**: 100% compatible with Apache Kafka APIs
- **Built-in Monitoring**: Integrated Kafka UI for cluster management
- **Easy Setup**: Complete stack with single command deployment

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

## Architecture Overview

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

## Stopping the Demo

To stop all services and clean up resources:

```sh
$ ./stop.sh
```

## Configuration Details

### AutoMQ Specific Settings

- **S3 Data Buckets**: `s3://automq-data` - Primary data storage
- **S3 Ops Buckets**: `s3://automq-ops` - Operational metadata storage  
- **S3 WAL Path**: Write-Ahead Log storage in S3
- **Cluster ID**: Unique identifier for the AutoMQ cluster
- **Node Configuration**: Single-node setup with combined controller and broker roles

### Network Ports

- `8080`: Lightstreamer Kafka Connector web interface
- `9092`: AutoMQ Kafka broker (external access)
- `29092`: AutoMQ Kafka broker (internal access)
- `9000`: MinIO S3 API
- `9001`: MinIO Console
- `12000`: Kafka UI

### Storage Configuration

AutoMQ leverages S3 storage for:
- **Data Tiering**: Automatic movement of data to cost-effective storage
- **Infinite Retention**: Store data indefinitely without local disk constraints
- **Elastic Scaling**: Scale compute independently from storage
- **Disaster Recovery**: Built-in data replication and backup

## Troubleshooting

### Common Issues

1. **Memory Issues**: Ensure at least 4GB RAM is available
2. **Port Conflicts**: Check that ports 8080, 9000, 9001, 9092, and 12000 are not in use
3. **S3 Connection**: Verify MinIO is healthy before AutoMQ starts
4. **Network Issues**: Ensure Docker network `automq_net` is created properly

### Logs

View service logs using:
```sh
$ docker-compose logs -f [service-name]
```

Where `[service-name]` can be: `broker`, `minio`, `kafka-connector`, `producer`, or `kafka-ui`.

## Next Steps

- Explore AutoMQ's [official documentation](https://docs.automq.com/)
- Learn about [S3 storage configuration](https://docs.automq.com/docs/automq-s3-kafka/YUzOwI8saiDGHDkv1BucTw)
- Try [AutoMQ Cloud](https://console.automq.com/) for production deployments
- Integrate with your existing Kafka applications