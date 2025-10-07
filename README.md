# Flink PostgreSQL Doris Pipeline

A real-time data pipeline built with Apache Flink that integrates PostgreSQL, Kafka, and Apache Doris for job analytics data processing.

## Overview

This pipeline consumes Change Data Capture (CDC) events from Kafka topics, extracts primary key IDs, queries PostgreSQL for merged records, and loads the results into Apache Doris for analytics.

## Architecture

```
Kafka Topics → Flink Job → PostgreSQL Query → Doris Stream Load
     ↓              ↓              ↓              ↓
CDC Events → ID Extraction → Merged Results → Analytics DB
```

## Features

- **Multi-topic Kafka consumption** from PostgreSQL CDC events
- **Intelligent ID extraction** from various CDC formats (Debezium, Maxwell)
- **Complex PostgreSQL merge queries** with JDBC prepared statements
- **Batch processing** with configurable batch sizes
- **HTTP Stream Load** to Apache Doris with redirect handling
- **Retry logic** with exponential backoff
- **Fault tolerance** with Flink checkpointing
- **Comprehensive logging** and monitoring

## Components

### 1. Data Models
- `MergedResult`: Represents the merged data from PostgreSQL query

### 2. Configuration
- `PipelineConfig`: Centralized configuration management

### 3. Kafka Integration
- `KafkaConsumerSetup`: Multi-topic Kafka consumer configuration

### 4. Data Processing
- `IdExtractor`: Extracts primary key IDs from CDC events
- `PostgresMergeQueryProcessor`: Executes PostgreSQL merge queries

### 5. Data Sink
- `DorisSink`: Batched HTTP Stream Load to Apache Doris

### 6. Utilities
- `PipelineUtils`: Common utility functions for retry logic and validation

## Configuration

### Kafka Configuration
```java
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPICS = [
    "enhanced_job_server_v2.public.jobs",
    "enhanced_job_server_v2.public.job_activity",
    "enhanced_job_server_v2.public.clients_to_exclude",
    "enhanced_job_server_v2.public.standard_revenue"
]
```

### PostgreSQL Configuration
```java
POSTGRES_URL = "jdbc:postgresql://localhost:5432/enhanced_job_server_v2"
POSTGRES_USERNAME = "postgres"
POSTGRES_PASSWORD = "password"
```

### Doris Configuration
```java
DORIS_STREAM_LOAD_URL = "http://localhost:8030/api/job_analytics/merged_job_data/_stream_load"
DORIS_BATCH_SIZE = 100
DORIS_MAX_RETRIES = 3
```

## Building and Running

### Prerequisites
- Java 11+
- Maven 3.6+
- Apache Flink 1.18.0
- PostgreSQL database
- Apache Kafka
- Apache Doris

### Build
```bash
mvn clean package
```

### Run
```bash
java -jar target/flink-postgres-doris-pipeline-1.0.0.jar
```

## Data Flow

1. **Kafka Consumption**: Consumes CDC events from multiple PostgreSQL tables
2. **ID Extraction**: Parses CDC events and extracts relevant primary key IDs
3. **PostgreSQL Query**: Executes complex merge query using extracted IDs
4. **Data Transformation**: Maps ResultSet to MergedResult objects
5. **Batch Processing**: Groups records into configurable batches
6. **Doris Loading**: Sends batches to Doris via HTTP Stream Load API

## Error Handling

- **Retry Logic**: Exponential backoff for failed operations
- **Fault Tolerance**: Flink checkpointing for job recovery
- **Logging**: Comprehensive error logging and monitoring
- **Graceful Degradation**: Continues processing despite individual failures

## Monitoring

The pipeline provides detailed logging for:
- Kafka message consumption
- ID extraction success/failure rates
- PostgreSQL query performance
- Doris loading statistics
- Error conditions and retries

## Performance Tuning

- **Parallelism**: Configurable Flink parallelism
- **Batch Sizes**: Adjustable Doris batch sizes
- **Checkpointing**: Configurable checkpoint intervals
- **Connection Pooling**: PostgreSQL connection management

## Security Considerations

- Database credentials should be externalized
- Kafka authentication can be added
- Doris authentication via HTTP Basic Auth
- Network security for database connections
