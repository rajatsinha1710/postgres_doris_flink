# CDC-to-Doris Pipeline

Streamlined real-time pipeline for Change Data Capture (CDC) events from Kafka directly to Doris via JDBC. This project is now fully configurable through the `application.properties` file, making it easy to deploy with different Kafka endpoints, topic names, and Doris endpoints.

## Overview

This pipeline consumes CDC events from Kafka topics and performs direct upserts to Doris using JDBC. No intermediate processing, merging, or enrichment - each CDC event is mapped to relevant columns and upserted directly to the Doris table.

## Architecture

```
Kafka CDC Events → CDC Processor → Doris JDBC Sink → Doris Table
```

- **Kafka Consumer**: Consumes CDC events from multiple topics
- **CDC Processor**: Maps CDC events to Doris table columns based on source table
- **Doris JDBC Sink**: Performs direct upserts using `INSERT ... ON DUPLICATE KEY UPDATE`

## Configuration

### Quick Start

1. Copy the example configuration:
   ```bash
   cp src/main/resources/application-example.properties src/main/resources/application.properties
   ```

2. Edit `src/main/resources/application.properties` with your specific values:
   ```properties
   # Your Kafka configuration
   kafka.bootstrap.servers=your-kafka-server:9092
   kafka.topics=your.schema.table1,your.schema.table2
   
   # Your Doris configuration
   doris.jdbc.url=jdbc:mysql://your-doris-fe:9030/your_database
   doris.username=your_username
   doris.password=your_password
   doris.table=your_table_name
   ```

3. Build and run:
   ```bash
   mvn clean package
   java -jar target/flink-postgres-doris-pipeline-1.0.0.jar
   ```

### Configuration Parameters

#### Kafka Configuration
- `kafka.bootstrap.servers`: Comma-separated list of Kafka broker addresses
- `kafka.group.id`: Consumer group ID for Kafka
- `kafka.auto.offset.reset`: Offset reset strategy (earliest, latest, none)
- `kafka.topics`: Comma-separated list of Kafka topics to consume

#### Doris Configuration
- `doris.jdbc.url`: JDBC URL for Doris database connection
- `doris.username`: Doris database username
- `doris.password`: Doris database password
- `doris.database`: Doris database name
- `doris.table`: Doris table name for upserts

#### Flink Configuration
- `flink.parallelism`: Number of parallel tasks for Flink processing
- `flink.checkpoint.interval.ms`: Checkpoint interval in milliseconds

#### Doris JDBC Properties
- `doris.jdbc.useSSL`: Enable/disable SSL (true/false)
- `doris.jdbc.allowPublicKeyRetrieval`: Allow public key retrieval (true/false)
- `doris.jdbc.serverTimezone`: Server timezone (e.g., UTC)

### Default Values

If `application.properties` is not found or a property is missing, the system will use these default values:

```properties
kafka.bootstrap.servers=localhost:29092
kafka.group.id=flink-cdc-doris-pipeline
kafka.auto.offset.reset=earliest
kafka.topics=pgserver1.public.clients_to_exclude,pgserver1.public.job_activity,pgserver1.public.jobs,pgserver1.public.standard_revenue
doris.jdbc.url=jdbc:mysql://127.0.0.1:9030/job_analytics
doris.username=root
doris.password=
doris.database=job_analytics
doris.table=merged_job_data
flink.parallelism=4
flink.checkpoint.interval.ms=60000
doris.jdbc.useSSL=false
doris.jdbc.allowPublicKeyRetrieval=true
doris.jdbc.serverTimezone=UTC
```

### Environment-Specific Examples

#### Development Environment
```properties
kafka.bootstrap.servers=localhost:29092
doris.jdbc.url=jdbc:mysql://localhost:9030/dev_db
doris.password=dev_password
flink.parallelism=2
```

#### Production Environment
```properties
kafka.bootstrap.servers=kafka1:9092,kafka2:9092,kafka3:9092
doris.jdbc.url=jdbc:mysql://doris-fe-cluster:9030/production_db
doris.password=secure_production_password
flink.parallelism=8
flink.checkpoint.interval.ms=30000
```

### Custom Table Schema

If you need to use a different table schema, simply update the `doris.table` property:

```properties
doris.table=your_custom_table_name
```

The pipeline will automatically use the configured table name for all upsert operations.

## Doris Table Schema

The target table should be created with the following schema (or adapt it to your needs):

```sql
CREATE TABLE merged_job_data (
    activity_id VARCHAR(255) NOT NULL,
    job_id VARCHAR(255),
    jobdiva_no VARCHAR(255),
    candidate VARCHAR(255),
    company_name VARCHAR(255),
    assignment_start_date DATETIME,
    assignment_end_date DATETIME,
    job_title VARCHAR(255),
    location VARCHAR(255),
    excluded_reason VARCHAR(255),
    sr_assignment_start DATETIME,
    sr_assignment_end DATETIME,
    standard_revenue_report_month VARCHAR(255),
    clienterpid VARCHAR(255),
    bcworkerid VARCHAR(255),
    adjustedstbillrate DECIMAL(10,2),
    adjustedstpayrate DECIMAL(10,2),
    adjgphrst DECIMAL(10,2),
    adjrevenue DECIMAL(10,2),
    stbillrate DECIMAL(10,2),
    PRIMARY KEY (activity_id)
) ENGINE=OLAP
DUPLICATE KEY(activity_id)
DISTRIBUTED BY HASH(activity_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);
```

## Column Mapping

The pipeline maps CDC events to Doris columns based on the source table:

### job_activity table
- `activity_id` → `activity_id` (primary key)
- `job_id` → `job_id`
- `jobdiva_no` → `jobdiva_no`
- `candidate_name` → `candidate`
- `company_name` → `company_name`
- `assignment_start_date` → `assignment_start_date`
- `assignment_end_date` → `assignment_end_date`
- `bcworkerid` → `bcworkerid`

### jobs table
- `job_id` → `job_id`
- `job_title` → `job_title`
- `location` → `location`

### clients_to_exclude table
- `exclusion_reason` → `excluded_reason`
- `client_id` → `company_name`

### standard_revenue table
- `standard_revenue_report_month` → `standard_revenue_report_month`
- `clienterpid` → `clienterpid`
- `bcworkerid` → `bcworkerid`
- `adjusted_st_bill_rate` → `adjustedstbillrate`
- `adjusted_st_pay_rate` → `adjustedstpayrate`
- `adj_gph_rst` → `adjgphrst`
- `adj_revenue` → `adjrevenue`
- `st_bill_rate` → `stbillrate`
- `assignment_start_date` → `sr_assignment_start`
- `assignment_end_date` → `sr_assignment_end`

## Running the Pipeline

1. **Build the project**:
   ```bash
   mvn clean package
   ```

2. **Start Flink cluster** (if not already running)

3. **Submit the job**:
   ```bash
   flink run target/flink-postgres-doris-pipeline-1.0.0.jar
   ```

   Or run directly:
   ```bash
   java -jar target/flink-postgres-doris-pipeline-1.0.0.jar
   ```

## Key Features

- **Generic Configuration**: Fully configurable through `application.properties`
- **Direct CDC Processing**: No intermediate storage or complex merging logic
- **Table-Specific Mapping**: Only relevant columns are upserted based on source table
- **JDBC Upserts**: Uses `INSERT ... ON DUPLICATE KEY UPDATE` for reliable upserts
- **Error Handling**: Basic error handling with logging, no complex retry frameworks
- **Minimal Dependencies**: Removed PostgreSQL, HikariCP, and HTTP client dependencies
- **Streamlined Architecture**: Simple, direct pipeline with minimal abstractions

## Monitoring

The pipeline provides basic logging for:
- CDC event processing
- Column mapping
- Upsert operations
- Error conditions
- Configuration loading

Check Flink logs for pipeline status and any error conditions. The pipeline will print the loaded configuration at startup. Look for the "=== Pipeline Configuration ===" section in the logs to verify your settings are loaded correctly.

## Troubleshooting

1. **Configuration not loading**: Check that `application.properties` is in `src/main/resources/`
2. **Connection errors**: Verify your Kafka and Doris endpoints are accessible
3. **Table not found**: Ensure the Doris table exists and matches your schema
4. **Topic not found**: Verify Kafka topics exist and are accessible
5. **Build errors**: Ensure all dependencies are properly installed

## Prerequisites

- Java 11+
- Maven 3.6+
- Apache Flink 1.18.0
- Apache Kafka
- Apache Doris

## Changes from Original Pipeline

- **Added**: Generic configuration system with `application.properties`
- **Removed**: PostgreSQL integration, complex merge queries, batching logic
- **Simplified**: Direct CDC-to-Doris mapping with JDBC upserts
- **Streamlined**: Minimal dependencies and straightforward architecture
- **Focused**: Single responsibility - CDC event processing and direct upserts
- **Configurable**: Easy deployment with different endpoints and configurations