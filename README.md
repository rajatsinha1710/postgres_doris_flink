# CDC-to-Doris Pipeline

Streamlined real-time pipeline for Change Data Capture (CDC) events from Kafka directly to Doris via JDBC.

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

### Kafka Configuration
```java
KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
KAFKA_GROUP_ID = "flink-cdc-doris-pipeline"
KAFKA_TOPICS = [
    "pgserver1.public.clients_to_exclude",
    "pgserver1.public.job_activity", 
    "pgserver1.public.jobs",
    "pgserver1.public.standard_revenue"
]
```

### Doris Configuration
```java
DORIS_JDBC_URL = "jdbc:mysql://127.0.0.1:9030/job_analytics"
DORIS_USERNAME = "root"
DORIS_PASSWORD = ""
DORIS_DATABASE = "job_analytics"
DORIS_TABLE = "merged_job_data"
```

## Doris Table Schema

The target table `merged_job_data` should be created with the following schema:

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

## Key Features

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

Check Flink logs for pipeline status and any error conditions.

## Prerequisites

- Java 11+
- Maven 3.6+
- Apache Flink 1.18.0
- Apache Kafka
- Apache Doris

## Changes from Original Pipeline

- **Removed**: PostgreSQL integration, complex merge queries, batching logic
- **Simplified**: Direct CDC-to-Doris mapping with JDBC upserts
- **Streamlined**: Minimal dependencies and straightforward architecture
- **Focused**: Single responsibility - CDC event processing and direct upserts