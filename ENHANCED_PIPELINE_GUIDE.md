# Enhanced CDC-to-Doris Pipeline Guide

## Overview

This enhanced pipeline removes PostgreSQL dependency completely and implements selective column updates based on source table changes. When CDC events arrive from Kafka, only relevant columns in the Doris merged table are updated, avoiding full row replacements.

## Key Features

### 1. **Source Table-Based Selective Updates**
- **job_activity** → Updates: `activity_id, job_id, jobdiva_no, candidate, company_name, assignment_start_date, assignment_end_date, bcworkerid`
- **jobs** → Updates: `job_title, location` (matched by `job_id`)
- **clients_to_exclude** → Updates: `excluded_reason` (matched by `company_name`)
- **standard_revenue** → Updates: `standard_revenue_report_month, clienterpid, adjustedstbillrate, adjustedstpayrate, adjgphrst, adjrevenue, stbillrate, sr_assignment_start, sr_assignment_end` (matched by `bcworkerid`)

### 2. **No PostgreSQL Dependency**
- Direct Kafka → Doris pipeline
- No intermediate storage or lookups
- Maintains data integrity through smart upsert logic

### 3. **Dynamic SQL Generation**
- Prepared statements cached for performance
- Different SQL patterns for each source table
- Handles missing records gracefully (creates placeholders when needed)

## Architecture

```
┌─────────────┐    ┌──────────────┐    ┌──────────────┐    ┌─────────────────┐
│   Kafka     │───▶│  CDC         │───▶│  Enhanced    │───▶│      Doris      │
│   Topics    │    │  Processor   │    │  JDBC Sink   │    │  merged_job_data│
└─────────────┘    └──────────────┘    └──────────────┘    └─────────────────┘
```

## Example Scenarios

### Scenario 1: Job Activity Update
When `job_activity` table receives an update:
```sql
-- Only these columns are updated/inserted
INSERT INTO merged_job_data 
(activity_id, job_id, jobdiva_no, candidate, company_name, 
 assignment_start_date, assignment_end_date, bcworkerid) 
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE 
job_id = VALUES(job_id), 
candidate = VALUES(candidate),
-- ... other job_activity fields only
```

### Scenario 2: Jobs Table Update
When `jobs` table receives an update:
```sql
-- Only job-related fields are updated
UPDATE merged_job_data SET 
job_title = ?, location = ? 
WHERE job_id = ?
```

### Scenario 3: Standard Revenue Update
When `standard_revenue` table receives an update:
```sql
-- Only revenue-related fields are updated
UPDATE merged_job_data SET 
standard_revenue_report_month = ?, clienterpid = ?, 
adjustedstbillrate = ?, adjustedstpayrate = ?, 
-- ... other revenue fields
WHERE bcworkerid = ?
```

## Configuration

### Doris Table Schema
```sql
CREATE TABLE merged_job_data (
    activity_id VARCHAR(50) PRIMARY KEY,
    job_id VARCHAR(50),
    jobdiva_no VARCHAR(50),
    candidate VARCHAR(255),
    company_name VARCHAR(255),
    assignment_start_date DATETIME,
    assignment_end_date DATETIME,
    job_title VARCHAR(255),
    location VARCHAR(255),
    excluded_reason VARCHAR(255),
    sr_assignment_start DATETIME,
    sr_assignment_end DATETIME,
    standard_revenue_report_month VARCHAR(50),
    clienterpid VARCHAR(50),
    bcworkerid VARCHAR(50),
    adjustedstbillrate DECIMAL(10,2),
    adjustedstpayrate DECIMAL(10,2),
    adjgphrst DECIMAL(10,2),
    adjrevenue DECIMAL(10,2),
    stbillrate DECIMAL(10,2)
);
```

### Kafka Topics
Configure these topics in `PipelineConfig.java`:
```java
public static final String[] KAFKA_TOPICS = {
    "pgserver1.public.clients_to_exclude",
    "pgserver1.public.job_activity", 
    "pgserver1.public.jobs",
    "pgserver1.public.standard_revenue"
};
```

## Running the Pipeline

### 1. Build the Project
```bash
mvn clean package
```

### 2. Start Dependencies
```bash
# Start Kafka
docker-compose up kafka zookeeper

# Start Doris
docker-compose up doris-fe doris-be
```

### 3. Create Doris Table
```sql
-- Connect to Doris and create the merged table
CREATE DATABASE IF NOT EXISTS job_analytics;
USE job_analytics;
-- Execute the CREATE TABLE statement above
```

### 4. Run the Pipeline
```bash
java -jar target/flink-postgres-doris-pipeline-1.0.0.jar
```

## Monitoring and Debugging

### Log Levels
- **DEBUG**: Shows individual upsert operations and row counts
- **INFO**: Shows connection status and pipeline lifecycle events
- **WARN**: Shows skipped operations and missing data scenarios
- **ERROR**: Shows connection failures and processing errors

### Key Metrics to Monitor
- **Kafka Consumer Lag**: Ensure pipeline keeps up with CDC events
- **Doris Connection Health**: Monitor JDBC connection status
- **Upsert Success Rate**: Track successful vs failed operations
- **Selective Update Patterns**: Verify correct columns are being updated

### Common Issues and Solutions

#### 1. No Matching Records for Updates
- **Issue**: Jobs/clients_to_exclude/standard_revenue updates find no matching records
- **Solution**: Pipeline creates placeholder records or logs warnings appropriately

#### 2. Data Type Mismatches
- **Issue**: Source CDC data types don't match Doris expectations
- **Solution**: Enhanced type conversion in `setParameter()` method

#### 3. Connection Pool Exhaustion
- **Issue**: Too many concurrent JDBC connections
- **Solution**: Prepared statement caching and proper resource cleanup

## Testing Strategy

### Unit Tests
- Test CDC event parsing and column mapping
- Validate SQL generation for different source tables
- Test data type conversions and null handling

### Integration Tests
- End-to-end pipeline testing with sample CDC events
- Verify selective update behavior
- Test error handling and recovery scenarios

### Performance Tests
- Benchmark throughput with high-volume CDC streams
- Measure latency from Kafka to Doris
- Test prepared statement caching effectiveness

## Benefits of This Approach

1. **Reduced Latency**: No PostgreSQL round trips
2. **Better Performance**: Selective updates reduce Doris load
3. **Data Consistency**: Atomic upserts per source table
4. **Scalability**: Parallel processing per source table type
5. **Maintainability**: Clear separation of concerns per table

## Future Enhancements

1. **Batch Processing**: Group updates for better throughput
2. **Conflict Resolution**: Handle concurrent updates to same records
3. **Schema Evolution**: Support dynamic schema changes
4. **Monitoring Dashboard**: Real-time pipeline health monitoring