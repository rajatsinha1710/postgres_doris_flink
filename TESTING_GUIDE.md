# Doris Upsert Testing Guide

This guide explains how to test the Doris upsert functionality in the CDC-to-Doris pipeline.

## Test Structure

The testing mechanism consists of several components:

### 1. Unit Tests (`EnhancedPipelineTest.java`)
- Tests CDC event parsing and column mapping
- Tests configuration loading
- Tests data generation utilities
- **No database connection required**

### 2. Sink Tests (`DorisJdbcSinkTest.java`)
- Tests direct JDBC upsert operations
- Tests various data types and scenarios
- **Requires Doris database connection**

### 3. Integration Tests (`DorisUpsertIntegrationTest.java`)
- Tests end-to-end pipeline functionality
- Tests multi-table upsert scenarios
- Tests performance and error handling
- **Requires Doris database connection**

### 4. SQL Tests (`doris_upsert_test.sql`)
- Manual SQL-based upsert testing
- Tests different upsert scenarios
- **Requires Doris database connection**

### 5. Test Data Generator (`TestDataGenerator.java`)
- Generates realistic test CDC events
- Creates test data for different source tables
- Supports batch and performance testing

## Prerequisites

### Required Software
- Java 11+
- Maven 3.6+
- Apache Doris (running on 127.0.0.1:9030)
- MySQL client (for SQL tests)

### Required Databases
1. **Production Database**: `job_analytics.merged_job_data`
2. **Test Database**: `test_job_analytics.test_merged_job_data`

## Running Tests

### Quick Start
```bash
# Make the test runner executable
chmod +x scripts/run_tests.sh

# Run all tests
./scripts/run_tests.sh all
```

### Individual Test Categories

#### Unit Tests Only (No Database Required)
```bash
./scripts/run_tests.sh unit
```

#### Sink Tests (Requires Doris)
```bash
./scripts/run_tests.sh sink
```

#### Integration Tests (Requires Doris)
```bash
./scripts/run_tests.sh integration
```

#### SQL Tests (Requires Doris)
```bash
./scripts/run_tests.sh sql
```

### Setup and Cleanup
```bash
# Setup test environment
./scripts/run_tests.sh setup

# Clean up test data
./scripts/run_tests.sh cleanup
```

## Test Scenarios

### 1. Basic Upsert Testing
- **Insert**: New record creation
- **Update**: Existing record modification
- **Upsert**: Insert or update based on primary key

### 2. Multi-Table Upsert Testing
- **job_activity** → Updates candidate, company, dates
- **jobs** → Updates job_title, location
- **standard_revenue** → Updates revenue fields
- **clients_to_exclude** → Updates exclusion reason

### 3. Data Type Testing
- **VARCHAR**: String values
- **DATETIME**: Timestamp values
- **DECIMAL**: Numeric values with precision

### 4. Error Handling Testing
- Invalid JSON handling
- Null value handling
- Connection error handling
- Data type conversion errors

### 5. Performance Testing
- Batch processing (100+ records)
- Concurrent upserts
- Large dataset handling

## Test Configuration

### Test Properties (`src/test/resources/application-test.properties`)
```properties
# Test-specific configuration
doris.jdbc.url=jdbc:mysql://127.0.0.1:9030/test_job_analytics
doris.database=test_job_analytics
doris.table=test_merged_job_data
flink.parallelism=2
```

### Test Data Patterns
- **Test IDs**: `test_*`, `integration_*`, `multi_*`, `concurrent_*`, `consistency_*`, `perf_test_*`
- **Cleanup**: All test data is automatically cleaned up after tests

## Manual Testing

### Using SQL Directly
```sql
-- Connect to Doris
mysql -h 127.0.0.1 -P 9030 -u root

-- Run test script
source scripts/doris_upsert_test.sql
```

### Using Maven
```bash
# Run specific test class
mvn test -Dtest=EnhancedPipelineTest

# Run all tests
mvn test

# Run with test properties
mvn test -Dspring.profiles.active=test
```

## Test Results Interpretation

### Success Indicators
- ✅ All assertions pass
- ✅ Records correctly inserted/updated in database
- ✅ No exceptions thrown
- ✅ Performance within acceptable limits

### Failure Indicators
- ❌ Assertion failures
- ❌ Database connection errors
- ❌ Data type conversion errors
- ❌ Performance timeouts

### Common Issues

#### Database Connection Issues
```
Error: Could not connect to Doris
Solution: Ensure Doris is running on 127.0.0.1:9030
```

#### Table Not Found
```
Error: Table 'test_job_analytics.test_merged_job_data' doesn't exist
Solution: Run setup command: ./scripts/run_tests.sh setup
```

#### Configuration Issues
```
Error: Configuration not loaded
Solution: Check application.properties file exists in src/main/resources/
```

## Continuous Integration

### GitHub Actions Example
```yaml
name: Doris Upsert Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Setup Java
        uses: actions/setup-java@v2
        with:
          java-version: '11'
      - name: Setup Doris
        run: |
          # Setup Doris test environment
          docker run -d --name doris-test -p 9030:9030 apache/doris:latest
      - name: Run Tests
        run: ./scripts/run_tests.sh all
```

## Performance Benchmarks

### Expected Performance
- **Unit Tests**: < 1 second
- **Sink Tests**: < 10 seconds
- **Integration Tests**: < 30 seconds
- **Batch Processing**: 100 records/second

### Performance Monitoring
- Monitor memory usage during tests
- Check database connection pool usage
- Verify checkpoint performance
- Monitor garbage collection

## Troubleshooting

### Debug Mode
```bash
# Enable debug logging
export LOG_LEVEL=DEBUG
./scripts/run_tests.sh all
```

### Database Inspection
```sql
-- Check test data
SELECT COUNT(*) FROM test_job_analytics.test_merged_job_data 
WHERE activity_id LIKE 'test_%';

-- Check specific test record
SELECT * FROM test_job_analytics.test_merged_job_data 
WHERE activity_id = 'test_activity_001';
```

### Log Analysis
- Check Flink logs for processing errors
- Monitor Doris logs for connection issues
- Review test output for assertion failures

## Best Practices

1. **Always clean up test data** after running tests
2. **Use test-specific database** to avoid affecting production
3. **Run tests in isolation** to avoid interference
4. **Monitor performance** and set appropriate timeouts
5. **Use realistic test data** that matches production patterns
6. **Test error scenarios** as well as success cases
7. **Document test failures** for debugging purposes
