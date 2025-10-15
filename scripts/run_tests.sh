#!/bin/bash

# Doris Upsert Test Runner
# This script runs comprehensive tests for the Doris upsert functionality

echo "=== Doris Upsert Test Runner ==="
echo "Starting comprehensive testing for CDC-to-Doris pipeline..."

# Set test environment variables
export TEST_ENV=true
export LOG_LEVEL=DEBUG

# Function to run unit tests
run_unit_tests() {
    echo "Running unit tests..."
    mvn test -Dtest=EnhancedPipelineTest
    if [ $? -eq 0 ]; then
        echo "✅ Unit tests passed"
    else
        echo "❌ Unit tests failed"
        return 1
    fi
}

# Function to run sink tests (requires Doris connection)
run_sink_tests() {
    echo "Running Doris JDBC sink tests..."
    echo "Note: This requires a running Doris instance"
    
    # Check if Doris is accessible
    mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT 1" 2>/dev/null
    if [ $? -eq 0 ]; then
        mvn test -Dtest=DorisJdbcSinkTest
        if [ $? -eq 0 ]; then
            echo "✅ Sink tests passed"
        else
            echo "❌ Sink tests failed"
            return 1
        fi
    else
        echo "⚠️  Skipping sink tests - Doris not accessible"
        echo "   Make sure Doris is running on 127.0.0.1:9030"
    fi
}

# Function to run integration tests (requires Doris connection)
run_integration_tests() {
    echo "Running integration tests..."
    echo "Note: This requires a running Doris instance"
    
    # Check if Doris is accessible
    mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT 1" 2>/dev/null
    if [ $? -eq 0 ]; then
        mvn test -Dtest=DorisUpsertIntegrationTest
        if [ $? -eq 0 ]; then
            echo "✅ Integration tests passed"
        else
            echo "❌ Integration tests failed"
            return 1
        fi
    else
        echo "⚠️  Skipping integration tests - Doris not accessible"
        echo "   Make sure Doris is running on 127.0.0.1:9030"
    fi
}

# Function to run SQL tests
run_sql_tests() {
    echo "Running SQL upsert tests..."
    echo "Note: This requires a running Doris instance"
    
    # Check if Doris is accessible
    mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT 1" 2>/dev/null
    if [ $? -eq 0 ]; then
        mysql -h 127.0.0.1 -P 9030 -u root < scripts/doris_upsert_test.sql
        if [ $? -eq 0 ]; then
            echo "✅ SQL tests passed"
        else
            echo "❌ SQL tests failed"
            return 1
        fi
    else
        echo "⚠️  Skipping SQL tests - Doris not accessible"
        echo "   Make sure Doris is running on 127.0.0.1:9030"
    fi
}

# Function to run all tests
run_all_tests() {
    echo "Running all tests..."
    
    run_unit_tests
    run_sink_tests
    run_integration_tests
    run_sql_tests
    
    echo "=== Test Summary ==="
    echo "All tests completed!"
}

# Function to setup test environment
setup_test_env() {
    echo "Setting up test environment..."
    
    # Create test database if it doesn't exist
    mysql -h 127.0.0.1 -P 9030 -u root -e "CREATE DATABASE IF NOT EXISTS test_job_analytics" 2>/dev/null
    
    # Create test table
    mysql -h 127.0.0.1 -P 9030 -u root test_job_analytics -e "
    CREATE TABLE IF NOT EXISTS test_merged_job_data (
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
        \"replication_num\" = \"1\"
    );" 2>/dev/null
    
    echo "✅ Test environment setup complete"
}

# Function to clean up test data
cleanup_test_data() {
    echo "Cleaning up test data..."
    
    mysql -h 127.0.0.1 -P 9030 -u root test_job_analytics -e "
    DELETE FROM test_merged_job_data 
    WHERE activity_id LIKE 'test_%' 
       OR activity_id LIKE 'integration_%' 
       OR activity_id LIKE 'multi_%' 
       OR activity_id LIKE 'concurrent_%' 
       OR activity_id LIKE 'consistency_%' 
       OR activity_id LIKE 'perf_test_%'
       OR activity_id LIKE 'upsert_test_%'
       OR activity_id LIKE 'dup_test_%';" 2>/dev/null
    
    echo "✅ Test data cleanup complete"
}

# Main execution
case "$1" in
    "unit")
        run_unit_tests
        ;;
    "sink")
        run_sink_tests
        ;;
    "integration")
        run_integration_tests
        ;;
    "sql")
        run_sql_tests
        ;;
    "setup")
        setup_test_env
        ;;
    "cleanup")
        cleanup_test_data
        ;;
    "all")
        run_all_tests
        ;;
    *)
        echo "Usage: $0 {unit|sink|integration|sql|setup|cleanup|all}"
        echo ""
        echo "Commands:"
        echo "  unit        - Run unit tests only"
        echo "  sink        - Run Doris JDBC sink tests"
        echo "  integration - Run integration tests"
        echo "  sql         - Run SQL upsert tests"
        echo "  setup       - Setup test environment"
        echo "  cleanup     - Clean up test data"
        echo "  all         - Run all tests"
        echo ""
        echo "Prerequisites:"
        echo "  - Doris running on 127.0.0.1:9030"
        echo "  - MySQL client installed"
        echo "  - Maven installed"
        exit 1
        ;;
esac
