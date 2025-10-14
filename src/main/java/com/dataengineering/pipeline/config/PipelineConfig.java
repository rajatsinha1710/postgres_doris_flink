package com.dataengineering.pipeline.config;

import java.util.Properties;

/**
 * Configuration management for the streamlined CDC-to-Doris pipeline.
 * Centralizes all configuration parameters for Kafka and Doris JDBC.
 */
public class PipelineConfig {
    
    // Kafka Configuration
    public static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:29092";
    public static final String KAFKA_GROUP_ID = "flink-cdc-doris-pipeline";
    public static final String KAFKA_AUTO_OFFSET_RESET = "earliest";
    
    // Kafka Topics - CDC events from source tables
    public static final String[] KAFKA_TOPICS = {
        "pgserver1.public.clients_to_exclude",
        "pgserver1.public.job_activity", 
        "pgserver1.public.jobs",
        "pgserver1.public.standard_revenue"
    };
    
    // Doris JDBC Configuration
    public static final String DORIS_JDBC_URL = "jdbc:mysql://127.0.0.1:9030/job_analytics";
    public static final String DORIS_USERNAME = "root";
    public static final String DORIS_PASSWORD = "";
    public static final String DORIS_DATABASE = "job_analytics";
    public static final String DORIS_TABLE = "merged_job_data";
    
    // Doris Table Schema Documentation
    /*
     * Doris Table: merged_job_data
     * Primary Key: activity_id (for upsert operations)
     * 
     * Columns:
     * - activity_id (VARCHAR) - Primary key from job_activity table
     * - job_id (VARCHAR) - From jobs table
     * - jobdiva_no (VARCHAR) - From job_activity table
     * - candidate (VARCHAR) - From job_activity table (candidate_name)
     * - company_name (VARCHAR) - From job_activity or clients_to_exclude table
     * - assignment_start_date (DATETIME) - From job_activity table
     * - assignment_end_date (DATETIME) - From job_activity table
     * - job_title (VARCHAR) - From jobs table
     * - location (VARCHAR) - From jobs table
     * - excluded_reason (VARCHAR) - From clients_to_exclude table
     * - sr_assignment_start (DATETIME) - From standard_revenue table
     * - sr_assignment_end (DATETIME) - From standard_revenue table
     * - standard_revenue_report_month (VARCHAR) - From standard_revenue table
     * - clienterpid (VARCHAR) - From standard_revenue table
     * - bcworkerid (VARCHAR) - From job_activity or standard_revenue table
     * - adjustedstbillrate (DECIMAL) - From standard_revenue table
     * - adjustedstpayrate (DECIMAL) - From standard_revenue table
     * - adjgphrst (DECIMAL) - From standard_revenue table
     * - adjrevenue (DECIMAL) - From standard_revenue table
     * - stbillrate (DECIMAL) - From standard_revenue table
     */
    
    // Flink Configuration
    public static final int FLINK_PARALLELISM = 4;
    public static final long FLINK_CHECKPOINT_INTERVAL_MS = 60000;
    
    /**
     * Creates Kafka consumer properties for Flink Kafka connector.
     */
    public static Properties getKafkaProperties() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
        props.setProperty("group.id", KAFKA_GROUP_ID);
        props.setProperty("auto.offset.reset", KAFKA_AUTO_OFFSET_RESET);
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
    
    /**
     * Creates Doris JDBC connection properties.
     */
    public static Properties getDorisProperties() {
        Properties props = new Properties();
        props.setProperty("user", DORIS_USERNAME);
        props.setProperty("password", DORIS_PASSWORD);
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");
        props.setProperty("serverTimezone", "UTC");
        return props;
    }
}
