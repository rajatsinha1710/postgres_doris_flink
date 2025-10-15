package com.dataengineering.pipeline.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Configuration management for the streamlined CDC-to-Doris pipeline.
 * Centralizes all configuration parameters for Kafka and Doris JDBC.
 * Reads configuration from application.properties file.
 */
public class PipelineConfig {
    
    private static final Properties properties = new Properties();
    private static boolean initialized = false;
    
    // Initialize properties from application.properties
    static {
        loadProperties();
    }
    
    // Kafka Configuration
    public static final String KAFKA_BOOTSTRAP_SERVERS = getProperty("kafka.bootstrap.servers", "localhost:29092");
    public static final String KAFKA_GROUP_ID = getProperty("kafka.group.id", "flink-cdc-doris-pipeline");
    public static final String KAFKA_AUTO_OFFSET_RESET = getProperty("kafka.auto.offset.reset", "earliest");
    
    // Kafka Topics - CDC events from source tables
    public static final String[] KAFKA_TOPICS = getProperty("kafka.topics", 
        "pgserver1.public.clients_to_exclude,pgserver1.public.job_activity,pgserver1.public.jobs,pgserver1.public.standard_revenue")
        .split(",");
    
    // Doris JDBC Configuration
    public static final String DORIS_JDBC_URL = getProperty("doris.jdbc.url", "jdbc:mysql://127.0.0.1:9030/job_analytics");
    public static final String DORIS_USERNAME = getProperty("doris.username", "root");
    public static final String DORIS_PASSWORD = getProperty("doris.password", "");
    public static final String DORIS_DATABASE = getProperty("doris.database", "job_analytics");
    public static final String DORIS_TABLE = getProperty("doris.table", "merged_job_data");
    
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
    public static final int FLINK_PARALLELISM = Integer.parseInt(getProperty("flink.parallelism", "4"));
    public static final long FLINK_CHECKPOINT_INTERVAL_MS = Long.parseLong(getProperty("flink.checkpoint.interval.ms", "60000"));
    
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
        props.setProperty("useSSL", getProperty("doris.jdbc.useSSL", "false"));
        props.setProperty("allowPublicKeyRetrieval", getProperty("doris.jdbc.allowPublicKeyRetrieval", "true"));
        props.setProperty("serverTimezone", getProperty("doris.jdbc.serverTimezone", "UTC"));
        return props;
    }
    
    /**
     * Loads properties from application.properties file.
     */
    private static void loadProperties() {
        if (initialized) {
            return;
        }
        
        try (InputStream input = PipelineConfig.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                System.err.println("Warning: application.properties file not found. Using default values.");
                initialized = true;
                return;
            }
            
            properties.load(input);
            initialized = true;
            System.out.println("Configuration loaded from application.properties");
            
        } catch (IOException e) {
            System.err.println("Error loading application.properties: " + e.getMessage());
            System.err.println("Using default configuration values.");
            initialized = true;
        }
    }
    
    /**
     * Gets a property value with a default fallback.
     */
    private static String getProperty(String key, String defaultValue) {
        if (!initialized) {
            loadProperties();
        }
        return properties.getProperty(key, defaultValue);
    }
    
    /**
     * Prints current configuration for debugging purposes.
     */
    public static void printConfiguration() {
        System.out.println("=== Pipeline Configuration ===");
        System.out.println("Kafka Bootstrap Servers: " + KAFKA_BOOTSTRAP_SERVERS);
        System.out.println("Kafka Group ID: " + KAFKA_GROUP_ID);
        System.out.println("Kafka Topics: " + String.join(", ", KAFKA_TOPICS));
        System.out.println("Doris JDBC URL: " + DORIS_JDBC_URL);
        System.out.println("Doris Database: " + DORIS_DATABASE);
        System.out.println("Doris Table: " + DORIS_TABLE);
        System.out.println("Flink Parallelism: " + FLINK_PARALLELISM);
        System.out.println("Flink Checkpoint Interval: " + FLINK_CHECKPOINT_INTERVAL_MS + "ms");
        System.out.println("=============================");
    }
}
