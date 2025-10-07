package com.dataengineering.pipeline.config;

import java.util.Properties;

/**
 * Configuration management for the Flink pipeline.
 * Centralizes all configuration parameters for Kafka, PostgreSQL, and Doris.
 */
public class PipelineConfig {
    
    // Kafka Configuration
    public static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:29092";
    public static final String KAFKA_GROUP_ID = "flink-postgres-doris-pipeline";
    public static final String KAFKA_AUTO_OFFSET_RESET = "earliest";
    
    // Kafka Topics - CDC events from PostgreSQL
    public static final String[] KAFKA_TOPICS = {
        "pgserver1.public.clients_to_exclude",
        "pgserver1.public.job_activity", 
        "pgserver1.public.jobs",
        "pgserver1.public.standard_revenue"
    };
    
    // PostgreSQL Configuration
    public static final String POSTGRES_URL = "jdbc:postgresql://localhost:5432/postgres";
    public static final String POSTGRES_USERNAME = "rajatsinha";
    public static final String POSTGRES_PASSWORD = "";
    public static final int POSTGRES_MAX_CONNECTIONS = 10;
    
    // Doris Configuration
    public static final String DORIS_STREAM_LOAD_URL = "http://localhost:8030/api/job_analytics/merged_job_data/_stream_load";
    public static final String DORIS_USERNAME = "root";
    public static final String DORIS_PASSWORD = "";
    public static final int DORIS_BATCH_SIZE = 50; // Reduced batch size for better error handling
    public static final int DORIS_MAX_RETRIES = 5; // Increased retries
    public static final long DORIS_RETRY_DELAY_MS = 2000; // Increased delay
    
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
     * Creates PostgreSQL connection properties.
     */
    public static Properties getPostgresProperties() {
        Properties props = new Properties();
        props.setProperty("user", POSTGRES_USERNAME);
        props.setProperty("password", POSTGRES_PASSWORD);
        props.setProperty("ssl", "false");
        props.setProperty("maxConnections", String.valueOf(POSTGRES_MAX_CONNECTIONS));
        return props;
    }
}
