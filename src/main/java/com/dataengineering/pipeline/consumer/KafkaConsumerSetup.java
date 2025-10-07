package com.dataengineering.pipeline.consumer;

import com.dataengineering.pipeline.config.PipelineConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka consumer setup for multiple topics.
 * Handles consumption of CDC events from various PostgreSQL tables.
 */
public class KafkaConsumerSetup {
    
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerSetup.class);
    
    /**
     * Creates a Kafka consumer for multiple topics and returns a unified DataStream.
     * 
     * @param env Flink StreamExecutionEnvironment
     * @return DataStream<String> containing Kafka messages from all configured topics
     */
    public static DataStream<String> createKafkaConsumer(StreamExecutionEnvironment env) {
        logger.info("Setting up Kafka consumer for topics: {}", String.join(", ", PipelineConfig.KAFKA_TOPICS));
        
        // Create Kafka source with String deserializer
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(PipelineConfig.KAFKA_BOOTSTRAP_SERVERS)
            .setTopics(PipelineConfig.KAFKA_TOPICS)
            .setGroupId(PipelineConfig.KAFKA_GROUP_ID)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();
        
        // Add source to environment
        DataStream<String> kafkaStream = env.fromSource(kafkaSource,
            WatermarkStrategy.noWatermarks(), "Kafka Consumer")
            .setParallelism(PipelineConfig.FLINK_PARALLELISM);
        
        logger.info("Kafka consumer created successfully with parallelism: {}", PipelineConfig.FLINK_PARALLELISM);
        
        return kafkaStream;
    }
    
    /**
     * Creates individual Kafka consumers for each topic.
     * Useful when you need separate processing logic for different topics.
     * 
     * @param env Flink StreamExecutionEnvironment
     * @return Map of topic name to DataStream
     */
    public static java.util.Map<String, DataStream<String>> createIndividualConsumers(StreamExecutionEnvironment env) {
        logger.info("Setting up individual Kafka consumers for each topic");
        
        java.util.Map<String, DataStream<String>> topicStreams = new java.util.HashMap<>();
        
        for (String topic : PipelineConfig.KAFKA_TOPICS) {
            KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(PipelineConfig.KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(topic)
                .setGroupId(PipelineConfig.KAFKA_GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
            
            DataStream<String> stream = env.fromSource(kafkaSource,
                WatermarkStrategy.noWatermarks(), "Kafka Consumer - " + topic)
                .setParallelism(PipelineConfig.FLINK_PARALLELISM);
            
            topicStreams.put(topic, stream);
            logger.info("Created consumer for topic: {}", topic);
        }
        
        return topicStreams;
    }
}
