package com.dataengineering.pipeline;

import com.dataengineering.pipeline.config.PipelineConfig;
import com.dataengineering.pipeline.consumer.KafkaConsumerSetup;
import com.dataengineering.pipeline.processor.CdcProcessor;
import com.dataengineering.pipeline.sink.EnhancedDorisJdbcSink;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Streamlined Flink job for direct CDC-to-Doris pipeline.
 * Kafka CDC events -> Direct upsert to Doris via JDBC.
 * No intermediate processing, merging, or enrichment.
 */
public class JobAnalyticsPipeline {
    
    private static final Logger logger = LoggerFactory.getLogger(JobAnalyticsPipeline.class);
    
    public static void main(String[] args) throws Exception {
        logger.info("Starting CDC-to-Doris Pipeline");
        
        // Configure logging
        configureLogging();
        
        // Create Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Configure Flink environment
        configureFlinkEnvironment(env);
        
        // Build the streamlined data pipeline
        buildDataPipeline(env);
        
        // Execute the job
        logger.info("Executing Flink job: CdcToDorisPipeline");
        env.execute("CdcToDorisPipeline");
    }
    
    /**
     * Configures the Flink execution environment with appropriate settings.
     */
    private static void configureFlinkEnvironment(StreamExecutionEnvironment env) {
        // Set parallelism
        env.setParallelism(PipelineConfig.FLINK_PARALLELISM);
        
        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(PipelineConfig.FLINK_CHECKPOINT_INTERVAL_MS);
        
        // Configure checkpoint settings
        // Note: CheckpointingMode.EXACTLY_ONCE is the default for Flink 1.18+
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        
        // Configure restart strategy
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
            3, // Number of restart attempts
            Time.seconds(10) // Delay between restarts
        ));
        
        // Configure buffer timeout for better throughput
        env.setBufferTimeout(100);
        
        logger.info("Flink environment configured with parallelism: {}, checkpoint interval: {}ms", 
                   PipelineConfig.FLINK_PARALLELISM, PipelineConfig.FLINK_CHECKPOINT_INTERVAL_MS);
    }
    
    /**
     * Builds the streamlined CDC-to-Doris data pipeline.
     * Kafka CDC events -> Process -> Direct upsert to Doris.
     */
    private static void buildDataPipeline(StreamExecutionEnvironment env) {
        logger.info("Building streamlined CDC-to-Doris pipeline");
        
        // Step 1: Create Kafka consumer (reuse existing)
        DataStream<String> kafkaStream = KafkaConsumerSetup.createKafkaConsumer(env);
        
        // Step 2: Process CDC events and map to Doris columns
        DataStream<Map<String, Object>> dorisColumnsStream = kafkaStream
            .map(new CdcProcessor())
            .name("CDC Processor")
            .setParallelism(PipelineConfig.FLINK_PARALLELISM);
        
        // Step 3: Selective upsert to Doris via Enhanced JDBC Sink
        dorisColumnsStream
            .addSink(new EnhancedDorisJdbcSink())
            .name("Enhanced Doris JDBC Sink")
            .setParallelism(PipelineConfig.FLINK_PARALLELISM);
        
        logger.info("Streamlined CDC-to-Doris pipeline built successfully");
    }
    
    /**
     * Utility method to add custom logging configuration.
     */
    private static void configureLogging() {
        // Print configuration using the new method
        PipelineConfig.printConfiguration();
    }
}
