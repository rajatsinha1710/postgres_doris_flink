package com.dataengineering.pipeline;

import com.dataengineering.pipeline.config.PipelineConfig;
import com.dataengineering.pipeline.consumer.KafkaConsumerSetup;
import com.dataengineering.pipeline.processor.IdExtractor;
import com.dataengineering.pipeline.processor.PostgresMergeQueryProcessor;
import com.dataengineering.pipeline.sink.DorisSink;
import com.dataengineering.pipeline.sink.DorisJdbcSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Main Flink job orchestration class for the PostgreSQL-Kafka-Doris pipeline.
 * Coordinates the entire data flow from Kafka consumption to Doris loading.
 */
public class JobAnalyticsPipeline {
    
    private static final Logger logger = LoggerFactory.getLogger(JobAnalyticsPipeline.class);
    
    public static void main(String[] args) throws Exception {
        logger.info("Starting Job Analytics Pipeline");
        
        // Configure logging
        configureLogging();
        
        // Create Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Configure Flink environment
        configureFlinkEnvironment(env);
        
        // Build the data pipeline
        buildDataPipeline(env);
        
        // Execute the job
        logger.info("Executing Flink job: JobAnalyticsPipeline");
        env.execute("JobAnalyticsPipeline");
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
     * Builds the complete data pipeline from Kafka to Doris.
     */
    private static void buildDataPipeline(StreamExecutionEnvironment env) {
        logger.info("Building data pipeline");
        
        // Step 1: Create Kafka consumer
        DataStream<String> kafkaStream = KafkaConsumerSetup.createKafkaConsumer(env);
        
        // Step 2: Extract IDs from Kafka CDC events
        DataStream<List<String>> extractedIdsStream = kafkaStream
            .map(new IdExtractor())
            .name("ID Extractor")
            .setParallelism(PipelineConfig.FLINK_PARALLELISM);
        
        // Step 3: Flatten the list of IDs for individual processing
        DataStream<String> individualIdsStream = extractedIdsStream
            .flatMap(new FlatMapFunction<List<String>, String>() {
                @Override
                public void flatMap(List<String> ids, Collector<String> out) {
                    if (ids != null && !ids.isEmpty()) {
                        for (String id : ids) {
                            if (id != null && !id.trim().isEmpty()) {
                                out.collect(id);
                            }
                        }
                    }
                }
            })
            .name("ID Flattener")
            .setParallelism(PipelineConfig.FLINK_PARALLELISM);
        
        // Step 4: Group IDs into batches for PostgreSQL query efficiency
        DataStream<List<String>> batchedIdsStream = individualIdsStream
            .keyBy(id -> Math.abs(id.hashCode()) % PipelineConfig.FLINK_PARALLELISM) // Distribute load evenly
            .window(org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
                .of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))) // Increased window size for better batching
            .apply(new org.apache.flink.streaming.api.functions.windowing.WindowFunction<String, List<String>, Integer, org.apache.flink.streaming.api.windowing.windows.TimeWindow>() {
                @Override
                public void apply(Integer key, org.apache.flink.streaming.api.windowing.windows.TimeWindow window, 
                                Iterable<String> input, Collector<List<String>> out) {
                    List<String> batch = new java.util.ArrayList<>();
                    java.util.Set<String> uniqueIds = new java.util.HashSet<>(); // Deduplicate IDs
                    
                    for (String id : input) {
                        if (id != null && !id.trim().isEmpty() && uniqueIds.add(id)) {
                            batch.add(id);
                        }
                    }
                    
                    if (!batch.isEmpty()) {
                        logger.debug("Created batch of {} unique IDs for key {}", batch.size(), key);
                        out.collect(batch);
                    }
                }
            })
            .name("ID Batcher")
            .setParallelism(PipelineConfig.FLINK_PARALLELISM);
        
        // Step 5: Query PostgreSQL and get merged results
        DataStream<List<com.dataengineering.pipeline.model.MergedResult>> mergedResultsStream = batchedIdsStream
            .map(new PostgresMergeQueryProcessor())
            .name("PostgreSQL Merge Query Processor")
            .setParallelism(PipelineConfig.FLINK_PARALLELISM);
        
        // Step 6: Flatten merged results for individual Doris sink processing
        DataStream<com.dataengineering.pipeline.model.MergedResult> individualResultsStream = mergedResultsStream
            .flatMap(new FlatMapFunction<List<com.dataengineering.pipeline.model.MergedResult>, com.dataengineering.pipeline.model.MergedResult>() {
                @Override
                public void flatMap(List<com.dataengineering.pipeline.model.MergedResult> results, 
                                  Collector<com.dataengineering.pipeline.model.MergedResult> out) {
                    if (results != null && !results.isEmpty()) {
                        for (com.dataengineering.pipeline.model.MergedResult result : results) {
                            if (result != null) {
                                out.collect(result);
                            }
                        }
                    }
                }
            })
            .name("Results Flattener")
            .setParallelism(PipelineConfig.FLINK_PARALLELISM);
        
        // Step 7: Add watermarks for event time processing (optional)
        DataStream<com.dataengineering.pipeline.model.MergedResult> watermarkedStream = individualResultsStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<com.dataengineering.pipeline.model.MergedResult>forBoundedOutOfOrderness(
                    java.time.Duration.ofSeconds(5))
                .withTimestampAssigner((event, timestamp) -> 
                    event.getAssignmentStartDate() != null ? 
                        event.getAssignmentStartDate().atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli() :
                        System.currentTimeMillis())
            )
            .name("Watermark Assigner");
        
        // Step 8: Send to Doris sink (using JDBC for reliable data loading)
        watermarkedStream
            .addSink(new DorisJdbcSink())
            .name("Doris JDBC Sink")
            .setParallelism(PipelineConfig.FLINK_PARALLELISM);
        
        logger.info("Data pipeline built successfully");
    }
    
    /**
     * Utility method to add custom logging configuration.
     */
    private static void configureLogging() {
        // Log pipeline configuration
        logger.info("Pipeline Configuration:");
        logger.info("  Kafka Bootstrap Servers: {}", PipelineConfig.KAFKA_BOOTSTRAP_SERVERS);
        logger.info("  Kafka Topics: {}", String.join(", ", PipelineConfig.KAFKA_TOPICS));
        logger.info("  PostgreSQL URL: {}", PipelineConfig.POSTGRES_URL);
        logger.info("  Doris Stream Load URL: {}", PipelineConfig.DORIS_STREAM_LOAD_URL);
        logger.info("  Flink Parallelism: {}", PipelineConfig.FLINK_PARALLELISM);
        logger.info("  Doris Batch Size: {}", PipelineConfig.DORIS_BATCH_SIZE);
    }
}
