package com.dataengineering.pipeline;

import com.dataengineering.pipeline.model.MergedResult;
import com.dataengineering.pipeline.processor.IdExtractor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalTime;
import java.math.BigDecimal;
import java.util.List;

/**
 * Simple test class to demonstrate the pipeline components.
 * This class shows how to test individual components without running the full pipeline.
 */
public class PipelineTest {
    
    private static final Logger logger = LoggerFactory.getLogger(PipelineTest.class);
    
    public static void main(String[] args) {
        logger.info("Starting Pipeline Test");
        
        // Test MergedResult serialization
        testMergedResultSerialization();
        
        // Test ID extraction
        testIdExtraction();
        
        logger.info("Pipeline Test completed successfully");
    }
    
    /**
     * Tests MergedResult object creation and JSON serialization.
     */
    private static void testMergedResultSerialization() {
        logger.info("Testing MergedResult serialization");
        
        MergedResult result = new MergedResult();
        result.setActivityId("ACT001");
        result.setJobId("JOB001");
        result.setJobdivaNo("JD001");
        result.setCandidate("John Doe");
        result.setCompanyName("Test Company");
        result.setAssignmentStartDate(LocalDate.of(2024, 1, 1).atTime(LocalTime.MIDNIGHT));
        result.setAssignmentEndDate(LocalDate.of(2024, 12, 31).atTime(LocalTime.MIDNIGHT));
        result.setJobTitle("Software Engineer");
        result.setLocation("New York");
        result.setExcludedReason(null);
        result.setSrAssignmentStart(LocalDate.of(2024, 1, 1).atTime(LocalTime.MIDNIGHT));
        result.setSrAssignmentEnd(LocalDate.of(2024, 12, 31).atTime(LocalTime.MIDNIGHT));
        result.setStandardRevenueReportMonth("2024-01");
        result.setClienterpid("CLIENT001");
        result.setBcworkerid("WORKER001");
        result.setAdjustedstbillrate(new BigDecimal("100.00"));
        result.setAdjustedstpayrate(new BigDecimal("80.00"));
        result.setAdjgphrst(new BigDecimal("40.0"));
        result.setAdjrevenue(new BigDecimal("4000.00"));
        result.setStbillrate(new BigDecimal("100.00"));
        
        String json = result.toJson();
        logger.info("MergedResult JSON: {}", json);
        
        assert json != null && !json.isEmpty() : "JSON serialization failed";
        logger.info("MergedResult serialization test passed");
    }
    
    /**
     * Tests ID extraction from sample Kafka CDC messages.
     */
    private static void testIdExtraction() {
        logger.info("Testing ID extraction");
        
        IdExtractor extractor = new IdExtractor();
        
        // Sample Debezium CDC message for job_activity table
        String sampleCdcMessage = 
            "{\n" +
            "    \"payload\": {\n" +
            "        \"before\": null,\n" +
            "        \"after\": {\n" +
            "            \"activity_id\": \"ACT001\",\n" +
            "            \"job_id\": \"JOB001\",\n" +
            "            \"jobdiva_no\": \"JD001\",\n" +
            "            \"candidate\": \"John Doe\",\n" +
            "            \"company_name\": \"Test Company\"\n" +
            "        },\n" +
            "        \"source\": {\n" +
            "            \"table\": \"job_activity\",\n" +
            "            \"schema\": \"public\"\n" +
            "        },\n" +
            "        \"op\": \"c\"\n" +
            "    }\n" +
            "}";
        
        try {
            List<String> extractedIds = extractor.map(sampleCdcMessage);
            logger.info("Extracted IDs: {}", extractedIds);
            
            assert extractedIds != null && !extractedIds.isEmpty() : "ID extraction failed";
            assert extractedIds.contains("ACT001") : "Activity ID not extracted";
            assert extractedIds.contains("JOB001") : "Job ID not extracted";
            
            logger.info("ID extraction test passed");
            
        } catch (Exception e) {
            logger.error("ID extraction test failed", e);
            throw new RuntimeException("ID extraction test failed", e);
        }
    }
    
    /**
     * Creates a sample Flink environment for testing.
     * This method demonstrates how to set up a minimal Flink job for testing.
     */
    public static void createSampleFlinkJob() {
        logger.info("Creating sample Flink job for testing");
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Create a simple data stream for testing
        DataStream<String> testStream = env.fromElements(
            "test-message-1",
            "test-message-2",
            "test-message-3"
        );
        
        // Apply ID extraction
        DataStream<List<String>> extractedIds = testStream.map(new IdExtractor());
        
        // Print results
        extractedIds.print();
        
        logger.info("Sample Flink job created (not executed in test mode)");
    }
}
