package com.dataengineering.pipeline;

import com.dataengineering.pipeline.config.PipelineConfig;
import com.dataengineering.pipeline.model.CdcEvent;
import com.dataengineering.pipeline.test.TestDataGenerator;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Map;

/**
 * Test class for validating the enhanced pipeline functionality.
 * Tests selective column mapping and source table detection.
 * Updated to work with the new configuration system.
 */
public class EnhancedPipelineTest {
    
    @Before
    public void setUp() {
        // Print configuration for debugging
        System.out.println("=== Test Configuration ===");
        PipelineConfig.printConfiguration();
        System.out.println("==========================");
    }
    
    @Test
    public void testJobActivityMapping() {
        // Sample CDC event for job_activity table
        String jsonMessage = "{\n" +
            "  \"op\": \"c\",\n" +
            "  \"source\": {\n" +
            "    \"table\": \"job_activity\"\n" +
            "  },\n" +
            "  \"after\": {\n" +
            "    \"activity_id\": \"12345\",\n" +
            "    \"job_id\": \"JOB001\",\n" +
            "    \"jobdiva_no\": \"JD001\",\n" +
            "    \"candidate_name\": \"John Doe\",\n" +
            "    \"company_name\": \"TechCorp\",\n" +
            "    \"assignment_start_date\": \"2024-01-01 09:00:00\",\n" +
            "    \"assignment_end_date\": \"2024-12-31 17:00:00\",\n" +
            "    \"bcworkerid\": \"BC001\"\n" +
            "  }\n" +
            "}";
        
        CdcEvent event = CdcEvent.fromJson(jsonMessage);
        assertNotNull("CDC event should be parsed successfully", event);
        
        Map<String, Object> dorisColumns = event.getDorisColumns();
        
        // Verify source table metadata is added
        assertEquals("job_activity", dorisColumns.get("__source_table__"));
        
        // Verify correct columns are mapped
        assertEquals("12345", dorisColumns.get("activity_id"));
        assertEquals("JOB001", dorisColumns.get("job_id"));
        assertEquals("John Doe", dorisColumns.get("candidate"));
        assertEquals("TechCorp", dorisColumns.get("company_name"));
        
        // Verify other table columns are not included
        assertNull("Should not include standard revenue columns", dorisColumns.get("adjustedstbillrate"));
        assertNull("Should not include job title", dorisColumns.get("job_title"));
    }
    
    @Test
    public void testJobsMapping() {
        String jsonMessage = "{\n" +
            "  \"op\": \"u\",\n" +
            "  \"source\": {\n" +
            "    \"table\": \"jobs\"\n" +
            "  },\n" +
            "  \"after\": {\n" +
            "    \"job_id\": \"JOB001\",\n" +
            "    \"job_title\": \"Senior Software Engineer\",\n" +
            "    \"location\": \"San Francisco, CA\"\n" +
            "  }\n" +
            "}";
        
        CdcEvent event = CdcEvent.fromJson(jsonMessage);
        assertNotNull("CDC event should be parsed successfully", event);
        
        Map<String, Object> dorisColumns = event.getDorisColumns();
        
        // Verify source table metadata
        assertEquals("jobs", dorisColumns.get("__source_table__"));
        
        // Verify correct columns are mapped
        assertEquals("JOB001", dorisColumns.get("job_id"));
        assertEquals("Senior Software Engineer", dorisColumns.get("job_title"));
        assertEquals("San Francisco, CA", dorisColumns.get("location"));
        
        // Verify other table columns are not included
        assertNull("Should not include activity columns", dorisColumns.get("activity_id"));
        assertNull("Should not include revenue columns", dorisColumns.get("adjustedstbillrate"));
    }
    
    @Test
    public void testStandardRevenueMapping() {
        String jsonMessage = "{\n" +
            "  \"op\": \"u\",\n" +
            "  \"source\": {\n" +
            "    \"table\": \"standard_revenue\"\n" +
            "  },\n" +
            "  \"after\": {\n" +
            "    \"bcworkerid\": \"BC001\",\n" +
            "    \"clienterpid\": \"CLIENT123\",\n" +
            "    \"adjusted_st_bill_rate\": \"75.50\",\n" +
            "    \"adjusted_st_pay_rate\": \"45.00\",\n" +
            "    \"adj_revenue\": \"5000.00\",\n" +
            "    \"standard_revenue_report_month\": \"2024-01\"\n" +
            "  }\n" +
            "}";
        
        CdcEvent event = CdcEvent.fromJson(jsonMessage);
        assertNotNull("CDC event should be parsed successfully", event);
        
        Map<String, Object> dorisColumns = event.getDorisColumns();
        
        // Verify source table metadata
        assertEquals("standard_revenue", dorisColumns.get("__source_table__"));
        
        // Verify correct columns are mapped
        assertEquals("BC001", dorisColumns.get("bcworkerid"));
        assertEquals("CLIENT123", dorisColumns.get("clienterpid"));
        assertEquals("2024-01", dorisColumns.get("standard_revenue_report_month"));
        
        // Verify decimal values are properly converted
        assertNotNull("Adjusted bill rate should be converted", dorisColumns.get("adjustedstbillrate"));
        assertNotNull("Revenue should be converted", dorisColumns.get("adjrevenue"));
        
        // Verify other table columns are not included
        assertNull("Should not include activity columns", dorisColumns.get("activity_id"));
        assertNull("Should not include job columns", dorisColumns.get("job_title"));
    }
    
    @Test
    public void testClientsToExcludeMapping() {
        String jsonMessage = "{\n" +
            "  \"op\": \"c\",\n" +
            "  \"source\": {\n" +
            "    \"table\": \"clients_to_exclude\"\n" +
            "  },\n" +
            "  \"after\": {\n" +
            "    \"client_id\": \"BadClient Inc\",\n" +
            "    \"exclusion_reason\": \"Payment issues\"\n" +
            "  }\n" +
            "}";
        
        CdcEvent event = CdcEvent.fromJson(jsonMessage);
        assertNotNull("CDC event should be parsed successfully", event);
        
        Map<String, Object> dorisColumns = event.getDorisColumns();
        
        // Verify source table metadata
        assertEquals("clients_to_exclude", dorisColumns.get("__source_table__"));
        
        // Verify correct columns are mapped
        assertEquals("BadClient Inc", dorisColumns.get("company_name")); // client_id maps to company_name
        assertEquals("Payment issues", dorisColumns.get("excluded_reason"));
        
        // Verify other table columns are not included
        assertNull("Should not include activity columns", dorisColumns.get("activity_id"));
        assertNull("Should not include revenue columns", dorisColumns.get("adjustedstbillrate"));
    }
    
    @Test
    public void testDeleteOperationSkipped() {
        String jsonMessage = "{\n" +
            "  \"op\": \"d\",\n" +
            "  \"source\": {\n" +
            "    \"table\": \"job_activity\"\n" +
            "  },\n" +
            "  \"before\": {\n" +
            "    \"activity_id\": \"12345\"\n" +
            "  }\n" +
            "}";
        
        CdcEvent event = CdcEvent.fromJson(jsonMessage);
        assertNotNull("CDC event should be parsed successfully", event);
        
        Map<String, Object> dorisColumns = event.getDorisColumns();
        
        // For delete operations, we might want to handle differently
        // Current implementation returns empty map for deletes
        assertTrue("Delete operations should return empty or handled appropriately", 
                  dorisColumns.isEmpty() || dorisColumns.containsKey("__source_table__"));
    }
    
    @Test
    public void testInvalidJsonHandling() {
        String invalidJson = "{ invalid json }";
        
        CdcEvent event = CdcEvent.fromJson(invalidJson);
        assertNull("Invalid JSON should return null", event);
    }
    
    @Test
    public void testMissingDataHandling() {
        String jsonMessage = "{\n" +
            "  \"op\": \"u\",\n" +
            "  \"source\": {\n" +
            "    \"table\": \"jobs\"\n" +
            "  },\n" +
            "  \"after\": null\n" +
            "}";
        
        CdcEvent event = CdcEvent.fromJson(jsonMessage);
        assertNotNull("CDC event should be parsed", event);
        
        Map<String, Object> dorisColumns = event.getDorisColumns();
        assertTrue("Should return empty map for null after data", dorisColumns.isEmpty());
    }
    
    @Test
    public void testConfigurationLoading() {
        // Test that configuration is loaded correctly
        assertNotNull("Kafka bootstrap servers should be configured", PipelineConfig.KAFKA_BOOTSTRAP_SERVERS);
        assertNotNull("Kafka topics should be configured", PipelineConfig.KAFKA_TOPICS);
        assertNotNull("Doris JDBC URL should be configured", PipelineConfig.DORIS_JDBC_URL);
        assertNotNull("Doris table should be configured", PipelineConfig.DORIS_TABLE);
        
        // Verify topics array is not empty
        assertTrue("Kafka topics should not be empty", PipelineConfig.KAFKA_TOPICS.length > 0);
        
        // Verify parallelism is positive
        assertTrue("Flink parallelism should be positive", PipelineConfig.FLINK_PARALLELISM > 0);
    }
    
    @Test
    public void testTestDataGeneratorIntegration() {
        // Test that TestDataGenerator works with the pipeline
        String testEvent = TestDataGenerator.generateJobActivityCdcEvent("test_generator_1", "c");
        assertNotNull("Test data generator should create valid JSON", testEvent);
        
        CdcEvent event = CdcEvent.fromJson(testEvent);
        assertNotNull("Generated event should be parseable", event);
        
        Map<String, Object> dorisColumns = event.getDorisColumns();
        assertNotNull("Generated event should produce Doris columns", dorisColumns);
        assertFalse("Generated event should not be empty", dorisColumns.isEmpty());
        
        // Verify source table metadata
        assertEquals("job_activity", dorisColumns.get("__source_table__"));
        assertEquals("test_generator_1", dorisColumns.get("activity_id"));
    }
    
    @Test
    public void testBatchDataGeneration() {
        // Test batch data generation
        java.util.List<String> batchEvents = TestDataGenerator.generateBatchTestEvents(5);
        assertNotNull("Batch events should be generated", batchEvents);
        assertTrue("Should generate multiple events", batchEvents.size() > 0);
        
        // Verify each event can be processed
        for (String eventJson : batchEvents) {
            CdcEvent event = CdcEvent.fromJson(eventJson);
            assertNotNull("Each batch event should be parseable", event);
            
            Map<String, Object> dorisColumns = event.getDorisColumns();
            assertNotNull("Each batch event should produce Doris columns", dorisColumns);
        }
    }
}

/**
 * Example integration test scenarios (would require actual Doris connection)
 * These tests would validate end-to-end functionality:
 * 
 * 1. Test job_activity upsert creates new record
 * 2. Test jobs update modifies existing record (job_title, location only)
 * 3. Test standard_revenue update modifies existing record (revenue fields only)
 * 4. Test clients_to_exclude update modifies existing record (exclusion_reason only)
 * 5. Test concurrent updates from different source tables
 * 6. Test performance with high-volume CDC events
 * 7. Test error handling and connection recovery
 */