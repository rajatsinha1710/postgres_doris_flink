package com.dataengineering.pipeline.test;

import com.dataengineering.pipeline.config.PipelineConfig;
import com.dataengineering.pipeline.model.CdcEvent;
import com.dataengineering.pipeline.sink.DorisJdbcSink;
import org.junit.Before;
import org.junit.Test;
import org.junit.After;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.List;
import java.util.Map;

/**
 * Integration tests for end-to-end Doris upsert functionality.
 * Tests the complete pipeline from CDC event processing to Doris database upserts.
 */
public class DorisUpsertIntegrationTest {
    
    private Connection testConnection;
    private DorisJdbcSink sink;
    
    @Before
    public void setUp() throws Exception {
        // Initialize database connection
        Class.forName("com.mysql.cj.jdbc.Driver");
        testConnection = DriverManager.getConnection(
            PipelineConfig.DORIS_JDBC_URL,
            PipelineConfig.DORIS_USERNAME,
            PipelineConfig.DORIS_PASSWORD
        );
        
        // Initialize components
        sink = new DorisJdbcSink();
        
        // Clean up any existing test data
        cleanupTestData();
    }
    
    @After
    public void tearDown() throws Exception {
        cleanupTestData();
        if (testConnection != null && !testConnection.isClosed()) {
            testConnection.close();
        }
    }
    
    /**
     * Test complete pipeline: CDC event -> processing -> Doris upsert.
     */
    @Test
    public void testCompletePipelineJobActivity() throws Exception {
        // Generate test CDC event
        String cdcEventJson = TestDataGenerator.generateJobActivityCdcEvent("integration_test_1", "c");
        
        // Process CDC event
        CdcEvent event = CdcEvent.fromJson(cdcEventJson);
        assertNotNull("CDC event should be parsed", event);
        
        Map<String, Object> dorisColumns = event.getDorisColumns();
        assertNotNull("Doris columns should be generated", dorisColumns);
        assertFalse("Doris columns should not be empty", dorisColumns.isEmpty());
        
        // Execute upsert
        sink.open(null);
        sink.invoke(dorisColumns, null);
        sink.close();
        
        // Verify record in database
        Map<String, Object> dbRecord = getRecordFromDatabase("integration_test_1");
        assertNotNull("Record should exist in database", dbRecord);
        assertEquals("integration_test_1", dbRecord.get("activity_id"));
        assertNotNull("Job ID should be set", dbRecord.get("job_id"));
        assertNotNull("Candidate should be set", dbRecord.get("candidate"));
        assertNotNull("Company name should be set", dbRecord.get("company_name"));
    }
    
    /**
     * Test multi-table upsert scenario.
     */
    @Test
    public void testMultiTableUpsertScenario() throws Exception {
        String activityId = "multi_table_test_1";
        
        // Step 1: Insert job_activity data
        String jobActivityEvent = TestDataGenerator.generateJobActivityCdcEvent(activityId, "c");
        processAndUpsert(jobActivityEvent);
        
        // Verify initial insert
        Map<String, Object> initialRecord = getRecordFromDatabase(activityId);
        assertNotNull("Initial record should exist", initialRecord);
        assertNotNull("Candidate should be set", initialRecord.get("candidate"));
        assertNull("Job title should be null initially", initialRecord.get("job_title"));
        
        // Step 2: Update with jobs data
        String jobsEvent = TestDataGenerator.generateJobsCdcEvent("JOB_001", "u");
        processAndUpsert(jobsEvent);
        
        // Verify partial update
        Map<String, Object> updatedRecord = getRecordFromDatabase(activityId);
        assertNotNull("Job title should be updated", updatedRecord.get("job_title"));
        assertNotNull("Location should be updated", updatedRecord.get("location"));
        assertNotNull("Candidate should remain unchanged", updatedRecord.get("candidate"));
        
        // Step 3: Update with revenue data
        String revenueEvent = TestDataGenerator.generateStandardRevenueCdcEvent("BC001", "u");
        processAndUpsert(revenueEvent);
        
        // Verify revenue update
        Map<String, Object> finalRecord = getRecordFromDatabase(activityId);
        assertNotNull("Revenue data should be updated", finalRecord.get("adjustedstbillrate"));
        assertNotNull("Revenue should be updated", finalRecord.get("adjrevenue"));
        assertNotNull("Job title should remain unchanged", finalRecord.get("job_title"));
    }
    
    /**
     * Test batch processing with multiple records.
     */
    @Test
    public void testBatchProcessing() throws Exception {
        List<String> batchEvents = TestDataGenerator.generateBatchTestEvents(10);
        
        sink.open(null);
        
        for (String eventJson : batchEvents) {
            CdcEvent event = CdcEvent.fromJson(eventJson);
            if (event != null) {
                Map<String, Object> dorisColumns = event.getDorisColumns();
                if (dorisColumns != null && !dorisColumns.isEmpty()) {
                    sink.invoke(dorisColumns, null);
                }
            }
        }
        
        sink.close();
        
        // Verify batch processing results
        int recordCount = getTestRecordCount();
        assertTrue("Should have processed some records", recordCount > 0);
        
        // Verify specific test records exist
        Map<String, Object> testRecord = getRecordFromDatabase("test_activity_000");
        if (testRecord != null) {
            assertNotNull("Test record should have activity_id", testRecord.get("activity_id"));
        }
    }
    
    /**
     * Test error handling and recovery.
     */
    @Test
    public void testErrorHandling() throws Exception {
        sink.open(null);
        
        // Test with invalid data
        sink.invoke(null, null); // Should not throw exception
        
        Map<String, Object> emptyData = new java.util.HashMap<>();
        sink.invoke(emptyData, null); // Should not throw exception
        
        // Test with malformed CDC event
        String invalidJson = "{ invalid json }";
        CdcEvent invalidEvent = CdcEvent.fromJson(invalidJson);
        assertNull("Invalid JSON should return null", invalidEvent);
        
        sink.close();
    }
    
    /**
     * Test performance with larger dataset.
     */
    @Test
    public void testPerformanceWithLargeDataset() throws Exception {
        int recordCount = 100;
        List<String> performanceEvents = TestDataGenerator.generatePerformanceTestData(recordCount);
        
        long startTime = System.currentTimeMillis();
        
        sink.open(null);
        
        int processedCount = 0;
        for (String eventJson : performanceEvents) {
            CdcEvent event = CdcEvent.fromJson(eventJson);
            if (event != null) {
                Map<String, Object> dorisColumns = event.getDorisColumns();
                if (dorisColumns != null && !dorisColumns.isEmpty()) {
                    sink.invoke(dorisColumns, null);
                    processedCount++;
                }
            }
        }
        
        sink.close();
        
        long endTime = System.currentTimeMillis();
        long processingTime = endTime - startTime;
        
        System.out.println("Processed " + processedCount + " records in " + processingTime + "ms");
        System.out.println("Average processing time: " + (processingTime / (double) processedCount) + "ms per record");
        
        assertTrue("Should process records within reasonable time", processingTime < 30000); // 30 seconds max
        assertTrue("Should process most records", processedCount > recordCount * 0.8);
    }
    
    /**
     * Test concurrent upsert scenarios.
     */
    @Test
    public void testConcurrentUpsertScenarios() throws Exception {
        String activityId = "concurrent_test_1";
        
        // Create multiple events for the same activity_id from different tables
        String jobActivityEvent = TestDataGenerator.generateJobActivityCdcEvent(activityId, "c");
        String jobsEvent = TestDataGenerator.generateJobsCdcEvent("JOB_001", "u");
        String revenueEvent = TestDataGenerator.generateStandardRevenueCdcEvent("BC001", "u");
        
        sink.open(null);
        
        // Process events in sequence (simulating concurrent processing)
        processEvent(jobActivityEvent);
        processEvent(jobsEvent);
        processEvent(revenueEvent);
        
        sink.close();
        
        // Verify final state
        Map<String, Object> finalRecord = getRecordFromDatabase(activityId);
        assertNotNull("Final record should exist", finalRecord);
        
        // Should have data from all sources
        assertNotNull("Should have candidate from job_activity", finalRecord.get("candidate"));
        assertNotNull("Should have job_title from jobs", finalRecord.get("job_title"));
        assertNotNull("Should have revenue data from standard_revenue", finalRecord.get("adjustedstbillrate"));
    }
    
    /**
     * Test data consistency after multiple upserts.
     */
    @Test
    public void testDataConsistency() throws Exception {
        String activityId = "consistency_test_1";
        
        // Create initial record
        String initialEvent = TestDataGenerator.generateJobActivityCdcEvent(activityId, "c");
        processAndUpsert(initialEvent);
        
        Map<String, Object> initialRecord = getRecordFromDatabase(activityId);
        String initialCandidate = (String) initialRecord.get("candidate");
        String initialCompany = (String) initialRecord.get("company_name");
        
        // Update with jobs data
        String jobsEvent = TestDataGenerator.generateJobsCdcEvent("JOB_001", "u");
        processAndUpsert(jobsEvent);
        
        Map<String, Object> updatedRecord = getRecordFromDatabase(activityId);
        
        // Verify data consistency
        assertEquals("Candidate should remain unchanged", initialCandidate, updatedRecord.get("candidate"));
        assertEquals("Company should remain unchanged", initialCompany, updatedRecord.get("company_name"));
        assertNotNull("Job title should be added", updatedRecord.get("job_title"));
        assertNotNull("Location should be added", updatedRecord.get("location"));
    }
    
    // Helper methods
    
    private void processAndUpsert(String cdcEventJson) throws Exception {
        CdcEvent event = CdcEvent.fromJson(cdcEventJson);
        if (event != null) {
            Map<String, Object> dorisColumns = event.getDorisColumns();
            if (dorisColumns != null && !dorisColumns.isEmpty()) {
                sink.invoke(dorisColumns, null);
            }
        }
    }
    
    private void processEvent(String cdcEventJson) throws Exception {
        CdcEvent event = CdcEvent.fromJson(cdcEventJson);
        if (event != null) {
            Map<String, Object> dorisColumns = event.getDorisColumns();
            if (dorisColumns != null && !dorisColumns.isEmpty()) {
                sink.invoke(dorisColumns, null);
            }
        }
    }
    
    private Map<String, Object> getRecordFromDatabase(String activityId) throws SQLException {
        String sql = "SELECT * FROM " + PipelineConfig.DORIS_TABLE + " WHERE activity_id = ?";
        try (PreparedStatement stmt = testConnection.prepareStatement(sql)) {
            stmt.setString(1, activityId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    Map<String, Object> result = new java.util.HashMap<>();
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        Object value = rs.getObject(i);
                        result.put(columnName, value);
                    }
                    return result;
                }
            }
        }
        return null;
    }
    
    private int getTestRecordCount() throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + PipelineConfig.DORIS_TABLE + " WHERE activity_id LIKE 'test_%' OR activity_id LIKE 'integration_%' OR activity_id LIKE 'multi_%' OR activity_id LIKE 'concurrent_%' OR activity_id LIKE 'consistency_%' OR activity_id LIKE 'perf_test_%'";
        try (PreparedStatement stmt = testConnection.prepareStatement(sql)) {
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        }
        return 0;
    }
    
    private void cleanupTestData() throws SQLException {
        String sql = "DELETE FROM " + PipelineConfig.DORIS_TABLE + " WHERE activity_id LIKE 'test_%' OR activity_id LIKE 'integration_%' OR activity_id LIKE 'multi_%' OR activity_id LIKE 'concurrent_%' OR activity_id LIKE 'consistency_%' OR activity_id LIKE 'perf_test_%'";
        try (PreparedStatement stmt = testConnection.prepareStatement(sql)) {
            stmt.executeUpdate();
        }
    }
}
