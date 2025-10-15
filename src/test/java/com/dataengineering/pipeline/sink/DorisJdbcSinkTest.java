package com.dataengineering.pipeline.sink;

import com.dataengineering.pipeline.config.PipelineConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.After;
import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for DorisJdbcSink upsert functionality.
 * Tests the core upsert logic with mock data and real database connections.
 */
public class DorisJdbcSinkTest {
    
    private DorisJdbcSink sink;
    private Connection testConnection;
    
    @Before
    public void setUp() throws Exception {
        sink = new DorisJdbcSink();
        
        // Initialize test connection
        Class.forName("com.mysql.cj.jdbc.Driver");
        testConnection = DriverManager.getConnection(
            PipelineConfig.DORIS_JDBC_URL,
            PipelineConfig.DORIS_USERNAME,
            PipelineConfig.DORIS_PASSWORD
        );
        
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
     * Test basic insert operation for new records.
     */
    @Test
    public void testBasicInsert() throws Exception {
        Map<String, Object> testData = createJobActivityTestData("test_insert_1");
        
        // Execute upsert
        sink.open(null);
        sink.invoke(testData, null);
        sink.close();
        
        // Verify record was inserted
        Map<String, Object> result = getRecordFromDatabase("test_insert_1");
        assertNotNull("Record should be inserted", result);
        assertEquals("test_insert_1", result.get("activity_id"));
        assertEquals("TEST_JOB_001", result.get("job_id"));
        assertEquals("John Doe", result.get("candidate"));
    }
    
    /**
     * Test upsert operation - insert then update same record.
     */
    @Test
    public void testUpsertOperation() throws Exception {
        String activityId = "test_upsert_1";
        
        // First insert
        Map<String, Object> initialData = createJobActivityTestData(activityId);
        sink.open(null);
        sink.invoke(initialData, null);
        sink.close();
        
        // Verify initial insert
        Map<String, Object> initialResult = getRecordFromDatabase(activityId);
        assertEquals("John Doe", initialResult.get("candidate"));
        
        // Now update with different data
        Map<String, Object> updateData = createJobActivityTestData(activityId);
        updateData.put("candidate", "Jane Smith");
        updateData.put("company_name", "Updated Company");
        
        sink.open(null);
        sink.invoke(updateData, null);
        sink.close();
        
        // Verify update
        Map<String, Object> updatedResult = getRecordFromDatabase(activityId);
        assertEquals("Jane Smith", updatedResult.get("candidate"));
        assertEquals("Updated Company", updatedResult.get("company_name"));
        assertEquals("TEST_JOB_001", updatedResult.get("job_id")); // Should remain unchanged
    }
    
    /**
     * Test partial column updates (simulating different source tables).
     */
    @Test
    public void testPartialColumnUpdate() throws Exception {
        String activityId = "test_partial_1";
        
        // Insert initial record with job_activity data
        Map<String, Object> initialData = createJobActivityTestData(activityId);
        sink.open(null);
        sink.invoke(initialData, null);
        sink.close();
        
        // Update with jobs table data (only job_title and location)
        Map<String, Object> jobsData = new HashMap<>();
        jobsData.put("activity_id", activityId);
        jobsData.put("job_id", "TEST_JOB_001");
        jobsData.put("job_title", "Senior Software Engineer");
        jobsData.put("location", "San Francisco, CA");
        
        sink.open(null);
        sink.invoke(jobsData, null);
        sink.close();
        
        // Verify partial update
        Map<String, Object> result = getRecordFromDatabase(activityId);
        assertEquals("Senior Software Engineer", result.get("job_title"));
        assertEquals("San Francisco, CA", result.get("location"));
        assertEquals("John Doe", result.get("candidate")); // Should remain unchanged
        assertEquals("Test Company", result.get("company_name")); // Should remain unchanged
    }
    
    /**
     * Test decimal value handling for revenue data.
     */
    @Test
    public void testDecimalValueHandling() throws Exception {
        String activityId = "test_decimal_1";
        
        Map<String, Object> revenueData = new HashMap<>();
        revenueData.put("activity_id", activityId);
        revenueData.put("job_id", "REVENUE_JOB_001");
        revenueData.put("company_name", "Revenue Company");
        revenueData.put("bcworkerid", "BC001");
        revenueData.put("standard_revenue_report_month", "2024-01");
        revenueData.put("clienterpid", "CLIENT001");
        revenueData.put("adjustedstbillrate", new BigDecimal("75.50"));
        revenueData.put("adjustedstpayrate", new BigDecimal("45.00"));
        revenueData.put("adjgphrst", new BigDecimal("10.50"));
        revenueData.put("adjrevenue", new BigDecimal("1500.00"));
        revenueData.put("stbillrate", new BigDecimal("70.00"));
        revenueData.put("sr_assignment_start", LocalDateTime.of(2024, 1, 1, 0, 0));
        revenueData.put("sr_assignment_end", LocalDateTime.of(2024, 1, 31, 23, 59));
        
        sink.open(null);
        sink.invoke(revenueData, null);
        sink.close();
        
        // Verify decimal values
        Map<String, Object> result = getRecordFromDatabase(activityId);
        assertEquals(new BigDecimal("75.50"), result.get("adjustedstbillrate"));
        assertEquals(new BigDecimal("45.00"), result.get("adjustedstpayrate"));
        assertEquals(new BigDecimal("1500.00"), result.get("adjrevenue"));
    }
    
    /**
     * Test null value handling.
     */
    @Test
    public void testNullValueHandling() throws Exception {
        String activityId = "test_null_1";
        
        Map<String, Object> dataWithNulls = new HashMap<>();
        dataWithNulls.put("activity_id", activityId);
        dataWithNulls.put("job_id", "NULL_TEST_JOB");
        dataWithNulls.put("candidate", null);
        dataWithNulls.put("company_name", "Null Test Company");
        dataWithNulls.put("job_title", null);
        dataWithNulls.put("location", null);
        
        sink.open(null);
        sink.invoke(dataWithNulls, null);
        sink.close();
        
        // Verify null values are handled correctly
        Map<String, Object> result = getRecordFromDatabase(activityId);
        assertEquals(activityId, result.get("activity_id"));
        assertEquals("NULL_TEST_JOB", result.get("job_id"));
        assertEquals("Null Test Company", result.get("company_name"));
        assertNull(result.get("candidate"));
        assertNull(result.get("job_title"));
        assertNull(result.get("location"));
    }
    
    /**
     * Test error handling with invalid data.
     */
    @Test
    public void testErrorHandling() throws Exception {
        // Test with empty data
        Map<String, Object> emptyData = new HashMap<>();
        
        sink.open(null);
        sink.invoke(emptyData, null); // Should not throw exception
        sink.close();
        
        // Test with null data
        sink.open(null);
        sink.invoke(null, null); // Should not throw exception
        sink.close();
    }
    
    /**
     * Test concurrent upserts (simulate multiple threads).
     */
    @Test
    public void testConcurrentUpserts() throws Exception {
        String activityId = "test_concurrent_1";
        
        // Simulate concurrent updates from different source tables
        Map<String, Object> jobActivityData = createJobActivityTestData(activityId);
        Map<String, Object> jobsData = new HashMap<>();
        jobsData.put("activity_id", activityId);
        jobsData.put("job_id", "TEST_JOB_001");
        jobsData.put("job_title", "Concurrent Update Job");
        jobsData.put("location", "Concurrent City");
        
        Map<String, Object> revenueData = new HashMap<>();
        revenueData.put("activity_id", activityId);
        revenueData.put("job_id", "TEST_JOB_001");
        revenueData.put("adjustedstbillrate", new BigDecimal("100.00"));
        revenueData.put("adjrevenue", new BigDecimal("2000.00"));
        
        sink.open(null);
        
        // Execute upserts in sequence (simulating concurrent processing)
        sink.invoke(jobActivityData, null);
        sink.invoke(jobsData, null);
        sink.invoke(revenueData, null);
        
        sink.close();
        
        // Verify final state contains data from all sources
        Map<String, Object> result = getRecordFromDatabase(activityId);
        assertEquals("John Doe", result.get("candidate")); // From job_activity
        assertEquals("Concurrent Update Job", result.get("job_title")); // From jobs
        assertEquals(new BigDecimal("100.00"), result.get("adjustedstbillrate")); // From revenue
    }
    
    /**
     * Helper method to create test data for job_activity table.
     */
    private Map<String, Object> createJobActivityTestData(String activityId) {
        Map<String, Object> data = new HashMap<>();
        data.put("activity_id", activityId);
        data.put("job_id", "TEST_JOB_001");
        data.put("jobdiva_no", "JD001");
        data.put("candidate", "John Doe");
        data.put("company_name", "Test Company");
        data.put("assignment_start_date", LocalDateTime.of(2024, 1, 1, 9, 0));
        data.put("assignment_end_date", LocalDateTime.of(2024, 12, 31, 17, 0));
        data.put("bcworkerid", "BC001");
        return data;
    }
    
    /**
     * Helper method to retrieve a record from the database.
     */
    private Map<String, Object> getRecordFromDatabase(String activityId) throws SQLException {
        String sql = "SELECT * FROM " + PipelineConfig.DORIS_TABLE + " WHERE activity_id = ?";
        try (PreparedStatement stmt = testConnection.prepareStatement(sql)) {
            stmt.setString(1, activityId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    Map<String, Object> result = new HashMap<>();
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
    
    /**
     * Helper method to clean up test data.
     */
    private void cleanupTestData() throws SQLException {
        String[] testIds = {
            "test_insert_1", "test_upsert_1", "test_partial_1", "test_decimal_1",
            "test_null_1", "test_concurrent_1"
        };
        
        for (String testId : testIds) {
            String sql = "DELETE FROM " + PipelineConfig.DORIS_TABLE + " WHERE activity_id = ?";
            try (PreparedStatement stmt = testConnection.prepareStatement(sql)) {
                stmt.setString(1, testId);
                stmt.executeUpdate();
            }
        }
    }
}
