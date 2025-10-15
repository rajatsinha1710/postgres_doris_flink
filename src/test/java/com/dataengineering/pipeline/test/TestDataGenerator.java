package com.dataengineering.pipeline.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Utility class for generating test CDC events and data for Doris upsert testing.
 * Provides methods to create realistic test data for different source tables.
 */
public class TestDataGenerator {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Random random = new Random();
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    /**
     * Generates a CDC event JSON string for job_activity table.
     */
    public static String generateJobActivityCdcEvent(String activityId, String operation) {
        ObjectNode event = objectMapper.createObjectNode();
        event.put("op", operation);
        
        ObjectNode source = objectMapper.createObjectNode();
        source.put("table", "job_activity");
        event.set("source", source);
        
        ObjectNode after = objectMapper.createObjectNode();
        after.put("activity_id", activityId);
        after.put("job_id", "JOB_" + String.format("%03d", random.nextInt(1000)));
        after.put("jobdiva_no", "JD" + String.format("%03d", random.nextInt(1000)));
        after.put("candidate_name", generateRandomName());
        after.put("company_name", generateRandomCompany());
        after.put("assignment_start_date", generateRandomDateTime().format(dateTimeFormatter));
        after.put("assignment_end_date", generateRandomDateTime().format(dateTimeFormatter));
        after.put("bcworkerid", "BC" + String.format("%03d", random.nextInt(1000)));
        
        event.set("after", after);
        
        return event.toString();
    }
    
    /**
     * Generates a CDC event JSON string for jobs table.
     */
    public static String generateJobsCdcEvent(String jobId, String operation) {
        ObjectNode event = objectMapper.createObjectNode();
        event.put("op", operation);
        
        ObjectNode source = objectMapper.createObjectNode();
        source.put("table", "jobs");
        event.set("source", source);
        
        ObjectNode after = objectMapper.createObjectNode();
        after.put("job_id", jobId);
        after.put("job_title", generateRandomJobTitle());
        after.put("location", generateRandomLocation());
        
        event.set("after", after);
        
        return event.toString();
    }
    
    /**
     * Generates a CDC event JSON string for standard_revenue table.
     */
    public static String generateStandardRevenueCdcEvent(String bcworkerid, String operation) {
        ObjectNode event = objectMapper.createObjectNode();
        event.put("op", operation);
        
        ObjectNode source = objectMapper.createObjectNode();
        source.put("table", "standard_revenue");
        event.set("source", source);
        
        ObjectNode after = objectMapper.createObjectNode();
        after.put("bcworkerid", bcworkerid);
        after.put("clienterpid", "CLIENT" + String.format("%03d", random.nextInt(1000)));
        after.put("adjusted_st_bill_rate", String.format("%.2f", 50.0 + random.nextDouble() * 50));
        after.put("adjusted_st_pay_rate", String.format("%.2f", 30.0 + random.nextDouble() * 30));
        after.put("adj_gph_rst", String.format("%.2f", random.nextDouble() * 20));
        after.put("adj_revenue", String.format("%.2f", 1000.0 + random.nextDouble() * 2000));
        after.put("st_bill_rate", String.format("%.2f", 45.0 + random.nextDouble() * 40));
        after.put("standard_revenue_report_month", generateRandomMonth());
        after.put("assignment_start_date", generateRandomDateTime().format(dateTimeFormatter));
        after.put("assignment_end_date", generateRandomDateTime().format(dateTimeFormatter));
        
        event.set("after", after);
        
        return event.toString();
    }
    
    /**
     * Generates a CDC event JSON string for clients_to_exclude table.
     */
    public static String generateClientsToExcludeCdcEvent(String clientId, String operation) {
        ObjectNode event = objectMapper.createObjectNode();
        event.put("op", operation);
        
        ObjectNode source = objectMapper.createObjectNode();
        source.put("table", "clients_to_exclude");
        event.set("source", source);
        
        ObjectNode after = objectMapper.createObjectNode();
        after.put("client_id", clientId);
        after.put("exclusion_reason", generateRandomExclusionReason());
        
        event.set("after", after);
        
        return event.toString();
    }
    
    /**
     * Generates a batch of test CDC events for comprehensive testing.
     */
    public static List<String> generateBatchTestEvents(int count) {
        List<String> events = new ArrayList<>();
        
        for (int i = 0; i < count; i++) {
            String activityId = "test_activity_" + String.format("%03d", i);
            String jobId = "JOB_" + String.format("%03d", i);
            String bcworkerid = "BC" + String.format("%03d", i);
            String clientId = "CLIENT_" + String.format("%03d", i);
            
            // Generate events for different tables
            events.add(generateJobActivityCdcEvent(activityId, "c"));
            events.add(generateJobsCdcEvent(jobId, "c"));
            events.add(generateStandardRevenueCdcEvent(bcworkerid, "c"));
            
            if (i % 5 == 0) { // Add some exclusion events
                events.add(generateClientsToExcludeCdcEvent(clientId, "c"));
            }
        }
        
        return events;
    }
    
    /**
     * Generates upsert test scenarios for different source tables.
     */
    public static List<String> generateUpsertTestScenarios() {
        List<String> scenarios = new ArrayList<>();
        
        // Scenario 1: Initial job_activity insert
        scenarios.add(generateJobActivityCdcEvent("upsert_test_1", "c"));
        
        // Scenario 2: Update with jobs data (partial update)
        scenarios.add(generateJobsCdcEvent("JOB_001", "u"));
        
        // Scenario 3: Update with revenue data (partial update)
        scenarios.add(generateStandardRevenueCdcEvent("BC001", "u"));
        
        // Scenario 4: Update with exclusion data (partial update)
        scenarios.add(generateClientsToExcludeCdcEvent("CLIENT001", "u"));
        
        return scenarios;
    }
    
    /**
     * Generates test data for performance testing.
     */
    public static List<String> generatePerformanceTestData(int recordCount) {
        List<String> events = new ArrayList<>();
        
        for (int i = 0; i < recordCount; i++) {
            String activityId = "perf_test_" + String.format("%06d", i);
            events.add(generateJobActivityCdcEvent(activityId, "c"));
            
            // Add some updates
            if (i % 10 == 0) {
                events.add(generateJobsCdcEvent("JOB_" + String.format("%03d", i % 100), "u"));
            }
        }
        
        return events;
    }
    
    // Helper methods for generating random data
    
    private static String generateRandomName() {
        String[] firstNames = {"John", "Jane", "Mike", "Sarah", "David", "Lisa", "Chris", "Amy", "Tom", "Emma"};
        String[] lastNames = {"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"};
        return firstNames[random.nextInt(firstNames.length)] + " " + lastNames[random.nextInt(lastNames.length)];
    }
    
    private static String generateRandomCompany() {
        String[] companies = {"TechCorp", "DataSoft", "CloudSys", "Analytics Inc", "BigData Co", "AI Solutions", "DataFlow", "StreamTech", "Pipeline Corp", "RealTime Systems"};
        return companies[random.nextInt(companies.length)];
    }
    
    private static String generateRandomJobTitle() {
        String[] titles = {"Software Engineer", "Data Scientist", "DevOps Engineer", "Product Manager", "Data Engineer", "Backend Developer", "Frontend Developer", "Full Stack Developer", "Data Analyst", "System Architect"};
        return titles[random.nextInt(titles.length)];
    }
    
    private static String generateRandomLocation() {
        String[] locations = {"San Francisco, CA", "New York, NY", "Seattle, WA", "Austin, TX", "Boston, MA", "Chicago, IL", "Denver, CO", "Los Angeles, CA", "Miami, FL", "Portland, OR"};
        return locations[random.nextInt(locations.length)];
    }
    
    private static String generateRandomExclusionReason() {
        String[] reasons = {"Payment issues", "Contract disputes", "Quality concerns", "Timeline conflicts", "Budget overruns", "Communication problems", "Scope creep", "Resource constraints", "Technical incompatibility", "Business policy violation"};
        return reasons[random.nextInt(reasons.length)];
    }
    
    private static String generateRandomMonth() {
        int year = 2024;
        int month = 1 + random.nextInt(12);
        return String.format("%d-%02d", year, month);
    }
    
    private static LocalDateTime generateRandomDateTime() {
        int year = 2024;
        int month = 1 + random.nextInt(12);
        int day = 1 + random.nextInt(28); // Safe day range
        int hour = random.nextInt(24);
        int minute = random.nextInt(60);
        
        return LocalDateTime.of(year, month, day, hour, minute);
    }
    
    /**
     * Creates a test Doris record map for direct testing.
     */
    public static java.util.Map<String, Object> createTestDorisRecord(String activityId) {
        java.util.Map<String, Object> record = new java.util.HashMap<>();
        record.put("activity_id", activityId);
        record.put("job_id", "TEST_JOB_" + activityId);
        record.put("jobdiva_no", "JD" + String.format("%03d", random.nextInt(1000)));
        record.put("candidate", generateRandomName());
        record.put("company_name", generateRandomCompany());
        record.put("assignment_start_date", generateRandomDateTime());
        record.put("assignment_end_date", generateRandomDateTime());
        record.put("job_title", generateRandomJobTitle());
        record.put("location", generateRandomLocation());
        record.put("bcworkerid", "BC" + String.format("%03d", random.nextInt(1000)));
        record.put("adjustedstbillrate", new BigDecimal(String.format("%.2f", 50.0 + random.nextDouble() * 50)));
        record.put("adjustedstpayrate", new BigDecimal(String.format("%.2f", 30.0 + random.nextDouble() * 30)));
        record.put("adjrevenue", new BigDecimal(String.format("%.2f", 1000.0 + random.nextDouble() * 2000)));
        
        return record;
    }
}
