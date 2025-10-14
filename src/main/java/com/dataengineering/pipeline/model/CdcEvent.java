package com.dataengineering.pipeline.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a CDC (Change Data Capture) event from Kafka.
 * Handles deserialization of CDC payloads and maps them to Doris table columns.
 * Supports upsert operations with only relevant columns for each table.
 */
public class CdcEvent {
    
    private static final Logger logger = LoggerFactory.getLogger(CdcEvent.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    // CDC event fields
    private String operation; // INSERT, UPDATE, DELETE
    private String tableName;
    private Map<String, Object> before;
    private Map<String, Object> after;
    private Map<String, Object> source;
    
    // Constructor
    public CdcEvent() {}
    
    /**
     * Creates a CdcEvent from Kafka JSON message.
     */
    public static CdcEvent fromJson(String jsonMessage) {
        try {
            JsonNode root = objectMapper.readTree(jsonMessage);
            CdcEvent event = new CdcEvent();
            
            // Extract operation type
            if (root.has("op")) {
                event.operation = root.get("op").asText();
            }
            
            // Extract table name from source
            if (root.has("source")) {
                JsonNode sourceNode = root.get("source");
                @SuppressWarnings("unchecked")
                Map<String, Object> sourceMap = objectMapper.convertValue(sourceNode, Map.class);
                event.source = sourceMap;
                if (sourceNode.has("table")) {
                    event.tableName = sourceNode.get("table").asText();
                }
            }
            
            // Extract before and after data
            if (root.has("before")) {
                @SuppressWarnings("unchecked")
                Map<String, Object> beforeMap = objectMapper.convertValue(root.get("before"), Map.class);
                event.before = beforeMap;
            }
            if (root.has("after")) {
                @SuppressWarnings("unchecked")
                Map<String, Object> afterMap = objectMapper.convertValue(root.get("after"), Map.class);
                event.after = afterMap;
            }
            
            return event;
            
        } catch (Exception e) {
            logger.error("Failed to parse CDC event from JSON: {}", jsonMessage, e);
            return null;
        }
    }
    
    /**
     * Gets the data to upsert (after for INSERT/UPDATE, before for DELETE).
     */
    public Map<String, Object> getDataToUpsert() {
        if ("d".equals(operation) || "DELETE".equals(operation)) {
            return before;
        }
        return after;
    }
    
    /**
     * Maps CDC event to Doris table columns based on source table.
     * Returns only relevant columns for the specific table plus required matching keys.
     */
    public Map<String, Object> getDorisColumns() {
        Map<String, Object> data = getDataToUpsert();
        if (data == null) {
            return new HashMap<>();
        }
        
        Map<String, Object> dorisColumns = new HashMap<>();
        
        switch (tableName.toLowerCase()) {
            case "job_activity":
                mapJobActivityColumns(data, dorisColumns);
                break;
            case "jobs":
                mapJobsColumns(data, dorisColumns);
                break;
            case "clients_to_exclude":
                mapClientsToExcludeColumns(data, dorisColumns);
                break;
            case "standard_revenue":
                mapStandardRevenueColumns(data, dorisColumns);
                break;
            default:
                logger.warn("Unknown table: {}, mapping all columns", tableName);
                dorisColumns.putAll(data);
        }
        
        return dorisColumns;
    }
    
    /**
     * Gets the matching key for upserts based on source table.
     * This determines which existing records to update in Doris.
     */
    public Map<String, Object> getMatchingKey() {
        Map<String, Object> data = getDataToUpsert();
        if (data == null) {
            return new HashMap<>();
        }
        
        Map<String, Object> matchingKey = new HashMap<>();
        
        switch (tableName.toLowerCase()) {
            case "job_activity":
                // Primary key: activity_id
                if (data.get("activity_id") != null) {
                    matchingKey.put("activity_id", data.get("activity_id"));
                }
                break;
            case "jobs":
                // Match by job_id
                if (data.get("job_id") != null) {
                    matchingKey.put("job_id", data.get("job_id"));
                }
                break;
            case "clients_to_exclude":
                // Match by company_name (client_id)
                if (data.get("client_id") != null) {
                    matchingKey.put("company_name", data.get("client_id"));
                }
                break;
            case "standard_revenue":
                // Match by bcworkerid and clienterpid combination
                if (data.get("bcworkerid") != null) {
                    matchingKey.put("bcworkerid", data.get("bcworkerid"));
                }
                if (data.get("clienterpid") != null) {
                    matchingKey.put("clienterpid", data.get("clienterpid"));
                }
                break;
        }
        
        return matchingKey;
    }
    
    /**
     * Maps job_activity table columns to Doris merged_job_data table.
     */
    private void mapJobActivityColumns(Map<String, Object> source, Map<String, Object> target) {
        // Add source table metadata for selective updates
        target.put("__source_table__", "job_activity");
        
        target.put("activity_id", source.get("activity_id"));
        target.put("job_id", source.get("job_id"));
        target.put("jobdiva_no", source.get("jobdiva_no"));
        target.put("candidate", source.get("candidate_name"));
        target.put("company_name", source.get("company_name"));
        target.put("assignment_start_date", parseDateTime(source.get("assignment_start_date")));
        target.put("assignment_end_date", parseDateTime(source.get("assignment_end_date")));
        target.put("bcworkerid", source.get("bcworkerid"));
    }
    
    /**
     * Maps jobs table columns to Doris merged_job_data table.
     */
    private void mapJobsColumns(Map<String, Object> source, Map<String, Object> target) {
        // Add source table metadata for selective updates
        target.put("__source_table__", "jobs");
        
        target.put("job_id", source.get("job_id"));
        target.put("job_title", source.get("job_title"));
        target.put("location", source.get("location"));
    }
    
    /**
     * Maps clients_to_exclude table columns to Doris merged_job_data table.
     */
    private void mapClientsToExcludeColumns(Map<String, Object> source, Map<String, Object> target) {
        // Add source table metadata for selective updates
        target.put("__source_table__", "clients_to_exclude");
        
        target.put("excluded_reason", source.get("exclusion_reason"));
        // Use client_id as company_name for matching
        target.put("company_name", source.get("client_id"));
    }
    
    /**
     * Maps standard_revenue table columns to Doris merged_job_data table.
     */
    private void mapStandardRevenueColumns(Map<String, Object> source, Map<String, Object> target) {
        // Add source table metadata for selective updates
        target.put("__source_table__", "standard_revenue");
        
        target.put("standard_revenue_report_month", source.get("standard_revenue_report_month"));
        target.put("clienterpid", source.get("clienterpid"));
        target.put("bcworkerid", source.get("bcworkerid"));
        target.put("adjustedstbillrate", parseBigDecimal(source.get("adjusted_st_bill_rate")));
        target.put("adjustedstpayrate", parseBigDecimal(source.get("adjusted_st_pay_rate")));
        target.put("adjgphrst", parseBigDecimal(source.get("adj_gph_rst")));
        target.put("adjrevenue", parseBigDecimal(source.get("adj_revenue")));
        target.put("stbillrate", parseBigDecimal(source.get("st_bill_rate")));
        target.put("sr_assignment_start", parseDateTime(source.get("assignment_start_date")));
        target.put("sr_assignment_end", parseDateTime(source.get("assignment_end_date")));
    }
    
    /**
     * Parses datetime string to LocalDateTime.
     * Handles various formats including epoch days.
     */
    private LocalDateTime parseDateTime(Object dateTimeObj) {
        if (dateTimeObj == null) {
            return null;
        }
        
        try {
            String dateTimeStr = dateTimeObj.toString().trim();
            
            // Check if it's a numeric value (epoch days or timestamp)
            if (dateTimeStr.matches("^\\d+$")) {
                long value = Long.parseLong(dateTimeStr);
                
                // If the number is small (< 100000), treat as epoch days
                // Epoch days are days since 1970-01-01
                if (value < 100000) {
                    return java.time.LocalDate.ofEpochDay(value).atStartOfDay();
                } else {
                    // Assume it's an epoch timestamp in milliseconds
                    return LocalDateTime.ofInstant(
                        java.time.Instant.ofEpochMilli(value), 
                        java.time.ZoneOffset.UTC
                    );
                }
            }
            
            // Handle ISO format with T
            if (dateTimeStr.contains("T")) {
                return LocalDateTime.parse(dateTimeStr.replace("T", " "), DATE_TIME_FORMATTER);
            } else {
                // Handle standard format
                return LocalDateTime.parse(dateTimeStr, DATE_TIME_FORMATTER);
            }
        } catch (Exception e) {
            logger.warn("Failed to parse datetime: {}", dateTimeObj, e);
            return null;
        }
    }
    
    /**
     * Parses BigDecimal from string or number.
     * Handles non-numeric values gracefully.
     */
    private BigDecimal parseBigDecimal(Object value) {
        if (value == null) {
            return null;
        }
        
        try {
            String valueStr = value.toString().trim();
            
            // Skip empty values
            if (valueStr.isEmpty()) {
                return null;
            }
            
            // Skip obviously non-numeric values (like "E8WS")
            if (!valueStr.matches("^[+-]?\\d*\\.?\\d+([eE][+-]?\\d+)?$")) {
                logger.warn("Non-numeric value cannot be converted to BigDecimal: {}", value);
                return BigDecimal.ZERO; // Return 0 instead of null for calculations
            }
            
            if (value instanceof Number) {
                return new BigDecimal(value.toString());
            } else {
                return new BigDecimal(valueStr);
            }
        } catch (Exception e) {
            logger.warn("Failed to parse BigDecimal: {}, using 0 as fallback", value, e);
            return BigDecimal.ZERO;
        }
    }
    
    // Getters and setters
    public String getOperation() { return operation; }
    public void setOperation(String operation) { this.operation = operation; }
    
    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }
    
    public Map<String, Object> getBefore() { return before; }
    public void setBefore(Map<String, Object> before) { this.before = before; }
    
    public Map<String, Object> getAfter() { return after; }
    public void setAfter(Map<String, Object> after) { this.after = after; }
    
    public Map<String, Object> getSource() { return source; }
    public void setSource(Map<String, Object> source) { this.source = source; }
    
    @Override
    public String toString() {
        return "CdcEvent{" +
                "operation='" + operation + '\'' +
                ", tableName='" + tableName + '\'' +
                '}';
    }
}
