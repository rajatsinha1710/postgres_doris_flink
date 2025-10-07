package com.dataengineering.pipeline.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Extracts primary key IDs from Kafka CDC events.
 * Handles CDC JSON format with "before" and "after" fields and extracts 
 * appropriate IDs based on the topic name.
 */
public class IdExtractor implements MapFunction<String, List<String>> {
    
    private static final Logger logger = LoggerFactory.getLogger(IdExtractor.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public List<String> map(String kafkaMessage) throws Exception {
        List<String> extractedIds = new ArrayList<>();
        
        try {
            // Parse the Kafka message as JSON
            JsonNode messageNode = objectMapper.readTree(kafkaMessage);
            
            // Extract topic name from Kafka message metadata or infer from message structure
            String topicName = extractTopicName(messageNode);
            
            // Extract IDs based on the topic and CDC format
            List<String> ids = extractIdsFromCdcMessage(messageNode, topicName);
            extractedIds.addAll(ids);
            
            logger.debug("Extracted {} IDs from topic {}: {}", ids.size(), topicName, ids);
            
        } catch (Exception e) {
            logger.error("Failed to extract IDs from Kafka message: {}", kafkaMessage, e);
            // Return empty list to continue processing other messages
        }
        
        return extractedIds;
    }
    
    /**
     * Extracts topic name from Kafka message.
     * This could be from message metadata or inferred from the message structure.
     */
    private String extractTopicName(JsonNode messageNode) {
        // Try to extract from source metadata (common in CDC formats)
        if (messageNode.has("source")) {
            JsonNode source = messageNode.get("source");
            if (source.has("table")) {
                return source.get("table").asText();
            }
        }
        
        // Try to extract from topic field
        if (messageNode.has("topic")) {
            String topic = messageNode.get("topic").asText();
            // Extract table name from topic (e.g., "pgserver1.public.jobs" -> "jobs")
            String[] parts = topic.split("\\.");
            if (parts.length > 0) {
                return parts[parts.length - 1];
            }
        }
        
        return "unknown";
    }
    
    /**
     * Extracts IDs from CDC message format with "before" and "after" fields.
     * Maps topic names to their primary key fields as specified in requirements.
     */
    private List<String> extractIdsFromCdcMessage(JsonNode messageNode, String topicName) {
        List<String> ids = new ArrayList<>();
        
        // Handle CDC format with "before" and "after" fields
        JsonNode after = messageNode.get("after");
        JsonNode before = messageNode.get("before");
        
        // Use "after" for INSERT/UPDATE operations, "before" for DELETE operations
        JsonNode dataNode = after != null ? after : before;
        
        if (dataNode != null) {
            switch (topicName.toLowerCase()) {
                case "clients_to_exclude":
                    extractClientId(dataNode, ids);
                    break;
                case "job_activity":
                    extractActivityId(dataNode, ids);
                    break;
                case "jobs":
                    extractJobId(dataNode, ids);
                    break;
                case "standard_revenue":
                    extractRevenueId(dataNode, ids);
                    break;
                default:
                    logger.warn("Unknown topic name: {}, attempting to extract common IDs", topicName);
                    extractCommonIds(dataNode, ids);
            }
        }
        
        return ids;
    }
    
    /**
     * Extracts client_id from clients_to_exclude topic CDC events.
     */
    private void extractClientId(JsonNode dataNode, List<String> ids) {
        if (dataNode.has("client_id")) {
            String clientId = dataNode.get("client_id").asText();
            if (clientId != null && !clientId.trim().isEmpty()) {
                ids.add(clientId);
            }
        }
    }
    
    /**
     * Extracts activity_id from job_activity topic CDC events.
     */
    private void extractActivityId(JsonNode dataNode, List<String> ids) {
        if (dataNode.has("activity_id")) {
            String activityId = dataNode.get("activity_id").asText();
            if (activityId != null && !activityId.trim().isEmpty()) {
                ids.add(activityId);
            }
        }
    }
    
    /**
     * Extracts job_id from jobs topic CDC events.
     */
    private void extractJobId(JsonNode dataNode, List<String> ids) {
        if (dataNode.has("job_id")) {
            String jobId = dataNode.get("job_id").asText();
            if (jobId != null && !jobId.trim().isEmpty()) {
                ids.add(jobId);
            }
        }
    }
    
    /**
     * Extracts revenue_id from standard_revenue topic CDC events.
     */
    private void extractRevenueId(JsonNode dataNode, List<String> ids) {
        if (dataNode.has("revenue_id")) {
            String revenueId = dataNode.get("revenue_id").asText();
            if (revenueId != null && !revenueId.trim().isEmpty()) {
                ids.add(revenueId);
            }
        }
    }
    
    /**
     * Extracts common ID fields when topic name is unknown.
     */
    private void extractCommonIds(JsonNode dataNode, List<String> ids) {
        // Common ID field names to try
        String[] commonIdFields = {"id", "job_id", "activity_id", "client_id", "revenue_id", "candidate_id"};
        
        for (String field : commonIdFields) {
            if (dataNode.has(field)) {
                String id = dataNode.get(field).asText();
                if (id != null && !id.trim().isEmpty()) {
                    ids.add(id);
                }
            }
        }
    }
}
