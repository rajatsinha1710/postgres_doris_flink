package com.dataengineering.pipeline.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Utility class for common operations in the pipeline.
 * Provides helper methods for retry logic, validation, and error handling.
 */
public class PipelineUtils {
    
    private static final Logger logger = LoggerFactory.getLogger(PipelineUtils.class);
    
    /**
     * Executes a task with exponential backoff retry logic.
     * 
     * @param task The task to execute
     * @param maxRetries Maximum number of retry attempts
     * @param initialDelayMs Initial delay between retries in milliseconds
     * @param taskName Name of the task for logging purposes
     * @return true if task succeeded, false if all retries failed
     */
    public static boolean executeWithRetry(Runnable task, int maxRetries, long initialDelayMs, String taskName) {
        int retryCount = 0;
        long retryDelay = initialDelayMs;
        
        while (retryCount <= maxRetries) {
            try {
                task.run();
                return true;
                
            } catch (Exception e) {
                retryCount++;
                logger.warn("Task '{}' failed (attempt {}/{}): {}", 
                           taskName, retryCount, maxRetries + 1, e.getMessage());
                
                if (retryCount <= maxRetries) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(retryDelay);
                        retryDelay *= 2; // Exponential backoff
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        logger.error("Task '{}' interrupted during retry delay", taskName);
                        return false;
                    }
                }
            }
        }
        
        logger.error("Task '{}' failed after {} retries", taskName, maxRetries);
        return false;
    }
    
    /**
     * Validates that a string is not null or empty.
     * 
     * @param value The string to validate
     * @param fieldName The field name for error messages
     * @return true if valid, false otherwise
     */
    public static boolean isValidString(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            logger.warn("Invalid {}: null or empty", fieldName);
            return false;
        }
        return true;
    }
    
    /**
     * Safely converts a string to an integer with default value.
     * 
     * @param value The string to convert
     * @param defaultValue Default value if conversion fails
     * @return Converted integer or default value
     */
    public static int safeParseInt(String value, int defaultValue) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            logger.warn("Failed to parse integer from '{}', using default: {}", value, defaultValue);
            return defaultValue;
        }
    }
    
    /**
     * Safely converts a string to a long with default value.
     * 
     * @param value The string to convert
     * @param defaultValue Default value if conversion fails
     * @return Converted long or default value
     */
    public static long safeParseLong(String value, long defaultValue) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            logger.warn("Failed to parse long from '{}', using default: {}", value, defaultValue);
            return defaultValue;
        }
    }
    
    /**
     * Formats a duration in milliseconds to a human-readable string.
     * 
     * @param durationMs Duration in milliseconds
     * @return Formatted duration string
     */
    public static String formatDuration(long durationMs) {
        if (durationMs < 1000) {
            return durationMs + "ms";
        } else if (durationMs < 60000) {
            return String.format("%.2fs", durationMs / 1000.0);
        } else if (durationMs < 3600000) {
            return String.format("%.2fm", durationMs / 60000.0);
        } else {
            return String.format("%.2fh", durationMs / 3600000.0);
        }
    }
    
    /**
     * Creates a JSON string with proper error formatting.
     * 
     * @param error The error message
     * @param timestamp The timestamp of the error
     * @return JSON formatted error string
     */
    public static String createErrorJson(String error, long timestamp) {
        return String.format("{\"error\":\"%s\",\"timestamp\":%d,\"type\":\"pipeline_error\"}", 
                           error.replace("\"", "\\\""), timestamp);
    }
    
    /**
     * Measures execution time of a task and logs the result.
     * 
     * @param task The task to execute
     * @param taskName Name of the task for logging
     * @return The execution time in milliseconds
     */
    public static long measureExecutionTime(Runnable task, String taskName) {
        long startTime = System.currentTimeMillis();
        
        try {
            task.run();
            long executionTime = System.currentTimeMillis() - startTime;
            logger.info("Task '{}' completed in {}", taskName, formatDuration(executionTime));
            return executionTime;
            
        } catch (Exception e) {
            long executionTime = System.currentTimeMillis() - startTime;
            logger.error("Task '{}' failed after {}", taskName, formatDuration(executionTime), e);
            throw e;
        }
    }
}
