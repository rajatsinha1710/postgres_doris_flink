package com.dataengineering.pipeline.sink;

import com.dataengineering.pipeline.config.PipelineConfig;
import com.dataengineering.pipeline.model.MergedResult;
import com.dataengineering.pipeline.util.DorisSchemaValidator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Doris sink function with batching, HTTP client, redirect handling, and retry logic.
 * Implements RichSinkFunction<MergedResult> for Flink integration.
 * 
 * IMPORTANT: This implementation uses specific headers and URL parameters required by Doris:
 * - Content-Type: text/plain (not application/json) - Doris requirement for JSON stream loads
 * - format=json as URL parameter (not header)
 * - strip_outer_array=true as URL parameter to handle JSON arrays properly
 * - Expect: 100-continue header for large payloads
 */
public class DorisSink extends RichSinkFunction<MergedResult> {
    
    private static final Logger logger = LoggerFactory.getLogger(DorisSink.class);
    
    private HttpClient httpClient;
    private List<MergedResult> batchBuffer;
    private long lastFlushTime;
    private final long flushIntervalMs = 5000; // Flush every 5 seconds
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // Initialize HTTP client with timeout configuration and credentials
        RequestConfig requestConfig = RequestConfig.custom()
            .setConnectTimeout(30000)
            .setSocketTimeout(60000)
            .setRedirectsEnabled(true)
            .setMaxRedirects(5)
            .build();
        
        // Set up credentials provider for authentication during redirects
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
            new AuthScope("localhost", 8030),
            new UsernamePasswordCredentials(PipelineConfig.DORIS_USERNAME, PipelineConfig.DORIS_PASSWORD)
        );
        credentialsProvider.setCredentials(
            new AuthScope("localhost", 8040),
            new UsernamePasswordCredentials(PipelineConfig.DORIS_USERNAME, PipelineConfig.DORIS_PASSWORD)
        );
        credentialsProvider.setCredentials(
            new AuthScope("127.0.0.1", 8030),
            new UsernamePasswordCredentials(PipelineConfig.DORIS_USERNAME, PipelineConfig.DORIS_PASSWORD)
        );
        credentialsProvider.setCredentials(
            new AuthScope("127.0.0.1", 8040),
            new UsernamePasswordCredentials(PipelineConfig.DORIS_USERNAME, PipelineConfig.DORIS_PASSWORD)
        );
        
        httpClient = HttpClients.custom()
            .setDefaultRequestConfig(requestConfig)
            .setDefaultCredentialsProvider(credentialsProvider)
            .build();
        
        // Initialize batch buffer
        batchBuffer = new ArrayList<>();
        lastFlushTime = System.currentTimeMillis();
        
        logger.info("Doris sink initialized with batch size: {}, flush interval: {}ms", 
                   PipelineConfig.DORIS_BATCH_SIZE, flushIntervalMs);
        
        // Test Doris connection
        testDorisConnection();
    }
    
    /**
     * Tests the Doris connection and URL to verify it's accessible.
     */
    private void testDorisConnection() {
        try {
            logger.info("Testing Doris connection to: {}", PipelineConfig.DORIS_STREAM_LOAD_URL);
            
        // Create a simple test request with proper headers for Doris
        String testUrl = PipelineConfig.DORIS_STREAM_LOAD_URL + 
            "?format=json&strip_outer_array=true&label=test_" + System.currentTimeMillis();
        HttpPut testPut = new HttpPut(testUrl);
        testPut.setHeader("Content-Type", "text/plain");  // Fixed: Use text/plain instead of application/json
        testPut.setHeader("Authorization", "Basic " + 
            java.util.Base64.getEncoder().encodeToString(
                (PipelineConfig.DORIS_USERNAME + ":" + PipelineConfig.DORIS_PASSWORD).getBytes()));
        testPut.setHeader("Expect", "100-continue");
        
        // Send empty JSON array to test connection
        StringEntity testEntity = new StringEntity("[]", StandardCharsets.UTF_8);
            testPut.setEntity(testEntity);
            
            HttpResponse testResponse = httpClient.execute(testPut);
            int statusCode = testResponse.getStatusLine().getStatusCode();
            String responseBody = EntityUtils.toString(testResponse.getEntity(), StandardCharsets.UTF_8);
            
            logger.info("Doris connection test - Status: {}, Response: {}", statusCode, responseBody);
            
            if (statusCode == 200) {
                logger.info("Doris connection test successful");
            } else {
                logger.warn("Doris connection test returned status {}: {}", statusCode, responseBody);
            }
            
            EntityUtils.consumeQuietly(testResponse.getEntity());
            
        } catch (Exception e) {
            logger.error("Doris connection test failed: {}", e.getMessage(), e);
        }
    }
    
    @Override
    public void close() throws Exception {
        super.close();
        
        // Flush any remaining records in buffer
        if (!batchBuffer.isEmpty()) {
            flushBatch();
        }
        
        // HttpClient doesn't need explicit closing in version 4.x
        // Connection manager handles cleanup automatically
        
        logger.info("Doris sink closed");
    }
    
    @Override
    public void invoke(MergedResult mergedResult, Context context) throws Exception {
        // Add record to batch buffer
        batchBuffer.add(mergedResult);
        
        // Check if we should flush the batch
        boolean shouldFlush = batchBuffer.size() >= PipelineConfig.DORIS_BATCH_SIZE ||
                             (System.currentTimeMillis() - lastFlushTime) >= flushIntervalMs;
        
        if (shouldFlush) {
            flushBatch();
        }
    }
    
    /**
     * Flushes the current batch to Doris using HTTP Stream Load API.
     */
    private void flushBatch() throws Exception {
        if (batchBuffer.isEmpty()) {
            return;
        }
        
        List<MergedResult> batchToSend = new ArrayList<>(batchBuffer);
        batchBuffer.clear();
        lastFlushTime = System.currentTimeMillis();
        
        logger.info("Flushing batch of {} records to Doris", batchToSend.size());
        
        // Send batch with retry logic
        boolean success = sendBatchWithRetry(batchToSend);
        
        if (success) {
            logger.info("Successfully sent batch of {} records to Doris", batchToSend.size());
        } else {
            logger.error("Failed to send batch of {} records to Doris after all retries", batchToSend.size());
            // In production, you might want to send failed records to a dead letter queue
        }
    }
    
    /**
     * Sends batch to Doris with exponential backoff retry logic.
     */
    private boolean sendBatchWithRetry(List<MergedResult> batch) {
        int retryCount = 0;
        long retryDelay = PipelineConfig.DORIS_RETRY_DELAY_MS;
        
        while (retryCount <= PipelineConfig.DORIS_MAX_RETRIES) {
            try {
                boolean success = sendBatchToDoris(batch);
                if (success) {
                    return true;
                }
                
                retryCount++;
                if (retryCount <= PipelineConfig.DORIS_MAX_RETRIES) {
                    logger.warn("Batch send failed, retrying in {}ms (attempt {}/{})", 
                              retryDelay, retryCount, PipelineConfig.DORIS_MAX_RETRIES);
                    
                    TimeUnit.MILLISECONDS.sleep(retryDelay);
                    retryDelay *= 2; // Exponential backoff
                }
                
            } catch (Exception e) {
                retryCount++;
                logger.error("Exception during batch send (attempt {}/{}): {}", 
                           retryCount, PipelineConfig.DORIS_MAX_RETRIES + 1, e.getMessage());
                
                if (retryCount <= PipelineConfig.DORIS_MAX_RETRIES) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(retryDelay);
                        retryDelay *= 2;
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                }
            }
        }
        
        return false;
    }
    
    /**
     * Sends a batch of MergedResult objects to Doris using HTTP Stream Load API.
     * Handles HTTP 307 redirects properly by following redirects automatically.
     * Includes comprehensive debug logging for troubleshooting.
     */
    private boolean sendBatchToDoris(List<MergedResult> batch) throws IOException {
        // Convert batch to JSON format for Doris Stream Load
        String jsonPayload = convertBatchToJson(batch);
        
        // Log the JSON payload for debugging (first 500 chars to avoid huge logs)
        logger.debug("Sending JSON payload to Doris (first 500 chars): {}", 
                    jsonPayload.length() > 500 ? jsonPayload.substring(0, 500) + "..." : jsonPayload);
        
        // Create HTTP PUT request with proper URL parameters for Doris Stream Load
        String urlWithParams = PipelineConfig.DORIS_STREAM_LOAD_URL + 
            "?format=json&strip_outer_array=true&label=flink_stream_load_" + System.currentTimeMillis();
        HttpPut httpPut = new HttpPut(urlWithParams);
        
        // Set headers for Doris Stream Load (fixed Content-Type and proper authentication)
        httpPut.setHeader("Content-Type", "text/plain");  // Fixed: Use text/plain for JSON data
        httpPut.setHeader("Authorization", "Basic " + 
            java.util.Base64.getEncoder().encodeToString(
                (PipelineConfig.DORIS_USERNAME + ":" + PipelineConfig.DORIS_PASSWORD).getBytes()));
        httpPut.setHeader("Expect", "100-continue");
        
        // Set request body
        StringEntity entity = new StringEntity(jsonPayload, StandardCharsets.UTF_8);
        httpPut.setEntity(entity);
        
        logger.debug("Sending HTTP request to Doris URL: {}", PipelineConfig.DORIS_STREAM_LOAD_URL);
        
        // Execute request with automatic redirect handling
        HttpResponse response = httpClient.execute(httpPut);
        
        try {
            int statusCode = response.getStatusLine().getStatusCode();
            String responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            
            // Always log the response for debugging
            logger.info("Doris Stream Load response - Status: {}, Body: {}", statusCode, responseBody);
            
            if (statusCode == 200) {
                // Check if the response actually indicates success
                if (responseBody.contains("\"Status\":\"Success\"") || responseBody.contains("\"status\":\"success\"")) {
                    logger.info("Doris Stream Load successful: {}", responseBody);
                    return true;
                } else {
                    logger.error("Doris returned HTTP 200 but response indicates failure: {}", responseBody);
                    return false;
                }
                
            } else if (statusCode == 307) {
                // HTTP 307 Temporary Redirect - handle manually for Doris
                String location = response.getFirstHeader("Location") != null ? 
                    response.getFirstHeader("Location").getValue() : null;
                
                if (location != null) {
                    logger.info("Following HTTP 307 redirect to: {}", location);
                    
                    // Clean up the redirect URL (remove credentials if present)
                    String cleanLocation = location.replaceAll("://[^:]*:@", "://");
                    
                    // Create new request to the redirect location with proper headers
                    HttpPut redirectPut = new HttpPut(cleanLocation);
                    redirectPut.setHeader("Content-Type", "text/plain");  // Fixed: Use text/plain
                    redirectPut.setHeader("Authorization", "Basic " + 
                        java.util.Base64.getEncoder().encodeToString(
                            (PipelineConfig.DORIS_USERNAME + ":" + PipelineConfig.DORIS_PASSWORD).getBytes()));
                    redirectPut.setHeader("Expect", "100-continue");
                    redirectPut.setEntity(new StringEntity(jsonPayload, StandardCharsets.UTF_8));
                    
                    // Execute the redirected request
                    EntityUtils.consumeQuietly(response.getEntity()); // Clean up first response
                    HttpResponse redirectResponse = httpClient.execute(redirectPut);
                    
                    try {
                        int redirectStatusCode = redirectResponse.getStatusLine().getStatusCode();
                        String redirectResponseBody = EntityUtils.toString(redirectResponse.getEntity(), StandardCharsets.UTF_8);
                        
                        logger.info("Doris Stream Load redirect response - Status: {}, Body: {}", 
                                  redirectStatusCode, redirectResponseBody);
                        
                        if (redirectStatusCode == 200) {
                            // Check if the response indicates success
                            if (redirectResponseBody.contains("\"Status\":\"Success\"") || 
                                redirectResponseBody.contains("\"status\":\"Success\"")) {
                                logger.info("Doris Stream Load successful after redirect: {}", redirectResponseBody);
                                return true;
                            } else {
                                logger.error("Doris returned HTTP 200 after redirect but response indicates failure: {}", 
                                           redirectResponseBody);
                                return false;
                            }
                        } else {
                            logger.error("Doris Stream Load failed after redirect with status {}: {}", 
                                       redirectStatusCode, redirectResponseBody);
                            return false;
                        }
                        
                    } finally {
                        EntityUtils.consumeQuietly(redirectResponse.getEntity());
                    }
                    
                } else {
                    logger.error("HTTP 307 redirect received but no Location header found");
                    return false;
                }
                
            } else {
                // Error - log detailed information
                logger.error("Doris Stream Load failed with status {}: {}", statusCode, responseBody);
                
                // Log specific error details for common issues
                if (responseBody.contains("schema")) {
                    logger.error("Schema mismatch detected in Doris response. Check field names and types.");
                }
                if (responseBody.contains("format")) {
                    logger.error("JSON format error detected. Check date formats and null handling.");
                }
                if (responseBody.contains("duplicate")) {
                    logger.error("Duplicate key error detected. Check unique key constraints.");
                }
                if (responseBody.contains("authentication") || responseBody.contains("unauthorized")) {
                    logger.error("Authentication failed. Check Doris username/password.");
                }
                if (responseBody.contains("database") || responseBody.contains("table")) {
                    logger.error("Database or table not found. Check Doris URL and table name.");
                }
                
                return false;
            }
            
        } finally {
            EntityUtils.consumeQuietly(response.getEntity());
        }
    }
    
    /**
     * Converts a batch of MergedResult objects to JSON array format for Doris Stream Load.
     * Creates a JSON array with each record as a JSON object.
     * Includes validation and debug logging for each record.
     */
    private String convertBatchToJson(List<MergedResult> batch) {
        // Validate batch before processing
        DorisSchemaValidator.logValidationSummary(batch);
        
        StringBuilder jsonBuilder = new StringBuilder("[");
        int validRecordCount = 0;
        boolean firstRecord = true;
        
        for (int i = 0; i < batch.size(); i++) {
            MergedResult result = batch.get(i);
            
            // Validate critical fields before serialization
            if (result.getActivityId() == null || result.getActivityId().trim().isEmpty()) {
                logger.warn("Record {} has null or empty activity_id, skipping", i);
                continue;
            }
            
            // Additional schema validation
            if (!DorisSchemaValidator.validate(result)) {
                logger.warn("Record {} failed schema validation, skipping", i);
                continue;
            }
            
            String recordJson = result.toJson();
            
            // Add comma separator between valid records (not based on array index)
            if (!firstRecord) {
                jsonBuilder.append(",");
            }
            
            jsonBuilder.append(recordJson);
            validRecordCount++;
            firstRecord = false;
            
            // Log individual record for debugging (first record only to avoid spam)
            if (validRecordCount == 1) {
                logger.debug("Sample record JSON: {}", recordJson);
            }
        }
        
        jsonBuilder.append("]");
        String finalJson = jsonBuilder.toString();
        
        // Log batch statistics
        logger.debug("Converted batch of {} records to JSON ({} valid records, {} characters)", 
                    batch.size(), validRecordCount, finalJson.length());
        
        if (validRecordCount < batch.size()) {
            logger.warn("Skipped {} invalid records from batch of {}", 
                       batch.size() - validRecordCount, batch.size());
        }
        
        return finalJson;
    }
}
