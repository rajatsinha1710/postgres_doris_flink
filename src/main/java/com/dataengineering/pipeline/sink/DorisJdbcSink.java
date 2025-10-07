package com.dataengineering.pipeline.sink;

import com.dataengineering.pipeline.config.PipelineConfig;
import com.dataengineering.pipeline.model.MergedResult;
import com.dataengineering.pipeline.util.DorisSchemaValidator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Alternative Doris sink using JDBC/SQL INSERT instead of Stream Load API.
 * This approach bypasses Stream Load issues and uses standard SQL INSERT statements.
 * Less efficient than Stream Load but more reliable for problematic Doris configurations.
 * 
 * Use this as a fallback when Stream Load API has configuration issues.
 */
public class DorisJdbcSink extends RichSinkFunction<MergedResult> {
    
    private static final Logger logger = LoggerFactory.getLogger(DorisJdbcSink.class);
    
    private Connection connection;
    private PreparedStatement insertStatement;
    private List<MergedResult> batchBuffer;
    private long lastFlushTime;
    private final long flushIntervalMs = 5000; // Flush every 5 seconds
    
    // Doris JDBC connection details
    private static final String DORIS_JDBC_URL = "jdbc:mysql://127.0.0.1:9030/job_analytics";
    private static final String INSERT_SQL = 
        "INSERT INTO merged_job_data (" +
        "activity_id, job_id, jobdiva_no, candidate, company_name, " +
        "assignment_start_date, assignment_end_date, job_title, location, excluded_reason, " +
        "sr_assignment_start, sr_assignment_end, standard_revenue_report_month, " +
        "clienterpid, bcworkerid, adjustedstbillrate, adjustedstpayrate, " +
        "adjgphrst, adjrevenue, stbillrate" +
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // Initialize JDBC connection to Doris
        try {
            // Load MySQL JDBC driver (Doris uses MySQL protocol)
            Class.forName("com.mysql.cj.jdbc.Driver");
            
            connection = DriverManager.getConnection(
                DORIS_JDBC_URL, 
                PipelineConfig.DORIS_USERNAME, 
                PipelineConfig.DORIS_PASSWORD
            );
            
            connection.setAutoCommit(false); // Enable batching
            
            // Prepare the INSERT statement
            insertStatement = connection.prepareStatement(INSERT_SQL);
            
            // Initialize batch buffer
            batchBuffer = new ArrayList<>();
            lastFlushTime = System.currentTimeMillis();
            
            logger.info("Doris JDBC sink initialized with batch size: {}, flush interval: {}ms", 
                       PipelineConfig.DORIS_BATCH_SIZE, flushIntervalMs);
            
            // Test connection
            testDorisConnection();
            
        } catch (Exception e) {
            logger.error("Failed to initialize Doris JDBC connection", e);
            throw e;
        }
    }
    
    /**
     * Tests the Doris JDBC connection.
     */
    private void testDorisConnection() {
        try {
            Statement testStatement = connection.createStatement();
            ResultSet rs = testStatement.executeQuery("SELECT 1 as test");
            if (rs.next()) {
                logger.info("Doris JDBC connection test successful");
            }
            rs.close();
            testStatement.close();
        } catch (Exception e) {
            logger.error("Doris JDBC connection test failed", e);
        }
    }
    
    @Override
    public void close() throws Exception {
        super.close();
        
        // Flush any remaining records
        if (!batchBuffer.isEmpty()) {
            flushBatch();
        }
        
        // Close JDBC resources
        if (insertStatement != null) {
            insertStatement.close();
        }
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
        
        logger.info("Doris JDBC sink closed");
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
     * Flushes the current batch to Doris using JDBC batch INSERT.
     */
    private void flushBatch() throws Exception {
        if (batchBuffer.isEmpty()) {
            return;
        }
        
        List<MergedResult> batchToSend = new ArrayList<>(batchBuffer);
        batchBuffer.clear();
        lastFlushTime = System.currentTimeMillis();
        
        logger.info("Flushing batch of {} records to Doris via JDBC", batchToSend.size());
        
        boolean success = sendBatchWithRetry(batchToSend);
        
        if (success) {
            logger.info("Successfully sent batch of {} records to Doris", batchToSend.size());
        } else {
            logger.error("Failed to send batch of {} records to Doris after all retries", batchToSend.size());
        }
    }
    
    /**
     * Sends batch to Doris with retry logic.
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
                    logger.warn("JDBC batch send failed, retrying in {}ms (attempt {}/{})", 
                              retryDelay, retryCount, PipelineConfig.DORIS_MAX_RETRIES);
                    
                    TimeUnit.MILLISECONDS.sleep(retryDelay);
                    retryDelay *= 2; // Exponential backoff
                }
                
            } catch (Exception e) {
                retryCount++;
                logger.error("Exception during JDBC batch send (attempt {}/{}): {}", 
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
     * Sends a batch of records to Doris using JDBC batch INSERT.
     */
    private boolean sendBatchToDoris(List<MergedResult> batch) throws SQLException {
        // Validate batch
        DorisSchemaValidator.logValidationSummary(batch);
        
        int validRecords = 0;
        
        try {
            // Add all records to the batch
            for (MergedResult result : batch) {
                // Skip invalid records
                if (result.getActivityId() == null || result.getActivityId().trim().isEmpty()) {
                    logger.warn("Skipping record with null/empty activity_id");
                    continue;
                }
                
                if (!DorisSchemaValidator.validate(result)) {
                    logger.warn("Skipping record that failed schema validation");
                    continue;
                }
                
                // Set parameters for the prepared statement
                insertStatement.setString(1, result.getActivityId());
                insertStatement.setString(2, result.getJobId());
                insertStatement.setString(3, result.getJobdivaNo());
                insertStatement.setString(4, result.getCandidate());
                insertStatement.setString(5, result.getCompanyName());
                
                // Handle datetime fields
                setDateTime(insertStatement, 6, result.getAssignmentStartDate());
                setDateTime(insertStatement, 7, result.getAssignmentEndDate());
                
                insertStatement.setString(8, result.getJobTitle());
                insertStatement.setString(9, result.getLocation());
                insertStatement.setString(10, result.getExcludedReason());
                
                setDateTime(insertStatement, 11, result.getSrAssignmentStart());
                setDateTime(insertStatement, 12, result.getSrAssignmentEnd());
                
                insertStatement.setString(13, result.getStandardRevenueReportMonth());
                insertStatement.setString(14, result.getClienterpid());
                insertStatement.setString(15, result.getBcworkerid());
                
                // Handle BigDecimal fields
                if (result.getAdjustedstbillrate() != null) {
                    insertStatement.setBigDecimal(16, result.getAdjustedstbillrate());
                } else {
                    insertStatement.setNull(16, Types.DECIMAL);
                }
                
                if (result.getAdjustedstpayrate() != null) {
                    insertStatement.setBigDecimal(17, result.getAdjustedstpayrate());
                } else {
                    insertStatement.setNull(17, Types.DECIMAL);
                }
                
                if (result.getAdjgphrst() != null) {
                    insertStatement.setBigDecimal(18, result.getAdjgphrst());
                } else {
                    insertStatement.setNull(18, Types.DECIMAL);
                }
                
                if (result.getAdjrevenue() != null) {
                    insertStatement.setBigDecimal(19, result.getAdjrevenue());
                } else {
                    insertStatement.setNull(19, Types.DECIMAL);
                }
                
                if (result.getStbillrate() != null) {
                    insertStatement.setBigDecimal(20, result.getStbillrate());
                } else {
                    insertStatement.setNull(20, Types.DECIMAL);
                }
                
                insertStatement.addBatch();
                validRecords++;
            }
            
            if (validRecords > 0) {
                // Execute the batch
                int[] updateCounts = insertStatement.executeBatch();
                connection.commit();
                
                logger.info("JDBC batch executed successfully: {} records processed", validRecords);
                return true;
            } else {
                logger.warn("No valid records in batch to process");
                return false;
            }
            
        } catch (SQLException e) {
            logger.error("JDBC batch execution failed: {}", e.getMessage(), e);
            try {
                connection.rollback();
            } catch (SQLException rollbackEx) {
                logger.error("Failed to rollback transaction", rollbackEx);
            }
            throw e;
        }
    }
    
    /**
     * Helper method to set datetime parameters, handling nulls properly.
     */
    private void setDateTime(PreparedStatement stmt, int parameterIndex, LocalDateTime dateTime) throws SQLException {
        if (dateTime != null) {
            stmt.setTimestamp(parameterIndex, Timestamp.valueOf(dateTime));
        } else {
            stmt.setNull(parameterIndex, Types.TIMESTAMP);
        }
    }
}