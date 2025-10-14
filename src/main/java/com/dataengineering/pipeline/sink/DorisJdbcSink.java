package com.dataengineering.pipeline.sink;

import com.dataengineering.pipeline.config.PipelineConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * Streamlined Doris JDBC sink for direct CDC event upserts.
 * Uses INSERT ... ON DUPLICATE KEY UPDATE for upsert operations.
 * No batching complexity - simple, direct upserts per event.
 */
public class DorisJdbcSink extends RichSinkFunction<Map<String, Object>> {
    
    private static final Logger logger = LoggerFactory.getLogger(DorisJdbcSink.class);
    
    private Connection connection;
    private PreparedStatement upsertStatement;
    
    // Doris JDBC connection details
    private static final String DORIS_JDBC_URL = "jdbc:mysql://127.0.0.1:9030/job_analytics";
    
    // Upsert SQL with ON DUPLICATE KEY UPDATE for all columns
    private static final String UPSERT_SQL = 
        "INSERT INTO merged_job_data (" +
        "activity_id, job_id, jobdiva_no, candidate, company_name, " +
        "assignment_start_date, assignment_end_date, job_title, location, excluded_reason, " +
        "sr_assignment_start, sr_assignment_end, standard_revenue_report_month, " +
        "clienterpid, bcworkerid, adjustedstbillrate, adjustedstpayrate, " +
        "adjgphrst, adjrevenue, stbillrate" +
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
        "ON DUPLICATE KEY UPDATE " +
        "job_id = VALUES(job_id), " +
        "jobdiva_no = VALUES(jobdiva_no), " +
        "candidate = VALUES(candidate), " +
        "company_name = VALUES(company_name), " +
        "assignment_start_date = VALUES(assignment_start_date), " +
        "assignment_end_date = VALUES(assignment_end_date), " +
        "job_title = VALUES(job_title), " +
        "location = VALUES(location), " +
        "excluded_reason = VALUES(excluded_reason), " +
        "sr_assignment_start = VALUES(sr_assignment_start), " +
        "sr_assignment_end = VALUES(sr_assignment_end), " +
        "standard_revenue_report_month = VALUES(standard_revenue_report_month), " +
        "clienterpid = VALUES(clienterpid), " +
        "bcworkerid = VALUES(bcworkerid), " +
        "adjustedstbillrate = VALUES(adjustedstbillrate), " +
        "adjustedstpayrate = VALUES(adjustedstpayrate), " +
        "adjgphrst = VALUES(adjgphrst), " +
        "adjrevenue = VALUES(adjrevenue), " +
        "stbillrate = VALUES(stbillrate)";
    
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
            
            connection.setAutoCommit(true); // Auto-commit for simple upserts
            
            // Prepare the UPSERT statement
            upsertStatement = connection.prepareStatement(UPSERT_SQL);
            
            logger.info("Doris JDBC sink initialized for direct CDC upserts");
            
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
        
        // Close JDBC resources
        if (upsertStatement != null) {
            upsertStatement.close();
        }
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
        
        logger.info("Doris JDBC sink closed");
    }
    
    @Override
    public void invoke(Map<String, Object> dorisColumns, Context context) throws Exception {
        if (dorisColumns == null || dorisColumns.isEmpty()) {
            return;
        }
        
        try {
            // Execute direct upsert for each CDC event
            executeUpsert(dorisColumns);
            
        } catch (Exception e) {
            logger.error("Failed to upsert CDC event to Doris: {}", dorisColumns, e);
            // Continue processing other events
        }
    }
    
    /**
     * Executes a single upsert operation for CDC event columns.
     */
    private void executeUpsert(Map<String, Object> dorisColumns) throws SQLException {
        // Set parameters for the upsert statement
        setUpsertParameters(dorisColumns);
        
        // Execute the upsert
        int rowsAffected = upsertStatement.executeUpdate();
        
        logger.debug("Upsert executed - Rows affected: {}, Columns: {}", rowsAffected, dorisColumns.keySet());
    }
    
    /**
     * Sets parameters for the upsert statement based on CDC event columns.
     */
    private void setUpsertParameters(Map<String, Object> dorisColumns) throws SQLException {
        // Set all parameters to null first
        for (int i = 1; i <= 20; i++) {
            upsertStatement.setNull(i, Types.NULL);
        }
        
        // Map columns to parameter positions
        setParameterIfPresent(dorisColumns, "activity_id", 1, Types.VARCHAR);
        setParameterIfPresent(dorisColumns, "job_id", 2, Types.VARCHAR);
        setParameterIfPresent(dorisColumns, "jobdiva_no", 3, Types.VARCHAR);
        setParameterIfPresent(dorisColumns, "candidate", 4, Types.VARCHAR);
        setParameterIfPresent(dorisColumns, "company_name", 5, Types.VARCHAR);
        setParameterIfPresent(dorisColumns, "assignment_start_date", 6, Types.TIMESTAMP);
        setParameterIfPresent(dorisColumns, "assignment_end_date", 7, Types.TIMESTAMP);
        setParameterIfPresent(dorisColumns, "job_title", 8, Types.VARCHAR);
        setParameterIfPresent(dorisColumns, "location", 9, Types.VARCHAR);
        setParameterIfPresent(dorisColumns, "excluded_reason", 10, Types.VARCHAR);
        setParameterIfPresent(dorisColumns, "sr_assignment_start", 11, Types.TIMESTAMP);
        setParameterIfPresent(dorisColumns, "sr_assignment_end", 12, Types.TIMESTAMP);
        setParameterIfPresent(dorisColumns, "standard_revenue_report_month", 13, Types.VARCHAR);
        setParameterIfPresent(dorisColumns, "clienterpid", 14, Types.VARCHAR);
        setParameterIfPresent(dorisColumns, "bcworkerid", 15, Types.VARCHAR);
        setParameterIfPresent(dorisColumns, "adjustedstbillrate", 16, Types.DECIMAL);
        setParameterIfPresent(dorisColumns, "adjustedstpayrate", 17, Types.DECIMAL);
        setParameterIfPresent(dorisColumns, "adjgphrst", 18, Types.DECIMAL);
        setParameterIfPresent(dorisColumns, "adjrevenue", 19, Types.DECIMAL);
        setParameterIfPresent(dorisColumns, "stbillrate", 20, Types.DECIMAL);
    }
    
    /**
     * Helper method to set parameter if present in the map.
     */
    private void setParameterIfPresent(Map<String, Object> dorisColumns, String columnName, 
                                     int parameterIndex, int sqlType) throws SQLException {
        Object value = dorisColumns.get(columnName);
        if (value != null) {
            switch (sqlType) {
                case Types.VARCHAR:
                    upsertStatement.setString(parameterIndex, value.toString());
                    break;
                case Types.TIMESTAMP:
                    if (value instanceof LocalDateTime) {
                        upsertStatement.setTimestamp(parameterIndex, Timestamp.valueOf((LocalDateTime) value));
                    } else {
                        upsertStatement.setTimestamp(parameterIndex, Timestamp.valueOf(value.toString()));
                    }
                    break;
                case Types.DECIMAL:
                    if (value instanceof BigDecimal) {
                        upsertStatement.setBigDecimal(parameterIndex, (BigDecimal) value);
                    } else {
                        upsertStatement.setBigDecimal(parameterIndex, new BigDecimal(value.toString()));
                    }
                    break;
                default:
                    upsertStatement.setObject(parameterIndex, value);
            }
        }
    }
}