package com.dataengineering.pipeline.sink;

import com.dataengineering.pipeline.config.PipelineConfig;
import com.dataengineering.pipeline.model.CdcEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Enhanced Doris JDBC sink for selective column updates.
 * Dynamically generates SQL statements based on which source table triggered the CDC event.
 * Only updates relevant columns without touching other fields, avoiding PostgreSQL dependency.
 */
public class EnhancedDorisJdbcSink extends RichSinkFunction<Map<String, Object>> {
    
    private static final Logger logger = LoggerFactory.getLogger(EnhancedDorisJdbcSink.class);
    
    private Connection connection;
    private final Map<String, PreparedStatement> preparedStatements = new ConcurrentHashMap<>();
    
    // Doris JDBC connection details
    private static final String DORIS_JDBC_URL = "jdbc:mysql://127.0.0.1:9030/job_analytics";
    
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
            
            connection.setAutoCommit(true); // Auto-commit for individual upserts
            
            logger.info("Enhanced Doris JDBC sink initialized for selective updates");
            
            // Test connection
            testDorisConnection();
            
        } catch (Exception e) {
            logger.error("Failed to initialize Enhanced Doris JDBC connection", e);
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
                logger.info("Enhanced Doris JDBC connection test successful");
            }
            rs.close();
            testStatement.close();
        } catch (Exception e) {
            logger.error("Enhanced Doris JDBC connection test failed", e);
        }
    }
    
    @Override
    public void close() throws Exception {
        super.close();
        
        // Close all prepared statements
        for (PreparedStatement ps : preparedStatements.values()) {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
        preparedStatements.clear();
        
        // Close JDBC connection
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
        
        logger.info("Enhanced Doris JDBC sink closed");
    }
    
    @Override
    public void invoke(Map<String, Object> dorisColumns, Context context) throws Exception {
        if (dorisColumns == null || dorisColumns.isEmpty()) {
            logger.debug("Received empty or null columns, skipping");
            return;
        }
        
        try {
            // Extract source table information from the context
            String sourceTable = (String) dorisColumns.get("__source_table__");
            if (sourceTable == null) {
                logger.warn("No source table information found in columns: {}, skipping update", dorisColumns.keySet());
                return;
            }
            
            logger.info("Processing selective upsert for source table: {} with columns: {}", sourceTable, dorisColumns.keySet());
            
            // Execute selective upsert based on source table
            executeSelectiveUpsert(sourceTable, dorisColumns);
            
            logger.info("Successfully processed selective upsert for source table: {}", sourceTable);
            
        } catch (SQLException sqle) {
            logger.error("SQL error during selective upsert for source table: {}, columns: {}, error: {}", 
                        dorisColumns.get("__source_table__"), dorisColumns.keySet(), sqle.getMessage());
            // Don't rethrow SQL exceptions to prevent pipeline failure
        } catch (Exception e) {
            logger.error("Unexpected error during selective upsert for: {}", dorisColumns, e);
            // Continue processing other events
        }
    }
    
    /**
     * Executes selective upsert based on source table.
     * Only updates columns relevant to the source table that triggered the CDC event.
     */
    private void executeSelectiveUpsert(String sourceTable, Map<String, Object> dorisColumns) throws SQLException {
        switch (sourceTable.toLowerCase()) {
            case "job_activity":
                executeJobActivityUpsert(dorisColumns);
                break;
            case "jobs":
                executeJobsUpsert(dorisColumns);
                break;
            case "clients_to_exclude":
                executeClientsToExcludeUpsert(dorisColumns);
                break;
            case "standard_revenue":
                executeStandardRevenueUpsert(dorisColumns);
                break;
            default:
                logger.warn("Unknown source table: {}, skipping update", sourceTable);
        }
    }
    
    /**
     * Upserts job_activity table data using simple INSERT.
     * Doris UNIQUE KEY table with merge-on-write handles duplicates automatically.
     */
    private void executeJobActivityUpsert(Map<String, Object> dorisColumns) throws SQLException {
        String upsertSql = "INSERT INTO merged_job_data " +
                          "(activity_id, job_id, company_name, jobdiva_no, candidate, assignment_start_date, assignment_end_date, bcworkerid) " +
                          "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        
        PreparedStatement ps = getOrCreatePreparedStatement("job_activity_upsert", upsertSql);
        
        setParameter(ps, 1, dorisColumns.get("activity_id"), Types.VARCHAR);
        setParameter(ps, 2, dorisColumns.get("job_id"), Types.VARCHAR);
        setParameter(ps, 3, dorisColumns.get("company_name"), Types.VARCHAR);
        setParameter(ps, 4, dorisColumns.get("jobdiva_no"), Types.VARCHAR);
        setParameter(ps, 5, dorisColumns.get("candidate"), Types.VARCHAR);
        setParameter(ps, 6, dorisColumns.get("assignment_start_date"), Types.TIMESTAMP);
        setParameter(ps, 7, dorisColumns.get("assignment_end_date"), Types.TIMESTAMP);
        setParameter(ps, 8, dorisColumns.get("bcworkerid"), Types.VARCHAR);
        
        int rowsAffected = ps.executeUpdate();
        
        logger.debug("Job activity upsert executed - Rows affected: {}", rowsAffected);
    }
    
    /**
     * Upserts jobs table data using simple INSERT.
     * Creates job records with placeholder unique key values.
     */
    private void executeJobsUpsert(Map<String, Object> dorisColumns) throws SQLException {
        String upsertSql = "INSERT INTO merged_job_data " +
                          "(activity_id, job_id, company_name, job_title, location) " +
                          "VALUES (?, ?, ?, ?, ?)";
        
        PreparedStatement ps = getOrCreatePreparedStatement("jobs_upsert", upsertSql);
        
        // Use job_id as unique identifier with placeholder values
        setParameter(ps, 1, dorisColumns.get("job_id"), Types.VARCHAR); // activity_id placeholder
        setParameter(ps, 2, dorisColumns.get("job_id"), Types.VARCHAR);
        setParameter(ps, 3, "job_record", Types.VARCHAR); // company_name placeholder
        setParameter(ps, 4, dorisColumns.get("job_title"), Types.VARCHAR);
        setParameter(ps, 5, dorisColumns.get("location"), Types.VARCHAR);
        
        int rowsAffected = ps.executeUpdate();
        
        logger.debug("Jobs upsert executed - Rows affected: {}", rowsAffected);
    }
    
    /**
     * Creates a revenue record when no existing record is found.
     */
    private void createRevenueRecord(Map<String, Object> dorisColumns) throws SQLException {
        String sql = "INSERT INTO merged_job_data " +
                    "(bcworkerid, standard_revenue_report_month, clienterpid, adjustedstbillrate, " +
                    "adjustedstpayrate, adjgphrst, adjrevenue, stbillrate, sr_assignment_start, sr_assignment_end) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        PreparedStatement ps = getOrCreatePreparedStatement("revenue_record_insert", sql);
        
        setParameter(ps, 1, dorisColumns.get("bcworkerid"), Types.VARCHAR);
        setParameter(ps, 2, dorisColumns.get("standard_revenue_report_month"), Types.VARCHAR);
        setParameter(ps, 3, dorisColumns.get("clienterpid"), Types.VARCHAR);
        setParameter(ps, 4, dorisColumns.get("adjustedstbillrate"), Types.DECIMAL);
        setParameter(ps, 5, dorisColumns.get("adjustedstpayrate"), Types.DECIMAL);
        setParameter(ps, 6, dorisColumns.get("adjgphrst"), Types.DECIMAL);
        setParameter(ps, 7, dorisColumns.get("adjrevenue"), Types.DECIMAL);
        setParameter(ps, 8, dorisColumns.get("stbillrate"), Types.DECIMAL);
        setParameter(ps, 9, dorisColumns.get("sr_assignment_start"), Types.TIMESTAMP);
        setParameter(ps, 10, dorisColumns.get("sr_assignment_end"), Types.TIMESTAMP);
        
        ps.executeUpdate();
        logger.debug("Revenue record created for bcworkerid: {}", dorisColumns.get("bcworkerid"));
    }
    
    /**
     * Upserts clients_to_exclude table data using simple INSERT.
     */
    private void executeClientsToExcludeUpsert(Map<String, Object> dorisColumns) throws SQLException {
        String upsertSql = "INSERT INTO merged_job_data " +
                          "(activity_id, job_id, company_name, excluded_reason) " +
                          "VALUES (?, ?, ?, ?)";
        
        PreparedStatement ps = getOrCreatePreparedStatement("clients_exclude_upsert", upsertSql);
        
        // Use company_name as unique identifier with placeholder values
        setParameter(ps, 1, "client_record", Types.VARCHAR); // activity_id placeholder
        setParameter(ps, 2, "client_record", Types.VARCHAR); // job_id placeholder
        setParameter(ps, 3, dorisColumns.get("company_name"), Types.VARCHAR);
        setParameter(ps, 4, dorisColumns.get("excluded_reason"), Types.VARCHAR);
        
        int rowsAffected = ps.executeUpdate();
        
        logger.debug("Clients to exclude upsert executed - Rows affected: {}", rowsAffected);
    }
    
    /**
     * Upserts standard_revenue table data using simple INSERT.
     */
    private void executeStandardRevenueUpsert(Map<String, Object> dorisColumns) throws SQLException {
        String upsertSql = "INSERT INTO merged_job_data " +
                          "(activity_id, job_id, company_name, bcworkerid, standard_revenue_report_month, " +
                          "clienterpid, adjustedstbillrate, adjustedstpayrate, adjgphrst, adjrevenue, " +
                          "stbillrate, sr_assignment_start, sr_assignment_end) " +
                          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        PreparedStatement ps = getOrCreatePreparedStatement("standard_revenue_upsert", upsertSql);
        
        // Use bcworkerid as unique identifier with placeholder values
        setParameter(ps, 1, dorisColumns.get("bcworkerid"), Types.VARCHAR); // activity_id placeholder
        setParameter(ps, 2, "revenue_record", Types.VARCHAR); // job_id placeholder
        setParameter(ps, 3, "revenue_record", Types.VARCHAR); // company_name placeholder
        setParameter(ps, 4, dorisColumns.get("bcworkerid"), Types.VARCHAR);
        setParameter(ps, 5, dorisColumns.get("standard_revenue_report_month"), Types.VARCHAR);
        setParameter(ps, 6, dorisColumns.get("clienterpid"), Types.VARCHAR);
        setParameter(ps, 7, dorisColumns.get("adjustedstbillrate"), Types.DECIMAL);
        setParameter(ps, 8, dorisColumns.get("adjustedstpayrate"), Types.DECIMAL);
        setParameter(ps, 9, dorisColumns.get("adjgphrst"), Types.DECIMAL);
        setParameter(ps, 10, dorisColumns.get("adjrevenue"), Types.DECIMAL);
        setParameter(ps, 11, dorisColumns.get("stbillrate"), Types.DECIMAL);
        setParameter(ps, 12, dorisColumns.get("sr_assignment_start"), Types.TIMESTAMP);
        setParameter(ps, 13, dorisColumns.get("sr_assignment_end"), Types.TIMESTAMP);
        
        int rowsAffected = ps.executeUpdate();
        
        logger.debug("Standard revenue upsert executed - Rows affected: {}", rowsAffected);
    }
    
    /**
     * Gets or creates a prepared statement for the given key and SQL.
     */
    private PreparedStatement getOrCreatePreparedStatement(String key, String sql) throws SQLException {
        return preparedStatements.computeIfAbsent(key, k -> {
            try {
                return connection.prepareStatement(sql);
            } catch (SQLException e) {
                logger.error("Failed to prepare statement for key: {}", key, e);
                throw new RuntimeException(e);
            }
        });
    }
    
    /**
     * Helper method to set parameter with proper type handling.
     */
    private void setParameter(PreparedStatement ps, int parameterIndex, Object value, int sqlType) throws SQLException {
        if (value == null) {
            ps.setNull(parameterIndex, sqlType);
            return;
        }
        
        switch (sqlType) {
            case Types.VARCHAR:
                ps.setString(parameterIndex, value.toString());
                break;
            case Types.TIMESTAMP:
                if (value instanceof LocalDateTime) {
                    ps.setTimestamp(parameterIndex, Timestamp.valueOf((LocalDateTime) value));
                } else {
                    ps.setTimestamp(parameterIndex, Timestamp.valueOf(value.toString()));
                }
                break;
            case Types.DECIMAL:
                if (value instanceof BigDecimal) {
                    ps.setBigDecimal(parameterIndex, (BigDecimal) value);
                } else {
                    ps.setBigDecimal(parameterIndex, new BigDecimal(value.toString()));
                }
                break;
            default:
                ps.setObject(parameterIndex, value);
        }
    }
}