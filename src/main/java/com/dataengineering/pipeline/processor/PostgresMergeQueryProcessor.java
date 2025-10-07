package com.dataengineering.pipeline.processor;

import com.dataengineering.pipeline.config.PipelineConfig;
import com.dataengineering.pipeline.model.MergedResult;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

/**
 * PostgreSQL merge query function using HikariCP connection pooling and JDBC prepared statements.
 * Executes the complex merge query and maps ResultSet to MergedResult objects.
 * Handles different ID types from different CDC topics.
 */
public class PostgresMergeQueryProcessor extends RichMapFunction<List<String>, List<MergedResult>> {
    
    private static final Logger logger = LoggerFactory.getLogger(PostgresMergeQueryProcessor.class);
    
    // The complex merge query with OR clauses to match IDs from all topics
    private static final String MERGE_QUERY = 
        "SELECT " +
        "    ja.activity_id, " +
        "    ja.job_id, " +
        "    ja.jobdiva_no, " +
        "    ja.candidate_name, " +
        "    ja.company_name, " +
        "    ja.assignment_start_date, " +
        "    ja.assignment_end_date, " +
        "    j.job_title, " +
        "    j.location, " +
        "    ex.exclusion_reason AS excluded_reason, " +
        "    sr.assignment_start_date AS sr_assignment_start, " +
        "    sr.assignment_end_date AS sr_assignment_end, " +
        "    srr.standard_revenue_report_month, " +
        "    srr.clienterpid, " +
        "    srr.bcworkerid, " +
        "    srr.adjusted_st_bill_rate, " +
        "    srr.adjusted_st_pay_rate, " +
        "    srr.adj_gph_rst, " +
        "    srr.adj_revenue, " +
        "    srr.st_bill_rate " +
        "FROM job_activity ja " +
        "LEFT JOIN jobs j " +
        "    ON ja.job_id = j.job_id " +
        "LEFT JOIN clients_to_exclude ex " +
        "    ON ja.company_name = ex.client_id " +
        "LEFT JOIN ( " +
        "    SELECT DISTINCT ON (ja2.jobdiva_no, ja2.candidate_name, ja2.assignment_start_date) " +
        "        ja2.jobdiva_no, " +
        "        ja2.candidate_name, " +
        "        ja2.assignment_start_date, " +
        "        ja2.assignment_end_date " +
        "    FROM job_activity ja2 " +
        "    WHERE ja2.bcworkerid IS NOT NULL " +
        ") sr " +
        "    ON ja.jobdiva_no = sr.jobdiva_no " +
        "    AND ja.candidate_name = sr.candidate_name " +
        "LEFT JOIN ( " +
        "    SELECT DISTINCT ON (sr2.jobdivajobref, sr2.jobdiva_candidate_id, sr2.assignment_start_date) " +
        "        sr2.standard_revenue_report_month, " +
        "        sr2.clienterpid, " +
        "        sr2.bcworkerid, " +
        "        sr2.assignment_start_date, " +
        "        sr2.assignment_end_date, " +
        "        sr2.jobdivajobref, " +
        "        sr2.adjusted_st_bill_rate, " +
        "        sr2.adjusted_st_pay_rate, " +
        "        sr2.adj_gph_rst, " +
        "        sr2.jobdiva_candidate_id, " +
        "        sr2.adj_revenue, " +
        "        sr2.st_bill_rate " +
        "    FROM standard_revenue sr2 " +
        "    WHERE sr2.record_active = 'Y' " +
        "    ORDER BY " +
        "        sr2.jobdivajobref, " +
        "        sr2.jobdiva_candidate_id, " +
        "        sr2.assignment_start_date, " +
        "        sr2.standard_revenue_report_month DESC, " +
        "        sr2.rate_table_start_date DESC, " +
        "        sr2.bloc_data_gc_id DESC " +
        ") srr " +
        "    ON ja.jobdiva_no = srr.jobdivajobref " +
        "    AND ja.candidate_name = srr.jobdiva_candidate_id " +
        "    AND sr.assignment_start_date = srr.assignment_start_date " +
        "WHERE (" +
        "    ja.activity_id::text = ANY(?) OR " +  // Primary key from job_activity topic
        "    j.job_id::text = ANY(?) OR " +        // Primary key from jobs topic  
        "    ex.client_id::text = ANY(?)" +        // Primary key from clients_to_exclude topic
        ")";
    
    private HikariDataSource dataSource;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // Initialize HikariCP connection pool
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(PipelineConfig.POSTGRES_URL);
        config.setUsername(PipelineConfig.POSTGRES_USERNAME);
        config.setPassword(PipelineConfig.POSTGRES_PASSWORD);
        config.setMaximumPoolSize(PipelineConfig.POSTGRES_MAX_CONNECTIONS);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(30000);
        config.setIdleTimeout(600000);
        config.setMaxLifetime(1800000);
        config.setLeakDetectionThreshold(60000);
        
        // PostgreSQL specific settings
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.addDataSourceProperty("useServerPrepStmts", "true");
        config.addDataSourceProperty("ssl", "false");
        
        dataSource = new HikariDataSource(config);
        
        logger.info("HikariCP connection pool initialized with max connections: {}", 
                   PipelineConfig.POSTGRES_MAX_CONNECTIONS);
    }
    
    @Override
    public void close() throws Exception {
        super.close();
        
        if (dataSource != null) {
            dataSource.close();
            logger.info("HikariCP connection pool closed");
        }
    }
    
    @Override
    public List<MergedResult> map(List<String> extractedIds) throws Exception {
        List<MergedResult> results = new ArrayList<>();
        
        if (extractedIds == null || extractedIds.isEmpty()) {
            logger.debug("No IDs to process");
            return results;
        }
        
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        
        try {
            // Get connection from HikariCP pool
            connection = dataSource.getConnection();
            
            // Prepare the statement
            preparedStatement = connection.prepareStatement(MERGE_QUERY);
            
            // Convert List<String> to Array for PostgreSQL ANY clause
            Array idArray = connection.createArrayOf("text", extractedIds.toArray());
            
            // Set parameters for OR clauses using primary key IDs from all topics
            // This allows matching primary key IDs across all relevant tables
            for (int i = 1; i <= 3; i++) {
                preparedStatement.setArray(i, idArray);
            }
            
            // Execute query
            resultSet = preparedStatement.executeQuery();
            
            // Map ResultSet to MergedResult objects
            while (resultSet.next()) {
                MergedResult mergedResult = mapResultSetToMergedResult(resultSet);
                results.add(mergedResult);
            }
            
            logger.info("Processed {} primary key IDs from job_activity, jobs, and clients_to_exclude topics, returned {} merged results. Primary Key IDs: {}", 
                       extractedIds.size(), results.size(), extractedIds);
            
        } catch (SQLException e) {
            logger.error("Failed to execute merge query for IDs: {}", extractedIds, e);
            throw new RuntimeException("Database query failed", e);
        } finally {
            // Close resources in reverse order
            if (resultSet != null) {
                try { resultSet.close(); } catch (SQLException e) { logger.warn("Error closing ResultSet", e); }
            }
            if (preparedStatement != null) {
                try { preparedStatement.close(); } catch (SQLException e) { logger.warn("Error closing PreparedStatement", e); }
            }
            if (connection != null) {
                try { connection.close(); } catch (SQLException e) { logger.warn("Error closing Connection", e); }
            }
        }
        
        return results;
    }
    
    /**
     * Maps a ResultSet row to a MergedResult object.
     * Handles null values and proper type conversions.
     */
    private MergedResult mapResultSetToMergedResult(ResultSet rs) throws SQLException {
        MergedResult result = new MergedResult();
        
        // Basic job activity fields
        result.setActivityId(getStringOrNull(rs, "activity_id"));
        result.setJobId(getStringOrNull(rs, "job_id"));
        result.setJobdivaNo(getStringOrNull(rs, "jobdiva_no"));
        result.setCandidate(getStringOrNull(rs, "candidate_name"));
        result.setCompanyName(getStringOrNull(rs, "company_name"));
        result.setAssignmentStartDate(getDateOrNull(rs, "assignment_start_date"));
        result.setAssignmentEndDate(getDateOrNull(rs, "assignment_end_date"));
        
        // Job fields
        result.setJobTitle(getStringOrNull(rs, "job_title"));
        result.setLocation(getStringOrNull(rs, "location"));
        
        // Exclusion reason
        result.setExcludedReason(getStringOrNull(rs, "excluded_reason"));
        
        // Standard revenue fields
        result.setSrAssignmentStart(getDateOrNull(rs, "sr_assignment_start"));
        result.setSrAssignmentEnd(getDateOrNull(rs, "sr_assignment_end"));
        result.setStandardRevenueReportMonth(getStringOrNull(rs, "standard_revenue_report_month"));
        result.setClienterpid(getStringOrNull(rs, "clienterpid"));
        result.setBcworkerid(getStringOrNull(rs, "bcworkerid"));
        
        // Financial fields
        result.setAdjustedstbillrate(getBigDecimalOrNull(rs, "adjusted_st_bill_rate"));
        result.setAdjustedstpayrate(getBigDecimalOrNull(rs, "adjusted_st_pay_rate"));
        result.setAdjgphrst(getBigDecimalOrNull(rs, "adj_gph_rst"));
        result.setAdjrevenue(getBigDecimalOrNull(rs, "adj_revenue"));
        result.setStbillrate(getBigDecimalOrNull(rs, "st_bill_rate"));
        
        return result;
    }
    
    /**
     * Helper method to safely get String values from ResultSet.
     */
    private String getStringOrNull(ResultSet rs, String columnName) throws SQLException {
        String value = rs.getString(columnName);
        return rs.wasNull() ? null : value;
    }
    
    /**
     * Helper method to safely get Date values from ResultSet and convert to LocalDateTime.
     * Converts LocalDate to LocalDateTime with time set to 00:00:00 for Doris compatibility.
     */
    private LocalDateTime getDateOrNull(ResultSet rs, String columnName) throws SQLException {
        Date date = rs.getDate(columnName);
        if (rs.wasNull()) {
            return null;
        }
        // Convert LocalDate to LocalDateTime with time set to 00:00:00
        LocalDate localDate = date.toLocalDate();
        return localDate.atTime(LocalTime.MIDNIGHT);
    }
    
    /**
     * Helper method to safely get BigDecimal values from ResultSet.
     */
    private BigDecimal getBigDecimalOrNull(ResultSet rs, String columnName) throws SQLException {
        BigDecimal value = rs.getBigDecimal(columnName);
        return rs.wasNull() ? null : value;
    }
}
