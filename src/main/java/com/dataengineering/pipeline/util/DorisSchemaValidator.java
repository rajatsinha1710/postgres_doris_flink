package com.dataengineering.pipeline.util;

import com.dataengineering.pipeline.model.MergedResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Utility class to validate MergedResult objects against Doris table schema.
 * Helps identify schema mismatches before sending data to Doris.
 */
public class DorisSchemaValidator {
    
    private static final Logger logger = LoggerFactory.getLogger(DorisSchemaValidator.class);
    
    // Doris schema field lengths (from CREATE TABLE statement)
    private static final int ACTIVITY_ID_MAX_LENGTH = 64;
    private static final int JOB_ID_MAX_LENGTH = 64;
    private static final int JOBDIVA_NO_MAX_LENGTH = 64;
    private static final int CANDIDATE_MAX_LENGTH = 128;
    private static final int COMPANY_NAME_MAX_LENGTH = 256;
    private static final int JOB_TITLE_MAX_LENGTH = 256;
    private static final int LOCATION_MAX_LENGTH = 256;
    private static final int EXCLUDED_REASON_MAX_LENGTH = 256;
    private static final int STANDARD_REVENUE_REPORT_MONTH_MAX_LENGTH = 16;
    private static final int CLIENTERPID_MAX_LENGTH = 128;
    private static final int BCWORKERID_MAX_LENGTH = 128;
    
    // Decimal precision and scale for financial fields
    private static final int DECIMAL_PRECISION = 18;
    private static final int DECIMAL_SCALE_4 = 4; // For rate fields
    private static final int DECIMAL_SCALE_2 = 2; // For revenue field
    
    /**
     * Validates a MergedResult object against Doris table schema.
     * 
     * @param result The MergedResult to validate
     * @return true if valid, false if there are schema issues
     */
    public static boolean validate(MergedResult result) {
        boolean isValid = true;
        
        // Validate activity_id (required field)
        if (result.getActivityId() == null || result.getActivityId().trim().isEmpty()) {
            logger.error("Validation failed: activity_id is null or empty");
            isValid = false;
        } else if (result.getActivityId().length() > ACTIVITY_ID_MAX_LENGTH) {
            logger.error("Validation failed: activity_id length {} exceeds max length {}", 
                        result.getActivityId().length(), ACTIVITY_ID_MAX_LENGTH);
            isValid = false;
        }
        
        // Validate string field lengths
        isValid &= validateStringField("job_id", result.getJobId(), JOB_ID_MAX_LENGTH);
        isValid &= validateStringField("jobdiva_no", result.getJobdivaNo(), JOBDIVA_NO_MAX_LENGTH);
        isValid &= validateStringField("candidate", result.getCandidate(), CANDIDATE_MAX_LENGTH);
        isValid &= validateStringField("company_name", result.getCompanyName(), COMPANY_NAME_MAX_LENGTH);
        isValid &= validateStringField("job_title", result.getJobTitle(), JOB_TITLE_MAX_LENGTH);
        isValid &= validateStringField("location", result.getLocation(), LOCATION_MAX_LENGTH);
        isValid &= validateStringField("excluded_reason", result.getExcludedReason(), EXCLUDED_REASON_MAX_LENGTH);
        isValid &= validateStringField("standard_revenue_report_month", result.getStandardRevenueReportMonth(), STANDARD_REVENUE_REPORT_MONTH_MAX_LENGTH);
        isValid &= validateStringField("clienterpid", result.getClienterpid(), CLIENTERPID_MAX_LENGTH);
        isValid &= validateStringField("bcworkerid", result.getBcworkerid(), BCWORKERID_MAX_LENGTH);
        
        // Validate date fields (should be LocalDateTime for Doris datetime type)
        isValid &= validateDateTimeField("assignment_start_date", result.getAssignmentStartDate());
        isValid &= validateDateTimeField("assignment_end_date", result.getAssignmentEndDate());
        isValid &= validateDateTimeField("sr_assignment_start", result.getSrAssignmentStart());
        isValid &= validateDateTimeField("sr_assignment_end", result.getSrAssignmentEnd());
        
        // Validate decimal fields
        isValid &= validateDecimalField("adjustedstbillrate", result.getAdjustedstbillrate(), DECIMAL_SCALE_4);
        isValid &= validateDecimalField("adjustedstpayrate", result.getAdjustedstpayrate(), DECIMAL_SCALE_4);
        isValid &= validateDecimalField("adjgphrst", result.getAdjgphrst(), DECIMAL_SCALE_4);
        isValid &= validateDecimalField("adjrevenue", result.getAdjrevenue(), DECIMAL_SCALE_2);
        isValid &= validateDecimalField("stbillrate", result.getStbillrate(), DECIMAL_SCALE_4);
        
        if (isValid) {
            logger.debug("Schema validation passed for activity_id: {}", result.getActivityId());
        } else {
            logger.error("Schema validation failed for activity_id: {}", result.getActivityId());
        }
        
        return isValid;
    }
    
    /**
     * Validates a string field against maximum length.
     */
    private static boolean validateStringField(String fieldName, String value, int maxLength) {
        if (value != null && value.length() > maxLength) {
            logger.error("Validation failed: {} length {} exceeds max length {}", 
                        fieldName, value.length(), maxLength);
            return false;
        }
        return true;
    }
    
    /**
     * Validates a LocalDateTime field (should not be null for datetime columns).
     */
    private static boolean validateDateTimeField(String fieldName, LocalDateTime value) {
        // DateTime fields can be null in Doris, so this is just for logging
        if (value == null) {
            logger.debug("DateTime field {} is null", fieldName);
        }
        return true;
    }
    
    /**
     * Validates a BigDecimal field against precision and scale.
     */
    private static boolean validateDecimalField(String fieldName, BigDecimal value, int expectedScale) {
        if (value != null) {
            // Check scale
            if (value.scale() > expectedScale) {
                logger.warn("Decimal field {} has scale {} which exceeds expected scale {}", 
                           fieldName, value.scale(), expectedScale);
            }
            
            // Check precision
            if (value.precision() > DECIMAL_PRECISION) {
                logger.error("Validation failed: {} precision {} exceeds max precision {}", 
                            fieldName, value.precision(), DECIMAL_PRECISION);
                return false;
            }
        }
        return true;
    }
    
    /**
     * Logs a summary of validation results for a batch of records.
     */
    public static void logValidationSummary(java.util.List<MergedResult> batch) {
        int validCount = 0;
        int invalidCount = 0;
        
        for (MergedResult result : batch) {
            if (validate(result)) {
                validCount++;
            } else {
                invalidCount++;
            }
        }
        
        logger.info("Schema validation summary: {} valid, {} invalid out of {} total records", 
                   validCount, invalidCount, batch.size());
        
        if (invalidCount > 0) {
            logger.warn("Found {} records with schema validation issues. Check logs for details.", invalidCount);
        }
    }
}
