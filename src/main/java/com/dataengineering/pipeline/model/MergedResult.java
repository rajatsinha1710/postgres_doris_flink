package com.dataengineering.pipeline.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.LocalDateTime;
import java.math.BigDecimal;

/**
 * Represents the merged result from PostgreSQL query combining job_activity, jobs, 
 * clients_to_exclude, and standard_revenue tables.
 */
public class MergedResult {
    
    @JsonProperty("activity_id")
    private String activityId;
    
    @JsonProperty("job_id")
    private String jobId;
    
    @JsonProperty("jobdiva_no")
    private String jobdivaNo;
    
    @JsonProperty("candidate")
    private String candidate;
    
    @JsonProperty("company_name")
    private String companyName;
    
    @JsonProperty("assignment_start_date")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime assignmentStartDate;
    
    @JsonProperty("assignment_end_date")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime assignmentEndDate;
    
    @JsonProperty("job_title")
    private String jobTitle;
    
    @JsonProperty("location")
    private String location;
    
    @JsonProperty("excluded_reason")
    private String excludedReason;
    
    @JsonProperty("sr_assignment_start")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime srAssignmentStart;
    
    @JsonProperty("sr_assignment_end")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime srAssignmentEnd;
    
    @JsonProperty("standard_revenue_report_month")
    private String standardRevenueReportMonth;
    
    @JsonProperty("clienterpid")
    private String clienterpid;
    
    @JsonProperty("bcworkerid")
    private String bcworkerid;
    
    @JsonProperty("adjustedstbillrate")
    private BigDecimal adjustedstbillrate;
    
    @JsonProperty("adjustedstpayrate")
    private BigDecimal adjustedstpayrate;
    
    @JsonProperty("adjgphrst")
    private BigDecimal adjgphrst;
    
    @JsonProperty("adjrevenue")
    private BigDecimal adjrevenue;
    
    @JsonProperty("stbillrate")
    private BigDecimal stbillrate;
    
    // Default constructor
    public MergedResult() {}
    
    // Constructor with all fields
    public MergedResult(String activityId, String jobId, String jobdivaNo, String candidate,
                       String companyName, LocalDateTime assignmentStartDate, LocalDateTime assignmentEndDate,
                       String jobTitle, String location, String excludedReason,
                       LocalDateTime srAssignmentStart, LocalDateTime srAssignmentEnd,
                       String standardRevenueReportMonth, String clienterpid, String bcworkerid,
                       BigDecimal adjustedstbillrate, BigDecimal adjustedstpayrate,
                       BigDecimal adjgphrst, BigDecimal adjrevenue, BigDecimal stbillrate) {
        this.activityId = activityId;
        this.jobId = jobId;
        this.jobdivaNo = jobdivaNo;
        this.candidate = candidate;
        this.companyName = companyName;
        this.assignmentStartDate = assignmentStartDate;
        this.assignmentEndDate = assignmentEndDate;
        this.jobTitle = jobTitle;
        this.location = location;
        this.excludedReason = excludedReason;
        this.srAssignmentStart = srAssignmentStart;
        this.srAssignmentEnd = srAssignmentEnd;
        this.standardRevenueReportMonth = standardRevenueReportMonth;
        this.clienterpid = clienterpid;
        this.bcworkerid = bcworkerid;
        this.adjustedstbillrate = adjustedstbillrate;
        this.adjustedstpayrate = adjustedstpayrate;
        this.adjgphrst = adjgphrst;
        this.adjrevenue = adjrevenue;
        this.stbillrate = stbillrate;
    }
    
    /**
     * Converts this MergedResult to JSON string using Jackson ObjectMapper.
     * This method is used by the Doris sink for serialization.
     * Handles null values properly and formats dates correctly for Doris.
     */
    public String toJson() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            mapper.setDateFormat(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
            
            // Configure to include null values explicitly
            mapper.setSerializationInclusion(com.fasterxml.jackson.annotation.JsonInclude.Include.ALWAYS);
            
            return mapper.writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize MergedResult to JSON", e);
        }
    }
    
    // Getters and Setters
    public String getActivityId() { return activityId; }
    public void setActivityId(String activityId) { this.activityId = activityId; }
    
    public String getJobId() { return jobId; }
    public void setJobId(String jobId) { this.jobId = jobId; }
    
    public String getJobdivaNo() { return jobdivaNo; }
    public void setJobdivaNo(String jobdivaNo) { this.jobdivaNo = jobdivaNo; }
    
    public String getCandidate() { return candidate; }
    public void setCandidate(String candidate) { this.candidate = candidate; }
    
    public String getCompanyName() { return companyName; }
    public void setCompanyName(String companyName) { this.companyName = companyName; }
    
    public LocalDateTime getAssignmentStartDate() { return assignmentStartDate; }
    public void setAssignmentStartDate(LocalDateTime assignmentStartDate) { this.assignmentStartDate = assignmentStartDate; }
    
    public LocalDateTime getAssignmentEndDate() { return assignmentEndDate; }
    public void setAssignmentEndDate(LocalDateTime assignmentEndDate) { this.assignmentEndDate = assignmentEndDate; }
    
    public String getJobTitle() { return jobTitle; }
    public void setJobTitle(String jobTitle) { this.jobTitle = jobTitle; }
    
    public String getLocation() { return location; }
    public void setLocation(String location) { this.location = location; }
    
    public String getExcludedReason() { return excludedReason; }
    public void setExcludedReason(String excludedReason) { this.excludedReason = excludedReason; }
    
    public LocalDateTime getSrAssignmentStart() { return srAssignmentStart; }
    public void setSrAssignmentStart(LocalDateTime srAssignmentStart) { this.srAssignmentStart = srAssignmentStart; }
    
    public LocalDateTime getSrAssignmentEnd() { return srAssignmentEnd; }
    public void setSrAssignmentEnd(LocalDateTime srAssignmentEnd) { this.srAssignmentEnd = srAssignmentEnd; }
    
    public String getStandardRevenueReportMonth() { return standardRevenueReportMonth; }
    public void setStandardRevenueReportMonth(String standardRevenueReportMonth) { this.standardRevenueReportMonth = standardRevenueReportMonth; }
    
    public String getClienterpid() { return clienterpid; }
    public void setClienterpid(String clienterpid) { this.clienterpid = clienterpid; }
    
    public String getBcworkerid() { return bcworkerid; }
    public void setBcworkerid(String bcworkerid) { this.bcworkerid = bcworkerid; }
    
    public BigDecimal getAdjustedstbillrate() { return adjustedstbillrate; }
    public void setAdjustedstbillrate(BigDecimal adjustedstbillrate) { this.adjustedstbillrate = adjustedstbillrate; }
    
    public BigDecimal getAdjustedstpayrate() { return adjustedstpayrate; }
    public void setAdjustedstpayrate(BigDecimal adjustedstpayrate) { this.adjustedstpayrate = adjustedstpayrate; }
    
    public BigDecimal getAdjgphrst() { return adjgphrst; }
    public void setAdjgphrst(BigDecimal adjgphrst) { this.adjgphrst = adjgphrst; }
    
    public BigDecimal getAdjrevenue() { return adjrevenue; }
    public void setAdjrevenue(BigDecimal adjrevenue) { this.adjrevenue = adjrevenue; }
    
    public BigDecimal getStbillrate() { return stbillrate; }
    public void setStbillrate(BigDecimal stbillrate) { this.stbillrate = stbillrate; }
    
    @Override
    public String toString() {
        return "MergedResult{" +
                "activityId='" + activityId + '\'' +
                ", jobId='" + jobId + '\'' +
                ", jobdivaNo='" + jobdivaNo + '\'' +
                ", candidate='" + candidate + '\'' +
                ", companyName='" + companyName + '\'' +
                '}';
    }
}
