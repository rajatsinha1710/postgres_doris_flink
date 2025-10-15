-- Doris Upsert Test Script
-- This script tests the upsert functionality for the merged_job_data table
-- Updated to work with configurable table names

-- 1. Check table structure and key type
SHOW CREATE TABLE job_analytics.merged_job_data;

-- 2. Check current data count
SELECT COUNT(*) as total_records FROM job_analytics.merged_job_data;

-- 3. Show sample data
SELECT * FROM job_analytics.merged_job_data LIMIT 5;

-- 4. Check if test table exists (for testing)
SHOW CREATE TABLE test_job_analytics.test_merged_job_data;

-- 4. Test basic INSERT (should work for new records)
INSERT INTO job_analytics.merged_job_data 
(activity_id, job_id, company_name, jobdiva_no, candidate, assignment_start_date, assignment_end_date, bcworkerid)
VALUES 
('test_activity_1', 'test_job_1', 'Test Company A', 'JD001', 'John Doe', '2024-01-01 00:00:00', '2024-12-31 23:59:59', 'BC001');

-- 5. Check if record was inserted
SELECT * FROM job_analytics.merged_job_data WHERE activity_id = 'test_activity_1';

-- 6. Test UPDATE by inserting same activity_id with different data (should upsert)
INSERT INTO job_analytics.merged_job_data 
(activity_id, job_id, company_name, jobdiva_no, candidate, assignment_start_date, assignment_end_date, bcworkerid)
VALUES 
('test_activity_1', 'test_job_1_updated', 'Test Company A Updated', 'JD001_UPDATED', 'Jane Doe', '2024-02-01 00:00:00', '2024-11-30 23:59:59', 'BC001_UPDATED');

-- 7. Verify upsert worked (should show updated data, not duplicate)
SELECT * FROM job_analytics.merged_job_data WHERE activity_id = 'test_activity_1';

-- 8. Test partial column update (jobs table scenario)
INSERT INTO job_analytics.merged_job_data 
(activity_id, job_id, company_name, job_title, location)
VALUES 
('test_job_record', 'test_job_2', 'job_record', 'Senior Developer', 'New York');

-- 9. Test revenue data upsert (standard_revenue table scenario)  
INSERT INTO job_analytics.merged_job_data 
(activity_id, job_id, company_name, bcworkerid, standard_revenue_report_month, clienterpid, 
 adjustedstbillrate, adjustedstpayrate, adjgphrst, adjrevenue, stbillrate, 
 sr_assignment_start, sr_assignment_end)
VALUES 
('BC001_REV', 'revenue_record', 'revenue_record', 'BC001', '2024-01', 'CLIENT001',
 75.50, 65.00, 10.50, 1500.00, 70.00,
 '2024-01-01 00:00:00', '2024-01-31 23:59:59');

-- 10. Test client exclusion scenario
INSERT INTO job_analytics.merged_job_data 
(activity_id, job_id, company_name, excluded_reason)
VALUES 
('client_record', 'client_record', 'Excluded Company', 'Contract issues');

-- 11. Verify all test records
SELECT 
    activity_id,
    job_id, 
    company_name,
    candidate,
    job_title,
    excluded_reason,
    bcworkerid,
    adjustedstbillrate,
    'test_record' as record_type
FROM job_analytics.merged_job_data 
WHERE activity_id IN ('test_activity_1', 'test_job_record', 'BC001_REV', 'client_record')
ORDER BY activity_id;

-- 12. Test duplicate key behavior with different approach
-- First insert a record
INSERT INTO job_analytics.merged_job_data 
(activity_id, job_id, company_name, candidate)
VALUES 
('dup_test_1', 'job_123', 'Company XYZ', 'Alice Smith');

-- Then try to insert again with same activity_id (primary key)
INSERT INTO job_analytics.merged_job_data 
(activity_id, job_id, company_name, candidate, job_title)
VALUES 
('dup_test_1', 'job_456', 'Company ABC', 'Bob Johnson', 'Data Engineer');

-- Check final result (should show merged/updated data)
SELECT * FROM job_analytics.merged_job_data WHERE activity_id = 'dup_test_1';

-- 13. Clean up test data
DELETE FROM job_analytics.merged_job_data 
WHERE activity_id IN ('test_activity_1', 'test_job_record', 'BC001_REV', 'client_record', 'dup_test_1');

-- 14. Verify cleanup
SELECT COUNT(*) as remaining_test_records 
FROM job_analytics.merged_job_data 
WHERE activity_id IN ('test_activity_1', 'test_job_record', 'BC001_REV', 'client_record', 'dup_test_1');

-- Expected result: 0 records remaining