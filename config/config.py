"""Configuration settings for ETL Test Automation."""

# Snowflake objects
TEST_SCRIPTS_TABLE = "TEST_AUTOMATION_UTILITY.PUBLIC.TEST_SCRIPTS"
TEST_LOGS_TABLE = "TEST_AUTOMATION_UTILITY.PUBLIC.TEST_LOGS"
RUN_ID_TRACKER_TABLE = "TEST_AUTOMATION_UTILITY.PUBLIC.RUN_ID_TRACKER"

# Snowflake stages
MISMATCH_RESULTS_STAGE = "@TEST_AUTOMATION_UTILITY.PUBLIC.MISMATCH_RESULTS"
TEST_CASE_RESULTS_STAGE = "@TEST_AUTOMATION_UTILITY.PUBLIC.TEST_CASE_RESULTS"

# Test case settings
TEST_ACTIVE_FLAG = "Y"

#TS_ID invalid error message
ts_id_error = "Invalid TS_ID : TS_ID should follow <OQ/PQ>-<FR/NFR>-<DatabaseName>-<SchemaName>-<OperationNo>-<TableName or FileName>-<ValidationType>"

#==========================================================================================================================#
#Mismatch Strings for Result 
mismatch_failed_case = "Mismatch Found"         #Actual result failed case
mismatch_passed_case = "No Mismatch Found"      #Actual result passed case

mismatch_expected_result = "No Mismatch"        #Expected result

mismatch_data_not_refreshed = "Data Not Refreshed, Aborted performing Data Check"     #Data Not Refreshed
mismatch_data_refreshed_expected_result = "Data Refreshed"                            #Data Refreshed Expected result

#==========================================================================================================================#
#Count Strings for Result 
count_failed_case = "Count Not Matched"         #Actual result failed case
count_passed_case = "Count Matched"             #Actual result passed case

count_expected_result = "Count Match"           #Expected result

count_data_refreshed = "[Count Matched] - Data Refreshed"
count_data_not_refreshed = "[Count Matched] - Data Not Refreshed"                     #Data Not Refreshed
count_data_refreshed_expected_result = "Data Refreshed"                               #Data Refreshed Expected result