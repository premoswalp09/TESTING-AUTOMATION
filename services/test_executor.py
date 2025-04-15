"""Functions for executing test cases."""
import time
import pandas as pd
import io
from datetime import datetime

from models.data_classes import TestResult, TestCase
from utils.db_utils import execute_query, get_object_details
from config.config import MISMATCH_RESULTS_STAGE
from services.test_logger import log_test_result, update_test_status
from services.data_refresh_check import check_data_refresh
from utils.query_utils import clean_query
from config.config import TEST_SCRIPTS_TABLE, ts_id_error, mismatch_failed_case, mismatch_passed_case, \
    mismatch_expected_result, mismatch_data_not_refreshed, mismatch_data_refreshed_expected_result, count_failed_case, \
    count_expected_result, count_passed_case, count_data_refreshed_expected_result, count_data_refreshed, count_data_not_refreshed

def execute_count_check(session, test_case, run_metadata=None):
    """Execute count check validation with data refresh verification."""
    # Execute source and target queries to get counts
    source_count = execute_query(session, test_case.source_script, "Source Query")
    target_count = execute_query(session, test_case.target_script, "Target Query")
    
    # First check if counts match
    count_match_status = "Pass" if source_count == target_count else "Fail"
    
    # Initialize result with common values
    result = TestResult(
        ts_id=test_case.ts_id,
        validation_type=test_case.validation_type,
        db_structure=f"{test_case.application_name}/{test_case.schema_name}/{get_object_details(test_case.ts_id)[2]}",
        executed_query=f"{test_case.source_script} | {test_case.target_script}",
        source_count=source_count,
        target_count=target_count
    )
    
    # Check if we need to perform data refresh validation
    ts_id_parts = test_case.ts_id.split('-')
    if len(ts_id_parts) != 7 and ts_id_parts[6].lower() != "count":
        result.status = "Error"
        result.error_description = f"{ts_id_error}"
        result.db_structure = "N/A"
        result.executed_query = "N/A"
        return result
    
    table_name = ts_id_parts[5]
    
    # Now check_data_refresh handles getting previous results internally
    data_refreshed, (prev_source_count, prev_target_count) = check_data_refresh(
        session, table_name, source_count, target_count, run_metadata
    )
    
    # Store previous counts in result for logging
    result.prev_source_count = prev_source_count
    result.prev_target_count = prev_target_count
    
    # Check count match first
    if count_match_status == "Fail":
        result.status = "Fail"
        result.actual_result = f"{count_failed_case}"
        result.expected_result = f"{count_expected_result}"
        result.error_description = "N/A"
    elif count_match_status == "Pass" and (prev_source_count == None and prev_target_count == None):
        result.status = "Pass"
        result.actual_result = f"{count_passed_case}"
        result.expected_result = f"{count_expected_result}"
        result.error_description = "N/A"
    else:
        # If counts match, check data refresh status
        if data_refreshed:
            result.status = "Pass"
            result.actual_result = f"{count_data_refreshed}"
        else:
            result.status = "Fail"
            result.actual_result = f"{count_data_not_refreshed}"
            
        result.expected_result = f"{count_data_refreshed_expected_result}"
        result.error_description = "N/A"
    
    return result

def execute_data_check(session, test_case, run_metadata=None):
    """Execute data check validation using MINUS queries to check both sides."""
    result = TestResult(
        ts_id=test_case.ts_id,
        validation_type=test_case.validation_type,
        db_structure=f"{test_case.application_name}/{test_case.schema_name}/{get_object_details(test_case.ts_id)[2]}",
        executed_query=test_case.minus_query,
        expected_result=f"{mismatch_expected_result}"
    )
    
    minus_query = test_case.minus_query
    
    if not minus_query:
        result.status = "Error"
        result.error_description = "Minus Query missing"
        result.actual_result = "NULL"
        return result
    
    #===========================================================================================#
    # Check Data Refresh
    ts_id_parts = test_case.ts_id.split('-')
    table_name = ts_id_parts[5]
    get_SourceScript_TargetScript = f"""
    SELECT *
    FROM {TEST_SCRIPTS_TABLE}
    WHERE TS_ID LIKE '%-%-%-%-%-{table_name}-Count';
    """
    # Execute get_SourceScript_TargetScript to get required scripts.
    script = session.sql(get_SourceScript_TargetScript).collect()

    if script:  # make sure the list is not empty
        row = script[0]  # get the first row
        Check_test_case = TestCase(
            ts_id=row['TS_ID'],
            application_name=row['APPLICATION_NAME'],
            schema_name=row['SCHEMA_NAME'],
            validation_type=row['VALIDATION_TYPE'],
            source_script=clean_query(row['SOURCE_SCRIPT']),
            target_script=clean_query(row['TARGET_SCRIPT']),
            minus_query=""
        )
        
        try:
            # Execute the count check queries to get current counts
            source_count = execute_query(session, Check_test_case.source_script, "Source Query")
            target_count = execute_query(session, Check_test_case.target_script, "Target Query")
        except Exception as e:
            error_msg = f"Check {Check_test_case.ts_id} test case. Error Due to Data Refresh Functionality"+str(e).replace("'", "''")
            result.error_description = error_msg
            result.executed_query = "N/A"
            return result

        # Now check if data is refreshed
        data_refreshed, _ = check_data_refresh(
            session, table_name, source_count, target_count, run_metadata
        )
        
        if not data_refreshed:
            result.status = "Fail"
            result.actual_result = f"{mismatch_data_not_refreshed}"
            result.expected_result = f"{mismatch_data_refreshed_expected_result}"
            result.error_description = "N/A"
            result.executed_query = "N/A"
            return result
    else:
        # No count check test case found - can't determine refresh state
        # You may want to decide how to handle this case
        pass
    
    #===========================================================================================#
    # Continue with data check if data is refreshed or if we can't determine refresh state
    try:
        # Parse the MINUS query to extract source and target queries
        # We expect minus_query to be in format: "SELECT ... FROM A MINUS SELECT ... FROM B"
        parts = minus_query.upper().split("MINUS")
        
        if len(parts) < 2:
            result.status = "Error"
            result.error_description = "Invalid MINUS query format"
            result.actual_result = "NULL"
            return result
            
        # Extract column list from the first SELECT statement
        source_query = parts[0].strip()
        target_query = parts[1].strip()
        
        # Extract columns from the source query (between SELECT and FROM)
        select_from_parts = source_query.split("FROM", 1)
        if len(select_from_parts) < 2:
            result.status = "Error"
            result.error_description = "Invalid query format - cannot find FROM in source query"
            result.actual_result = "NULL"
            return result
            
        column_list = select_from_parts[0].replace("SELECT", "", 1).strip()
        
        # Construct the bidirectional check query
        bidirectional_query = f"""
        SELECT 'Source_Only' AS DIFFERENCE_TYPE, {column_list}
        FROM (
            {minus_query}
        ) AS SOURCE_DIFF
        UNION ALL
        SELECT 'Target_Only' AS DIFFERENCE_TYPE, {column_list}
        FROM (
            {target_query}
            MINUS
            {source_query}
        ) AS TARGET_DIFF
        """
        
        minus_df = session.sql(bidirectional_query).toPandas()

        # Check if there are mismatches
        if not minus_df.empty:
            # Convert DataFrame to CSV in memory
            csv_buffer = io.BytesIO()
            minus_df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)  # Reset buffer position
            
            # Generate a unique file name with a readable timestamp
            DB, SCH, TAB, OP = get_object_details(test_case.ts_id)
            timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
            result.minus_query_file_path = f"{MISMATCH_RESULTS_STAGE}/{DB}/{SCH}/{TAB}/TS_{test_case.ts_id}_mismatch_results_{timestamp}.csv"

            # Upload CSV to Snowflake stage
            session.file.put_stream(csv_buffer, result.minus_query_file_path, auto_compress=False)
            
            result.status = "Fail"
            result.actual_result = f"{mismatch_failed_case}"
        else:
            result.status = "Pass"
            result.actual_result = f"{mismatch_passed_case}"

        return result

    except Exception as e:
        error_msg = str(e).replace("'", "''")
        result.status = "Error"
        result.error_description = error_msg
        result.actual_result = "NULL"
        return result
    
def execute_test_case(session, test_case, run_metadata):
    """Execute a single test case and log results."""
    # Initialize test result object
    result = TestResult(
        ts_id=test_case.ts_id,
        validation_type=test_case.validation_type,
        db_structure=f"{test_case.application_name}/{test_case.schema_name}/{get_object_details(test_case.ts_id)[2]}",
        executed_query="N/A"
    )
    
    start_time = time.time()

    # Check if TS_ID format is valid
    if len(test_case.ts_id.split('-')) != 7:
        result.status = "Error"
        result.error_description = f"{ts_id_error}"
        result.execution_time = round(time.time() - start_time, 2)
        result.db_structure = "N/A"
        
        # Log results
        log_test_result(session, result, run_metadata)
        return result.status
    
    try:
        # Execute appropriate test based on validation type
        if test_case.validation_type.lower() == "count check":
            result = execute_count_check(session, test_case, run_metadata)
            
        elif test_case.validation_type.lower() == "data check":
            result = execute_data_check(session, test_case, run_metadata)
            
        else:
            raise ValueError(f"Unsupported validation type: {test_case.validation_type}")
    
    except Exception as e:
        result.status = "Error"
        result.error_description = str(e).replace("'", "''")
        result.executed_query = (f"{test_case.source_script} | {test_case.target_script}" 
                               if test_case.validation_type.lower() == "count check" 
                               else test_case.minus_query)
    
    result.execution_time = round(time.time() - start_time, 2)
    
    # Log results and update test status
    log_test_result(session, result, run_metadata)
    
    # Only update the test status in the test scripts table if it passes
    # For data refresh checks, we already update all related test cases in check_data_refresh
    update_test_status(session, test_case.ts_id, result.status)
    
    return result.status