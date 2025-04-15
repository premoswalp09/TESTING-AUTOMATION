"""Functions for checking data refresh status."""
from services.test_logger import update_test_status
from models.data_classes import TestResult
from config.config import TEST_SCRIPTS_TABLE, TEST_LOGS_TABLE

def get_previous_test_results(session, table_name, run_metadata=None):
    """
    Retrieve the most recent test results for a specific table.
    
    Args:
        session: Snowflake session
        table_name: Name of the table to check previous results for
        
    Returns:
        tuple: (prev_source_count, prev_target_count) if results found, or (None, None) if not found
        str: Error message if an exception occurs, None otherwise
    """
    # Query to get the most recent test results for the table
    current_run_id = run_metadata.run_id
    # print(f"current_run_id:{current_run_id}")
    previous_results_query = f"""
    SELECT SOURCE_COUNT, TARGET_COUNT, EXECUTION_DATE 
    FROM {TEST_LOGS_TABLE}
    WHERE TS_ID LIKE '%-%-%-%-%-{table_name}-Count'
    AND STATUS NOT IN ('Error', 'Fail') AND RUN_ID !='{current_run_id}'
    ORDER BY EXECUTION_DATE DESC
    LIMIT 1;
    """
    
    try:
        # Execute query to get previous results
        previous_results = session.sql(previous_results_query).collect()
        
        # If no previous results found, return None values
        if not previous_results:
            # print("BYPassing")
            return None, None, "N/A"
            
        # Extract previous counts
        prev_source_count = previous_results[0]['SOURCE_COUNT']
        prev_target_count = previous_results[0]['TARGET_COUNT']
        # print(f"From Pervious function :{previous_results}")
        return prev_source_count, prev_target_count, "N/A"
            
    except Exception as e:
        error_msg = str(e).replace("'", "''")
        # print(error_msg)
        # print("We are directly getting here")
        return None, None, f"Error retrieving previous test results: {error_msg}"

def check_data_refresh(session, table_name, current_source_count, current_target_count, run_metadata=None):
    """
    Check if data has been refreshed by comparing current counts with previous run results.
    Also retrieves previous test results internally.
    
    Args:
        session: Snowflake session
        table_name: Name of the table to check
        current_source_count: Current source query count result
        current_target_count: Current target query count result
        
    Returns:
        bool: True if data is refreshed, False otherwise
        tuple: (prev_source_count, prev_target_count) for reference
    """
    # Get previous test results
    prev_source_count, prev_target_count, error_msg = get_previous_test_results(session, table_name, run_metadata)
    # print(f"prev_source_count:{prev_source_count} prev_target_count:{prev_target_count}")
    # If no previous results (first run), consider it as refreshed
    if prev_source_count is None and prev_target_count is None:
        # print("Returning NONE")
        return True, (None, None)
    
    try:
        # Convert counts to integers for comparison
        current_source_int = int(current_source_count)
        current_target_int = int(current_target_count)
        prev_source_int = int(prev_source_count)
        prev_target_int = int(prev_target_count)
        
        # Check if any of the counts has increased
        is_refreshed = (current_source_int > prev_source_int or current_target_int > prev_target_int)
        # print(f"is_refreshed:{is_refreshed} prev_source_count: {prev_source_count} prev_target_count:{prev_target_count}")
        return is_refreshed, (prev_source_count, prev_target_count)
            
    except ValueError as e:
        # Handle case where conversion to int fails
        # print(f"Error converting count values to integers: {str(e)}")
        return False, (prev_source_count, prev_target_count)  # Return False on error
