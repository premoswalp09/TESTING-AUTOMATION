"""Functions for logging test results."""
import io
from datetime import datetime
from config.config import TEST_LOGS_TABLE, TEST_CASE_RESULTS_STAGE, TEST_SCRIPTS_TABLE, ts_id_error
from utils.pdf_generator import create_pdf_report
from utils.db_utils import get_object_details

def save_test_result_as_pdf(session, result, metadata):
    """
    Save test results as PDF file in the results stage.
    
    Args:
        session: Snowflake session
        result: TestResult object
        metadata: RunMetadata object
        
    Returns:
        str: Path to the saved PDF file
    """
    try:
        # Generate PDF content
        pdf_content = create_pdf_report(result, metadata)
        
        # Create buffer from PDF bytes
        pdf_buffer = io.BytesIO(pdf_content)
        pdf_buffer.seek(0)
        
        # Format the timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        
        # Get database, schema, and table names from ts_id
        DB, SCH, TAB, OP = get_object_details(result.ts_id)
        
        # Define the file path
        pdf_file_path = f"{TEST_CASE_RESULTS_STAGE}/{DB}/{SCH}/{TAB}/{OP}/TS_{result.ts_id}_Test_Results_{timestamp}.pdf"
        
        # Upload to Snowflake stage
        session.file.put_stream(pdf_buffer, pdf_file_path, auto_compress=False)
        
        # print(f"PDF result saved at: {pdf_file_path}")
        return pdf_file_path
    
    except Exception as e:
        error_msg = str(e).replace("'", "''")
        print(f"Error saving PDF result: {error_msg}")
        return "N/A"

def log_test_result(session, result, metadata):
    """Log test execution results to the database."""
    
    # Save results as PDF and get the file path
    if result.status == 'Error' and result.error_description == f"{ts_id_error}":
        # print("Invalid TS_ID, So no results file will get generated")
        result.result_file_path = 'N/A'
    else:
        result.result_file_path = save_test_result_as_pdf(session, result, metadata)
    
    # Insert log entry with the PDF file path
    log_insert_query = f"""
        INSERT INTO {TEST_LOGS_TABLE} 
        (USER_ID, TS_ID, DB_STRUCTURE, VALIDATION_TYPE, EXECUTED_QUERY, SOURCE_COUNT, TARGET_COUNT, STATUS, EXPECTED_RESULT,
        ACTUAL_RESULT, MINUS_QUERY_FILE_PATH, EXECUTION_TIME, EXECUTION_DATE, ERROR_DESCRIPTION, RUN_ID, 
        RESULT_FILE_PATH)
        VALUES 
        ('{metadata.user_id}', '{result.ts_id}', '{result.db_structure}', '{result.validation_type}', 
        '{result.executed_query.replace("'", "''")}', 
        {result.source_count if result.source_count != "NULL" else "NULL"}, 
        {result.target_count if result.target_count != "NULL" else "NULL"}, 
        '{result.status}','{result.expected_result}', '{result.actual_result}', '{result.minus_query_file_path}', 
        {result.execution_time}, '{metadata.execution_date}', '{result.error_description}', '{metadata.run_id}', 
        '{result.result_file_path}')
    """
    
    # print(f"Inserting Log: {log_insert_query}")
    session.sql(log_insert_query).collect()

def update_test_status(session, ts_id, status):
    """Update test case status if test passes."""
    if status == "Pass":
        update_query = f"""
            UPDATE {TEST_SCRIPTS_TABLE}
            SET ACTIVE_FLAG = 'N'
            WHERE TS_ID = '{ts_id}'
        """
        # print(f"Updating ACTIVE_FLAG: {update_query}")
        session.sql(update_query).collect()