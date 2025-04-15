"""Functions for fetching test cases from the database."""
import pandas as pd
from config.config import TEST_SCRIPTS_TABLE, TEST_ACTIVE_FLAG, RUN_ID_TRACKER_TABLE
from models.data_classes import TestCase, RunMetadata
from utils.query_utils import clean_query

def fetch_active_test_cases(session):
    """Fetch all active test cases from the database."""
    query = f"""
        SELECT TS_ID, APPLICATION_NAME, SCHEMA_NAME, 
               TRIM(VALIDATION_TYPE) AS VALIDATION_TYPE, 
               TRIM(SOURCE_SCRIPT) AS SOURCE_SCRIPT, 
               TRIM(TARGET_SCRIPT) AS TARGET_SCRIPT, 
               TRIM(MINUS_QUERY) AS MINUS_QUERY
        FROM {TEST_SCRIPTS_TABLE}
        WHERE ACTIVE_FLAG = '{TEST_ACTIVE_FLAG}'
    """
    df = session.sql(query).toPandas()
    
    # Convert DataFrame rows to TestCase objects
    test_cases = []
    for _, row in df.iterrows():
        test_case = TestCase(
            ts_id=row['TS_ID'],
            application_name=row['APPLICATION_NAME'],
            schema_name=row['SCHEMA_NAME'],
            validation_type=row['VALIDATION_TYPE'],
            source_script=clean_query(row['SOURCE_SCRIPT']),
            target_script=clean_query(row['TARGET_SCRIPT']),
            minus_query=clean_query(row['MINUS_QUERY']) if pd.notna(row['MINUS_QUERY']) else ""
        )
        test_cases.append(test_case)
    
    return test_cases

def get_run_metadata(session, database_name):
    """Get user, execution timestamp, and generate RUN_ID with per-database counter."""
    # Retrieve user ID and execution timestamp
    user_id = session.sql("SELECT CURRENT_USER()").collect()[0][0]
    execution_date = session.sql("SELECT CURRENT_TIMESTAMP()").collect()[0][0]
    # Ensure an entry exists for the database (insert if not exists)
    session.sql(f"""
        INSERT INTO {RUN_ID_TRACKER_TABLE} (DATABASE_NAME, RUN_NUMBER)
        SELECT '{database_name}', 0
        WHERE NOT EXISTS (SELECT 1 FROM {RUN_ID_TRACKER_TABLE} WHERE DATABASE_NAME = '{database_name}')
    """).collect()
    
    # Increment the counter
    session.sql(f"""
        UPDATE {RUN_ID_TRACKER_TABLE}
        SET RUN_NUMBER = RUN_NUMBER + 1
        WHERE DATABASE_NAME = '{database_name}';
    """).collect()

    # Fetch the updated run number
    run_count = session.sql(f"""
        SELECT RUN_NUMBER FROM {RUN_ID_TRACKER_TABLE} WHERE DATABASE_NAME = '{database_name}';
    """).collect()[0][0]
    
    # Construct RUN_ID
    run_id = f"{database_name}_{run_count}"
    
    return RunMetadata(
        user_id=user_id,
        execution_date=execution_date,
        run_id=run_id,
        database_name=database_name
    )