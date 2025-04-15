"""
Script Name: reset_utility_execution.py

Description:
This script resets the state of the test automation utility by performing the following actions:
1. Truncates the TEST_LOGS table to remove previous test execution logs.
2. Sets all test cases as active in the TEST_SCRIPTS table.
3. Resets the RUN_NUMBER in the RUN_ID_TRACKER table to 0.
4. Removes all result and mismatch files from the configured Snowflake stages.

Usage:
Run this script from the CLI using Python:
> python InteractSF\reset_utility_execution.py

Configuration:
- Table names and stage locations are loaded from 'config.py':
  - TEST_LOGS_TABLE
  - TEST_SCRIPTS_TABLE
  - RUN_ID_TRACKER_TABLE
  - MISMATCH_RESULTS_STAGE
  - TEST_CASE_RESULTS_STAGE

Author: Prem Oswal
"""

import sys
import os

# Add root directory to sys.path for relative imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_utils import create_session
from config.config import (
    TEST_SCRIPTS_TABLE,
    TEST_LOGS_TABLE,
    RUN_ID_TRACKER_TABLE,
    MISMATCH_RESULTS_STAGE,
    TEST_CASE_RESULTS_STAGE
)

def reset_utility_execution():
    session = None
    try:
        session = create_session()

        queries = [
            f"TRUNCATE {TEST_LOGS_TABLE}",
            f"UPDATE {TEST_SCRIPTS_TABLE} SET ACTIVE_FLAG = 'Y'",
            f"UPDATE {RUN_ID_TRACKER_TABLE} SET RUN_NUMBER = 0",
            f"REMOVE {MISMATCH_RESULTS_STAGE}",
            f"REMOVE {TEST_CASE_RESULTS_STAGE}"
        ]

        for q in queries:
            print(f"▶ Executing: {q}")
            session.sql(q).collect()

        print("✅ Utility execution has been successfully reset.")

    except Exception as e:
        print(f"❌ Failed to reset utility execution: {e}")
    finally:
        if session:
            session.close()
            print("🔒 Snowflake session closed.")

if __name__ == "__main__":
    reset_utility_execution()
