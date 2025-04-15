"""
Script Name: Activate_all_test_cases.py

Description:
This script is used to activate all test cases by setting the ACTIVE_FLAG = 'Y' in the Snowflake table
configured as TEST_SCRIPTS_TABLE (defined in config.py). This is useful when you want to rerun all
test cases after deactivating or modifying them.

Usage:
Run this script from the CLI using Python:
> python InteractSF\Activate_all_test_cases.py

Configuration:
- Snowflake connection settings are loaded from 'config/connection.json'.
- Table name is loaded from 'config.py' as TEST_SCRIPTS_TABLE.

Author: Prem Oswal
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_utils import create_session
from config.config import TEST_SCRIPTS_TABLE


def activate_all_test_cases():
    session = None
    try:
        session = create_session()
        query = f"""
        UPDATE {TEST_SCRIPTS_TABLE}
        SET ACTIVE_FLAG = 'Y'
        """
        session.sql(query).collect()
        print(f"✅ All test cases in {TEST_SCRIPTS_TABLE} have been activated.")
    except Exception as e:
        print(f"❌ Failed to activate test cases: {e}")
    finally:
        if session:
            session.close()
            print("🔒 Snowflake session closed.")

if __name__ == "__main__":
    activate_all_test_cases()
