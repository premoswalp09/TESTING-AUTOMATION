"""
Script Name: clear_scripts.py

Description:
This script truncates the TEST_SCRIPTS table in Snowflake, effectively removing all stored test case records.
Use this before reloading or importing new test scripts into the automation utility.

Usage:
Run this script from the CLI using Python:
> python InteractSF\clear_scripts.py

Configuration:
- Table name is loaded from 'config.py' as TEST_SCRIPTS_TABLE.

Author: Prem Oswal
"""

import sys
import os

# Add root path to access shared modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_utils import create_session
from config.config import TEST_SCRIPTS_TABLE

def clear_scripts():
    session = None
    try:
        session = create_session()

        query = f"TRUNCATE {TEST_SCRIPTS_TABLE}"
        print(f"▶ Executing: {query}")
        session.sql(query).collect()

        print(f"✅ Table {TEST_SCRIPTS_TABLE} has been truncated.")

    except Exception as e:
        print(f"❌ Failed to truncate table {TEST_SCRIPTS_TABLE}: {e}")
    finally:
        if session:
            session.close()
            print("🔒 Snowflake session closed.")

if __name__ == "__main__":
    clear_scripts()
