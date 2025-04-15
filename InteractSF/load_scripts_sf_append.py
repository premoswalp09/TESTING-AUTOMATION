"""
Script Name: load_scripts_sf_append.py

Description:
This script reads test case data from a CSV file (scripts.csv) and appends it into the TEST_SCRIPTS table
in Snowflake. The first row in the CSV is treated as the header and must match the column names in the target table.

Key Features:
- Appends data using Snowpark DataFrame.
- Displays the number of rows being uploaded.
- Automatically uses the Snowflake connection defined in 'connection.json'.

Usage:
Place 'scripts.csv' inside the same directory as this script and run:
> python InteractSF/load_scripts_sf_append.py

Configuration:
- Table name is loaded from 'config.py' as TEST_SCRIPTS_TABLE.
- Connection info is pulled from 'config/connection.json'.

Note:
Ensure that the columns in 'scripts.csv' exactly match the structure of TEST_SCRIPTS table.

Author: Prem Oswal
"""


import sys
import os
import pandas as pd

# Add parent path to access shared modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_utils import create_session
from config.config import TEST_SCRIPTS_TABLE

def load_scripts_to_snowflake():
    session = None
    try:
        # Setup paths
        current_dir = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(current_dir, 'scripts.csv')

        # Read CSV using pandas
        print(f"📄 Reading CSV file: {csv_path}")
        df = pd.read_csv(csv_path)
        row_count = len(df)
        print(f"🔢 Total rows to append: {row_count}")

        if row_count == 0:
            print("⚠️ No rows found in CSV. Skipping upload.")
            return

        # Create Snowflake session
        session = create_session()

        # Upload as a Snowpark DataFrame
        snowpark_df = session.create_dataframe(df)

        # Append to existing table
        snowpark_df.write.mode("append").save_as_table(TEST_SCRIPTS_TABLE)

        print(f"✅ Successfully appended {row_count} rows to Snowflake table: {TEST_SCRIPTS_TABLE}")

    except Exception as e:
        print(f"❌ Failed to load data: {e}")
    finally:
        if session:
            session.close()
            print("🔒 Snowflake session closed.")

if __name__ == "__main__":
    load_scripts_to_snowflake()
