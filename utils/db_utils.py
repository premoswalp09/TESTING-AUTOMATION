"""Database utility functions for ETL testing."""
import json
import os
from snowflake.snowpark import Session

def create_session():
    """Create a Snowflake session from connection.json."""
    # Get the directory of the current script
    current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Construct the path to connection.json
    config_path = os.path.join(current_dir, 'config', 'connection.json')
    
    # Load connection parameters
    with open(config_path, 'r') as f:
        connection_params = json.load(f)
    
    # Create and return session
    return Session.builder.configs(connection_params).create()

def execute_query(session, query, description):
    """Execute a SQL query with error handling and logging."""
    # print(f"Executing {description}: {query}")
    return session.sql(query).collect()[0][0]

def get_object_details(ts_id):
    """Extract DB name, Schema name, Table name"""
    l = ts_id.split('-')
    return l[2], l[3], l[5], l[6]