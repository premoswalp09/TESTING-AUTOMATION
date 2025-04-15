"""Data classes for ETL testing."""

from dataclasses import dataclass
from typing import Optional, Any

@dataclass
class RunMetadata:
    """Data class for run metadata."""
    user_id: str
    execution_date: str
    run_id: str
    database_name: str

@dataclass
class TestCase:
    """Data class for a test case."""
    ts_id: str
    application_name: str
    schema_name: str
    validation_type: str
    source_script: str
    target_script: str
    minus_query: str

@dataclass
class TestResult:
    """Data class for a test result."""
    ts_id: str
    validation_type: str
    db_structure: str
    executed_query: str
    source_count: Any = "NULL"
    target_count: Any = "NULL"
    prev_source_count: Any = "NULL"
    prev_target_count: Any = "NULL"
    status: str = "Error"
    expected_result: str = "NULL"
    actual_result: str = "NULL"
    minus_query_file_path: str = "N/A"
    execution_time: float = 0.0
    error_description: str = "N/A"
    result_file_path: str = "N/A"