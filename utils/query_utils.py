"""Query utility functions for ETL testing."""

def clean_query(query):
    """Clean and prepare query for execution."""
    if not query:
        return ""
    return query.strip(";")  # Remove trailing semicolon