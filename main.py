"""Main entry point for ETL testing utility."""
from utils.db_utils import create_session
from services.test_fetcher import fetch_active_test_cases, get_run_metadata
from services.test_executor import execute_test_case
from tqdm import tqdm

def main():
    """Main handler function for ETL testing."""
    # Create a session
    print("Connecting Snowflake...")
    session = create_session()
    print("✅Connected to Snowflake❄️  |^-^|")

    try:
        # Fetch test cases
        print("Fetching active test cases...")
        test_cases = fetch_active_test_cases(session)
        
        if not test_cases:
            print("No active test cases found.")
            return
        
        print(f"Found {len(test_cases)} active test cases.")
        
        # Group test cases by database name
        test_cases_by_db = {}
        for test_case in test_cases:
            if test_case.application_name not in test_cases_by_db:
                test_cases_by_db[test_case.application_name] = []
            test_cases_by_db[test_case.application_name].append(test_case)
        
        # Generate run IDs for each database separately
        print("Generating run metadata...")
        run_metadata_map = {}
        for db_name in test_cases_by_db.keys():
            run_metadata_map[db_name] = get_run_metadata(session, db_name)
            print(f"  - Generated run ID for {db_name}")
        
        # Execute tests with progress bar
        print("Executing test cases:")
        results = []
        total_test_count = sum(len(cases) for cases in test_cases_by_db.values())
        
        with tqdm(total=total_test_count, desc="Progress", unit="test") as progress_bar:
            for db_name, db_test_cases in test_cases_by_db.items():
                run_metadata = run_metadata_map[db_name]
                
                for test_case in db_test_cases:
                    status = execute_test_case(session, test_case, run_metadata)
                    results.append(status)
                    progress_bar.update(1)
                    # Optional: Add description of current test
                    progress_bar.set_postfix_str(f"DB: {db_name} - Last: {status}")
        
        # Report summary
        total = len(results)
        passed = results.count("Pass")
        failed = results.count("Fail")
        error = results.count("Error")
        print(f"\nTest execution completed: {passed}/{total} tests passed✅ ({failed} failed❌)({error} error⚠️ ).")
        print("Generated RUN_IDs:")
        for db_name, metadata in run_metadata_map.items():
            print(f"{db_name}: {metadata.run_id}")
        print("Test reports have been saved as PDF files.")
    
    finally:
        # Close session
        session.close()

if __name__ == "__main__":
    main()