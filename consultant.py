import duckdb
import os

# 1. Path to your file
CSV_FILE = r"C:\NQ_Agent\live_nq.txt" 

def test_connection():
    print(f"--- Debugging ---")
    
    # Check if file exists
    if not os.path.exists(CSV_FILE):
        print(f"ERROR: File not found at {CSV_FILE}")
        return

    print("SUCCESS: File found!")

    # 2. Your SQL Query - Written clearly
    # We use f-string (the 'f' before the quotes) to insert the filename
    sql = f"""
        SELECT * FROM read_csv_auto('{CSV_FILE}') 
        ORDER BY 1 DESC, 2 DESC
        LIMIT 12
    """

    try:
        # 3. Execute the query
        data = duckdb.query(sql).df()
        print("--- Latest 10 Bars (Newest First) ---")
        print(data)
        
    except Exception as e:
        print(f"SQL ERROR: Something is wrong with the query logic.")
        print(f"Details: {e}")

# Run the function
test_connection()