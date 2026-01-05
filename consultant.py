import duckdb
import os

# CONFIGURATION
TXT_FILE = r"C:\NQ_Agent\live_nq.txt"
DB_FILE = "trading_research.db"

def sync_data():
    """Moves new bars from the Text file to the SQL Database"""
    print("--- Checking for new market data ---")
    
    # Connect to your new Database
    con = duckdb.connect(DB_FILE)
    
    try:
        # This clever SQL command only inserts rows that DON'T exist in the DB yet
        # It uses 'Date' and 'Time' as the unique fingerprint for each bar
        sync_sql = f"""
            INSERT INTO nq_data 
            SELECT * FROM read_csv_auto('{TXT_FILE}')
            WHERE NOT EXISTS (
                SELECT 1 FROM nq_data 
                WHERE nq_data.Date = read_csv_auto.Date 
                AND nq_data.Time = read_csv_auto.Time
            )
        """
        con.execute(sync_sql)
        print("Sync Complete: Database is now up-to-date with Sierra Chart.")
        
    except Exception as e:
        print(f"Sync Error: {e}")
    finally:
        con.close()

def run_analysis():
    """Run a quick test to see the newest bars in the DB"""
    con = duckdb.connect(DB_FILE)
    df = con.execute("SELECT * FROM nq_data ORDER BY Date DESC, Time DESC LIMIT 5").df()
    print("\n--- Current Market Context (from SQL) ---")
    print(df)
    con.close()

# RUN BOTH
sync_data()
run_analysis()