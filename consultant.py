import duckdb
import os

# CONFIGURATION
RAW_FILE = r"C:\NQ_Agent\live_nq.txt"
DB_FILE = "trading_research.db"

def build_feature_engine():
    if not os.path.exists(RAW_FILE):
        print(f"❌ Error: {RAW_FILE} not found.")
        return

    con = duckdb.connect(DB_FILE)
    print("--- Starting Feature Factory: Final Success Build ---")

    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE nq_features AS 
            WITH raw_data AS (
                SELECT * FROM read_csv_auto('{RAW_FILE}', normalize_names=True)
            )
            SELECT 
                *,
                -- 1. Create a clean Timestamp
                strptime(date || ' ' || time, '%Y-%m-%d %H:%M:%S') as full_timestamp,
                
                -- 2. Logic using the EXACT names from your diagnostic list
                -- Note: Fixed the < > symbols so _0930open is BETWEEN low and high
                (_0930open > asialow AND _0930open < asiahigh) as open_inside_asia,
                
                (high >= asiahigh) as hit_asia_high,
                (low <= asialow) as hit_asia_low,
                
                -- Using '_last' as confirmed by your engine's diagnostic output
                (_last > vwap) as above_vwap
                
            FROM raw_data;
        """)
        
        row_count = con.execute("SELECT COUNT(*) FROM nq_features").fetchone()[0]
        print(f"✅ SUCCESS: {row_count} rows processed.")
        print("✅ Column Mapping Verified: _last, _0930open, asialow, asiahigh.")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        con.close()

if __name__ == "__main__":
    build_feature_engine()
