import duckdb

#This script is for manual verification of the RTH open inside Asia range 
# feature. Follow this logic when the engine has been built and you want to
# verify more results.
# Connect to the engine's database 
con = duckdb.connect("trading_research.db")

print("--- RTH OPEN INSIDE ASIA RANGE: MANUAL VERIFICATION ---")

try:
    # We select the specific columns for the 09:30 open bar
    # This allows you to verify if 1/0 is printing correctly
    query = """
        SELECT 
            date, 
            time, 
            asialow, 
            _0930open, 
            asiahigh,
            open_inside_asia AS RTH_open_inside_Asia_Range
        FROM nq_features 
        WHERE time = '09:30:00'
        ORDER BY date DESC
    """
    
    df = con.execute(query).df()
    
    if df.empty:
        print("❌ No 09:30:00 rows found. Check if your Sierra Chart session starts at a different time.")
    else:
        print(df.to_string(index=False))

except Exception as e:
    print(f"❌ Error: {e}")
finally:
    con.close()
