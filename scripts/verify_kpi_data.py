from snowflake.connector import connect
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Connect to Snowflake
try:
    conn = connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )
    
    cur = conn.cursor()
    cur.execute("SELECT * FROM telecomdb.PUBLIC.TELECOM_KPIS LIMIT 10;")
    results = cur.fetchall()
    
    print("✅ Data in Snowflake:")
    for row in results:
        print(row)
    
    cur.close()
    conn.close()

except Exception as e:
    print(f"❌ Error verifying data in Snowflake: {e}")
