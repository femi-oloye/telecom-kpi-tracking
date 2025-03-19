import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv

# Load environment variables (Snowflake credentials)
load_dotenv()

# Snowflake connection details from .env
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_DATABASE = "TELECOMDB"
SNOWFLAKE_SCHEMA = "PUBLIC"
SNOWFLAKE_WAREHOUSE = "FMDEV_WAREHOUSE"

# Establish connection
def connect_to_snowflake():
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        print("✅ Connected to Snowflake successfully!")
        return conn
    except Exception as e:
        print(f"❌ Error connecting to Snowflake: {e}")
        return None

# Load CSV data and insert into Snowflake
def load_data_to_snowflake():
    # Read CSV file
    df = pd.read_csv("/home/oluwafemi/mlop-data/telecom-kpi-tracking/telecom-kpi-tracking/data/telecom_kpis.csv")
    
    # Connect to Snowflake
    conn = connect_to_snowflake()
    if conn is None:
        return

    cur = conn.cursor()

    # Insert data into Snowflake
    for _, row in df.iterrows():
        sql = f"""
        INSERT INTO telecomdb.PUBLIC.TELECOM_KPIS (date, region, network_type, latency, download_speed, upload_speed, downtime, users_affected)
        VALUES ('{row["date"]}', '{row["region"]}', '{row["network_type"]}', {row["latency"]}, {row["download_speed"]}, {row["upload_speed"]}, {row["downtime"]}, {row["users_affected"]});
        """
        cur.execute(sql)
    
    print("✅ Data successfully loaded into Snowflake!")
    cur.close()
    conn.close()

# Run the script
if __name__ == "__main__":
    load_data_to_snowflake()
