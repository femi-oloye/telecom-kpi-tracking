from snowflake.connector import connect
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

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

    # Execute KPI calculation query
    kpi_query = """
    CREATE OR REPLACE TABLE TELECOMDB.PUBLIC.kpi_data AS
    SELECT
        region,
        SUM(Latency) AS total_Latency,
        COUNT(Latency) AS total_Latency_count,
        SUM(Latency)/COUNT(Latency) AS avg_Latency,
        

        SUM(download_speed) AS total_download_speed,
        COUNT(download_speed) AS total_download_speed_count,
        SUM(download_speed)/COUNT(download_speed) AS avg_download_speed,
        

        SUM(upload_speed) AS total_upload_speed,
        COUNT(upload_speed) AS total_upload_speed_count,
        SUM(upload_speed)/COUNT(upload_speed) AS avg_upload_speed,
        
        SUM(downtime) AS total_downtime,
        COUNT(downtime) AS total_downtime_count,
        SUM(downtime)/COUNT(downtime) *100 AS avg_downtime,

    FROM TELECOMDB.PUBLIC.telecom_kpis
    GROUP BY region;
    """

    cur.execute(kpi_query)
    print("✅ KPI table updated successfully!")

    cur.close()
    conn.close()

except Exception as e:
    print(f"❌ Error updating KPIs: {e}")
