import os
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get credentials from .env file
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

# Debugging: Print values to check
print("🔹 SNOWFLAKE_USER:", SNOWFLAKE_USER)
print("🔹 SNOWFLAKE_ACCOUNT:", SNOWFLAKE_ACCOUNT)
print("🔹 SNOWFLAKE_DATABASE:", SNOWFLAKE_DATABASE)
print("🔹 SNOWFLAKE_SCHEMA:", SNOWFLAKE_SCHEMA)

# Ensure all values are present
if None in [SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA]:
    print("❌ ERROR: One or more environment variables are missing!")
    exit(1)

# Establish Snowflake connection
try:
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE
    )
    print("✅ Successfully connected to Snowflake!")
except Exception as e:
    print(f"❌ Error connecting to Snowflake: {e}")
