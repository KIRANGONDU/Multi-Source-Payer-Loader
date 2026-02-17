import os
import snowflake.connector
from dotenv import load_dotenv

from snowflake.connector.pandas_tools import write_pandas


def load_to_snowflake(df, payer):

    # Decide table name
    if payer.lower() == "anthem":
        table = "ANTHEM_TABLE"
    elif payer.lower() == "cigna":
        table = "CIGNA_TABLE"
    else:
        table = "GENERIC_CLAIMS"

    # Uppercase columns for Snowflake
    df.columns = df.columns.str.upper()

    # Connect using environment variables
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

    print("Connected to Snowflake")

    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name=table
    )

    if success:
        print(f"Loaded {nrows} rows into {table}")
    else:
        print("Load failed")

    conn.close()