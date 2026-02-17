import argparse
import pandas as pd
from datetime import datetime
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables once
load_dotenv(dotenv_path=Path(__file__).parent / ".env")

from snowflake_loader import load_to_snowflake


# 1️ Prepare Data (Overloading Style)


def prepare_dataframe(data):

    if isinstance(data, str):
        print("Reading data from CSV file...")
        return pd.read_csv(data)

    elif isinstance(data, list):
        print("Creating DataFrame from manual input...")
        return pd.DataFrame(data)

    else:
        raise ValueError("Unsupported input type")



# 2️ Data Transformation Logic


def transform_data(df, payer):

    df = df.copy()

    # Ensure numeric conversion
    df["claim_amount"] = pd.to_numeric(df["claim_amount"], errors="coerce")

    # Convert date properly
    df["service_date"] = pd.to_datetime(
        df["service_date"], errors="coerce"
    ).dt.date

    # Add ingestion timestamp
    df["ingestion_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    payer = payer.lower()

    if payer == "anthem":
        df["claim_amount"] = df["claim_amount"] * 1.05

    elif payer == "cigna":
        df["claim_amount"] = df["claim_amount"] * 0.98

    return df



# 3️ Argument Parser


def parse_arguments():

    parser = argparse.ArgumentParser(
        description="Multi-Source Payer Loader"
    )

    parser.add_argument(
        "--source",
        type=str,
        help="Path to CSV file"
    )

    parser.add_argument(
        "--payer",
        required=True,
        choices=["anthem", "cigna", "manual"],
        help="Payer name"
    )

    return parser.parse_args()


# 4️ Main Execution


def main():

    args = parse_arguments()

    # -------- Manual Input --------
    if args.payer == "manual":

        manual_data = [
            {
                "member_id": 1,
                "claim_id": 101,
                "claim_amount": 500,
                "service_date": "2025-01-01",
                "payer_name": "manual"
            },
            {
                "member_id": 2,
                "claim_id": 102,
                "claim_amount": 1500,
                "service_date": "2025-01-02",
                "payer_name": "manual"
            }
        ]

        df = prepare_dataframe(manual_data)

    # -------- File Input --------
    else:
        if not args.source:
            raise ValueError("Provide --source for file input")

        if not os.path.exists(args.source):
            raise FileNotFoundError(f"File not found: {args.source}")

        df = prepare_dataframe(args.source)

    # Transform data
    df = transform_data(df, args.payer)

    # Show preview
    print("\nData Preview:")
    print(df.head())

    # Load into Snowflake
    load_to_snowflake(df, args.payer)



# Entry Point

if __name__ == "__main__":
    main()