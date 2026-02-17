# Multi-Source Payer Loader (Snowflake ETL Project)

## 1. Project Overview

This project is a production-style ETL (Extract, Transform, Load) pipeline built using Python and Snowflake.

It loads healthcare claim data into Snowflake based on payer type and supports:

- CSV file input
- Manual data input
- Payer-specific business logic
- Bulk loading using write_pandas()
- Secure credential management using .env
- Modular and clean architecture

---

## 2. Project Architecture

User Input (CLI Arguments)
        ↓
Prepare Data (CSV or Manual)
        ↓
Transform Data (Business Logic)
        ↓
Bulk Load to Snowflake (write_pandas)
        ↓
Snowflake Tables


## 3. Project Structure

AGUMENT_PARSING/
│
├── etl.py
├── snowflake_loader.py
├── anthem_data.csv
├── cigna_data.csv
├── generic_data.csv
├── .env
├── requirements.txt
├── venv/
└── README.md
## 4. Features Implemented

- Argument Parser using argparse
- Function overloading-style input handling
- Data transformation layer
- Payer-specific business rules
- Environment-based configuration
- Bulk data loading with write_pandas()
- Error handling and validation
- Modular code structure


## 5. Environment Configuration
For loading data into snowflake tables
Create a file named `.env` in the project root directory:

SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=PROJECTDB
SNOWFLAKE_SCHEMA=PROJECTSCHEMA

Important:
- Do NOT add quotes
- Do NOT add spaces around =
- Do NOT commit .env to GitHub

---

## 6. Installation Steps

Step 1: Create Virtual Environment

python -m venv venv

Step 2: Activate Environment (Windows)

venv\Scripts\activate

Step 3: Install Dependencies

pip install -r requirements.txt


## 7. How to Run the Project

### Manual Mode

python etl.py --payer manual

### CSV Mode (Anthem)

python etl.py --payer anthem --source anthem_data.csv

### CSV Mode (Cigna)

python etl.py --payer cigna --source cigna_data.csv


## 8. Business Logic

Anthem:
- Adds 5% processing fee to claim_amount

Cigna:
- Applies 2% deduction to claim_amount

Manual:
- No additional transformation

All records receive:
- ingestion_timestamp
- Numeric conversion for claim_amount
- Date conversion for service_date


## 9. Snowflake Loading Strategy

This project uses:

write_pandas()

Internal Process:
1. Converts DataFrame to CSV
2. Uploads file to internal Snowflake stage
3. Executes COPY INTO command
4. Performs fast bulk insert

Advantages:
- Faster than row-by-row insert
- Production-ready


## 10. Data Schema

| Column              | Type            |
|---------------------|----------------|
| member_id           | NUMBER         |
| claim_id            | NUMBER         |
| claim_amount        | NUMBER(38,2)   |
| service_date        | DATE           |
| payer_name          | STRING         |
| ingestion_timestamp | TIMESTAMP      |


## 11. Error Handling Implemented

- Validates required CLI arguments
- Checks CSV file existence
- Converts invalid numeric values to NULL
- Converts invalid date values to NULL
- Uses environment-based secure credentials
