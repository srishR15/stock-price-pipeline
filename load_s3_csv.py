from dotenv import load_dotenv
import os
import pandas as pd
import boto3
import psycopg2
from io import StringIO

S3_BUCKET = "stock-pipeline-data"
REGION = "us-east-2"
OUTPUT_DIR = "processed_data"

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = int(os.getenv("DB_PORT"))

STOCK_SYMBOLS = ["AAPL", "MSFT"]
CRYPTO_SYMBOLS = ["bitcoin", "ethereum"]

s3 = boto3.client("s3", region_name=REGION)


def load_csv_to_postgres(df, table_name, conn):
    """Load pandas DataFrame into PostgreSQL table using COPY FROM"""
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep='')
    buffer.seek(0)
    cursor = conn.cursor()
    try:
        cursor.copy_from(buffer, table_name, sep=",", null="")
        conn.commit()
        print(f"Inserted {len(df)} rows into {table_name}")
    except Exception as e:
        conn.rollback()
        print(f"Error inserting into {table_name}: {e}")
    finally:
        cursor.close()

def fetch_csv_from_s3(s3_key):
    """Fetch CSV from S3 and return pandas DataFrame"""
    obj = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
    return pd.read_csv(obj['Body'])

def main():
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT
    )

    # Load stocks
    # Load stocks
    for symbol in STOCK_SYMBOLS:
        s3_key = f"processed/stocks/{symbol}/{symbol}_processed.csv"
        df = fetch_csv_from_s3(s3_key)
        # ✅ Fix: convert volume to int
        df['volume'] = df['volume'].astype(int)
        load_csv_to_postgres(df, "stocks", conn)

# Load crypto
    for symbol in CRYPTO_SYMBOLS:
        s3_key = f"processed/crypto/{symbol}/{symbol}_processed.csv"
        df = fetch_csv_from_s3(s3_key)
    # ✅ Fix: keep only expected columns, handle NaNs
        df = df[['timestamp', 'price', 'symbol', 'rolling_avg_1h', 'volatility']]
        load_csv_to_postgres(df, "crypto", conn)

    conn.close()
    print("All data loaded successfully!")

if __name__ == "__main__":
    main()