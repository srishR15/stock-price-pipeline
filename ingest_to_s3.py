import boto3
import requests
import json
import os
from datetime import datetime
from dotenv import load_dotenv

# -------------------- CONFIG --------------------
S3_BUCKET = "stock-pipeline-data"  # your bucket name
STOCK_API_KEY = os.getenv("STOCK_API_KEY")
STOCK_SYMBOLS = ["AAPL", "MSFT"]
CRYPTO_SYMBOLS = ["bitcoin", "ethereum"]
REGION = "us-east-2"

# Local folder to store temporary files
LOCAL_DIR = "temp_data"
os.makedirs(LOCAL_DIR, exist_ok=True)

# Initialize S3 client
s3 = boto3.client("s3", region_name=REGION)

# -------------------- FUNCTIONS --------------------
def fetch_stock_data(symbol):
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=5min&apikey={STOCK_API_KEY}"
    response = requests.get(url)
    return response.json()

def fetch_crypto_data(symbol):
    url = f"https://api.coingecko.com/api/v3/coins/{symbol}/market_chart?vs_currency=usd&days=1"
    response = requests.get(url)
    return response.json()

def upload_to_s3(file_path, s3_key):
    s3.upload_file(file_path, S3_BUCKET, s3_key)
    print(f"Uploaded {file_path} to s3://{S3_BUCKET}/{s3_key}")

def archive_old_files(symbol, category="stocks", max_archives=10):
    """Move existing files in current folder to archive, and enforce max archive files"""
    prefix_current = f"{category}/{symbol}/"
    prefix_archive = f"archived/{category}/{symbol}/"

    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix_current)
    if 'Contents' not in response:
        return  # No files to archive

    current_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith(".json")]

    for key in current_files:
        archive_key = prefix_archive + key.split('/')[-1]  # keep original timestamped name
        s3.copy_object(Bucket=S3_BUCKET, CopySource={'Bucket': S3_BUCKET, 'Key': key}, Key=archive_key)
        s3.delete_object(Bucket=S3_BUCKET, Key=key)
        print(f"Archived {key} to {archive_key}")

    # Keep only latest max_archives in archive
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix_archive)
    if 'Contents' in response:
        archive_files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
        for obj in archive_files[max_archives:]:
            s3.delete_object(Bucket=S3_BUCKET, Key=obj['Key'])
            print(f"Deleted old archive {obj['Key']} to maintain max {max_archives} files")

# -------------------- MAIN PIPELINE --------------------
def main():

    for f in os.listdir(LOCAL_DIR):
        file_path = os.path.join(LOCAL_DIR, f)
        if os.path.isfile(file_path):
            os.remove(file_path)
    print(f"Cleared {LOCAL_DIR} before new extraction")

    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")

    # Archive old stock files before uploading new
    for symbol in STOCK_SYMBOLS:
        archive_old_files(symbol, category="stocks")

    # Archive old crypto files before uploading new
    for symbol in CRYPTO_SYMBOLS:
        archive_old_files(symbol, category="crypto")

    # 1️⃣ Stock data ingestion
    for symbol in STOCK_SYMBOLS:
        data = fetch_stock_data(symbol)
        local_file = os.path.join(LOCAL_DIR, f"{symbol}_{timestamp}.json")
        with open(local_file, "w") as f:
            json.dump(data, f, indent=2)
        s3_key = f"stocks/{symbol}/{symbol}_{timestamp}.json"
        upload_to_s3(local_file, s3_key)

    # 2️⃣ Crypto data ingestion
    for symbol in CRYPTO_SYMBOLS:
        data = fetch_crypto_data(symbol)
        local_file = os.path.join(LOCAL_DIR, f"{symbol}_{timestamp}.json")
        with open(local_file, "w") as f:
            json.dump(data, f, indent=2)
        s3_key = f"crypto/{symbol}/{symbol}_{timestamp}.json"
        upload_to_s3(local_file, s3_key)

    print("Data ingestion with archiving completed successfully!")

if __name__ == "__main__":
    main()
