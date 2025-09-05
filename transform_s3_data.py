import os
import pandas as pd
import json
import boto3

# -------------------- CONFIG --------------------
TEMP_DIR = "temp_data"
OUTPUT_DIR = "processed_data"
S3_BUCKET = "stock-pipeline-data"  # your bucket name
REGION = "us-east-2"

STOCK_SYMBOLS = ["AAPL", "MSFT"]
CRYPTO_SYMBOLS = ["bitcoin", "ethereum"]

# Initialize S3 client
s3 = boto3.client("s3", region_name=REGION)

# -------------------- FUNCTIONS --------------------

def upload_to_s3(file_path, s3_key):
    s3.upload_file(file_path, S3_BUCKET, s3_key)
    print(f"Uploaded {file_path} to s3://{S3_BUCKET}/{s3_key}")

def transform_stock(symbol, file_path):
    with open(file_path) as f:
        data = json.load(f)
    ts = data["Time Series (5min)"]
    df = pd.DataFrame.from_dict(ts, orient="index")
    df = df.rename(columns={
        "1. open": "open",
        "2. high": "high",
        "3. low": "low",
        "4. close": "close",
        "5. volume": "volume"
    })
    df = df.astype(float)
    df.index.name = "timestamp"
    df.reset_index(inplace=True)
    df["symbol"] = symbol
    df["daily_avg"] = df[["open","high","low","close"]].mean(axis=1)
    df["volatility"] = (df["high"] - df["low"]) / df["open"]
    
    output_file = os.path.join(OUTPUT_DIR, f"{symbol}_processed.csv")
    df.to_csv(output_file, index=False)
    
    s3_key = f"processed/stocks/{symbol}/{symbol}_processed.csv"
    upload_to_s3(output_file, s3_key)
    
    return df

def transform_crypto(symbol, file_path):
    with open(file_path) as f:
        data = json.load(f)
    prices = data["prices"]
    df = pd.DataFrame(prices, columns=["timestamp_ms", "price"])
    df["timestamp"] = pd.to_datetime(df["timestamp_ms"], unit="ms")
    df["symbol"] = symbol
    df["price"] = df["price"].astype(float)
    df["rolling_avg_1h"] = df["price"].rolling(window=12).mean()  # 5-min intervals → 12 = 1 hour
    df["volatility"] = df["price"].pct_change()
    
    output_file = os.path.join(OUTPUT_DIR, f"{symbol}_processed.csv")
    df.to_csv(output_file, index=False)
    
    s3_key = f"processed/crypto/{symbol}/{symbol}_processed.csv"
    upload_to_s3(output_file, s3_key)
    
    return df

# -------------------- MAIN LOOP --------------------

os.makedirs(OUTPUT_DIR, exist_ok=True)

for root, _, files in os.walk(TEMP_DIR):
    for file in files:
        file_path = os.path.join(root, file)
        symbol = file.split("_")[0].lower()
        
        if symbol in [s.lower() for s in STOCK_SYMBOLS]:
            transform_stock(symbol.upper(), file_path)
            print(f"Processed stock: {symbol.upper()}")
        elif symbol in [c.lower() for c in CRYPTO_SYMBOLS]:
            transform_crypto(symbol.lower(), file_path)
            print(f"Processed crypto: {symbol.lower()}")
