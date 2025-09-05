import pandas as pd
from sqlalchemy import create_engine
import os

# PostgreSQL connection
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "finance_data")

engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}")

def load_stock_to_pg(file_path):
    df = pd.read_csv(file_path)
    df.to_sql('stocks', engine, if_exists='append', index=False)
    print(f"Loaded {file_path} into PostgreSQL stocks table")

def load_crypto_to_pg(file_path):
    df = pd.read_csv(file_path)
    df.to_sql('crypto', engine, if_exists='append', index=False)
    print(f"Loaded {file_path} into PostgreSQL crypto table")