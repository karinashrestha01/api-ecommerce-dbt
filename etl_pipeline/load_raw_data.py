import json
import pandas as pd
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from dotenv import load_dotenv
import io

load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

if not all([DB_USER, DB_PASSWORD, DB_NAME]):
    raise EnvironmentError("Missing one or more database environment variables.")

def _build_connection_url() -> str:
    return f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(_build_connection_url(), echo=False, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)

def get_engine():
    """Get the database engine instance."""
    return engine

# 2. Configuration
SCHEMA_NAME = 'raw'

# PATH LOGIC: 
# Get the directory where this script is located (e.g., /project/etl)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Go up one level to root, then down into data/raw
DATA_DIR = os.path.join(SCRIPT_DIR, '..', 'data', 'raw')

# Normalize the path to remove the '..'
DATA_DIR = os.path.normpath(DATA_DIR)

print(f"Looking for data in: {DATA_DIR}")

# MAPPING CONFIGURATION
files_to_load = {
    'customers.json': 'customers',
    'orders.json': 'orders',
    'products.json': 'products',
    'sellers.json': 'sellers',
    'orders_items.json': 'order_items',       
    'orders_payments.json': 'order_payments', 
    'orders_reviews.json': 'order_reviews'    
}

def create_schema_if_not_exists(engine, schema):
    """Creates the schema in Postgres if it doesn't exist."""
    with engine.connect() as connection:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
        connection.commit()


def clean_text_columns(df):
    """Clean newlines/tabs from string columns to prevent COPY errors"""
    object_cols = df.select_dtypes(include=['object']).columns
    for col in object_cols:
        df[col] = df[col].astype(str).str.replace(r'[\n\r\t]', ' ', regex=True)
    return df

def fast_load_to_postgres(df, table_name, engine, schema='raw'):
    df = clean_text_columns(df)
    
    # Create Table
    df.head(0).to_sql(table_name, engine, schema=schema, if_exists='replace', index=False)
    
    # Stream Data
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False, quotechar='"')
    output.seek(0)
    
    try:
        cur.copy_expert(
            f"COPY {schema}.{table_name} FROM STDIN WITH (FORMAT CSV, DELIMITER '\t', NULL '', QUOTE '\"')", 
            output
        )
        conn.commit()
        print(f"   -> Loaded {len(df)} rows into {schema}.{table_name}")
    except Exception as e:
        conn.rollback()
        print(f"   -> Error loading {table_name}: {e}")
    finally:
        cur.close()
        conn.close()

def main():
    print("Starting ELT Load Process...")
    
    # Step 1: Ensure Schema Exists
    create_schema_if_not_exists(engine, SCHEMA_NAME)
    
    # Step 2: Loop through files
    for filename, table_name in files_to_load.items():
        file_path = os.path.join(DATA_DIR, filename)
        
        if os.path.exists(file_path):
            try:
                print(f"Processing {filename}...")
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = json.load(f)
                
                # Handle "data": [...] wrapper versus direct [...] list
                if isinstance(content, dict) and 'data' in content:
                    records = content['data']
                else:
                    records = content

                if not records:
                    print(f"{filename} is empty, skipping.")
                    continue

                # Normalize JSON to Flat Table
                df = pd.json_normalize(records)
                
                # Perform the load
                fast_load_to_postgres(df, table_name, engine, SCHEMA_NAME)
                
            except Exception as e:
                print(f"Failed to process {filename}: {e}")
        else:
            print(f"File not found: {filename}")

    print("Process Complete.")

if __name__ == "__main__":
    main()