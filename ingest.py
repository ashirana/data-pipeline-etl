import os
import requests
import pandas as pd
import time
from sqlalchemy import create_engine, text

# ---------------- CONFIG ----------------
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

API_URL = "https://jsonplaceholder.typicode.com/comments"
TABLE_NAME = "comment_table"
STAGING_TABLE = f"{TABLE_NAME}_stg"

# ---------------- EXTRACT ----------------
def extract():
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        print("API fetch successful")
        return response.json()
    except Exception as e:
        print("API Error:", e)
        exit()

# ---------------- TRANSFORM ----------------
def transform(data):
    df = pd.DataFrame(data)

    # 🔥 FIX: normalize column names
    df.rename(columns={"postId": "postid"}, inplace=True)

    df["ingestion_time"] = pd.Timestamp.now(tz="UTC")

    print(f"Transformed data: {len(df)} rows")
    print("Columns:", df.columns.tolist())  # debug

    return df
# ---------------- CREATE FINAL TABLE ----------------
def create_table(engine):
    query = text(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            postid INT,
            id INT PRIMARY KEY,
            name TEXT,
            email TEXT,
            body TEXT,
            ingestion_time TIMESTAMP
        );
    """)

    with engine.connect() as conn:
        conn.execute(query)
        conn.commit()

    print("Final table ready")

# ---------------- CREATE STAGING TABLE ----------------
def create_staging_table(engine):
    query = text(f"""
        CREATE TABLE IF NOT EXISTS {STAGING_TABLE} (
            postid INT,
            id INT,
            name TEXT,
            email TEXT,
            body TEXT,
            ingestion_time TIMESTAMP
        );
    """)

    with engine.connect() as conn:
        conn.execute(query)
        conn.commit()

    print("Staging table ready")

# ---------------- LOAD TO STAGING (FAST BULK) ----------------
def load_to_staging(df, engine):
    df.to_sql(STAGING_TABLE, engine, if_exists='replace', index=False)
    print("Bulk loaded into staging table")

# ---------------- MERGE TO FINAL ----------------
def merge_to_final(engine):
    query = text(f"""
        INSERT INTO {TABLE_NAME} (postid, id, name, email, body, ingestion_time)
        SELECT s.postid, s.id, s.name, s.email, s.body, s.ingestion_time
        FROM {STAGING_TABLE} s
        ON CONFLICT (id) DO NOTHING;
    """)

    with engine.connect() as conn:
        conn.execute(query)
        conn.commit()

    print("Merged staging → final table")
# ---------------- DB CONNECTION (WITH RETRY) ----------------
def get_engine():
    for i in range(5):
        try:
            engine = create_engine(DB_URL)
            conn = engine.connect()
            conn.close()
            print("Connected to DB")
            return engine
        except Exception:
            print("DB not ready, retrying...")
            time.sleep(5)

    print("Failed to connect to DB")
    exit()

# ---------------- MAIN ----------------
def main():
    engine = get_engine()

    create_table(engine)
    create_staging_table(engine)

    data = extract()
    df = transform(data)

    load_to_staging(df, engine)
    merge_to_final(engine)

    print("Pipeline completed successfully")

# ---------------- ENTRY POINT ----------------
if __name__ == "__main__":
    main()