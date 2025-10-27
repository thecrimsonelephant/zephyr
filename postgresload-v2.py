import pandas as pd
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import Column, String, Integer, Float, DateTime
from sqlalchemy import text
import os

from dotenv import load_dotenv
from ingestion import main

pg_password = os.getenv("PGPWD")

# Load CSV
df = main()
# df = pd.read_csv('data/cleaned.csv')
print(df.head())

# Engine
engine = create_engine(f'postgresql+psycopg2://postgres:{pg_password}@localhost:5432/zephyr', future=True)

table_name = "daily_weather"
metadata = MetaData()

# Drop table safely in 2.x
with engine.begin() as conn:
    conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))

# Define table schema from DataFrame dtypes
cols = []
for col_name, dtype in zip(df.columns, df.dtypes):
    if pd.api.types.is_integer_dtype(dtype):
        cols.append(Column(col_name, Integer))
    elif pd.api.types.is_float_dtype(dtype):
        cols.append(Column(col_name, Float))
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        cols.append(Column(col_name, DateTime))
    else:
        cols.append(Column(col_name, String))

table = Table(table_name, metadata, *cols)

# Create table
metadata.create_all(engine)

# Insert all rows safely
with engine.begin() as conn:
    conn.execute(insert(table), df.to_dict(orient="records"))

# Verify
with engine.connect() as conn:
    result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
    print(f"Rows uploaded: {result.scalar()}")
