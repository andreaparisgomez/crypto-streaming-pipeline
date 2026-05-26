import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("WAREHOUSE_HOST"),
    dbname=os.getenv("WAREHOUSE_DB"),
    user=os.getenv("WAREHOUSE_USER"),
    password=os.getenv("WAREHOUSE_PASSWORD"),
    sslmode=os.getenv("WAREHOUSE_SSLMODE")
)

print("Connected successfully to Neon!")

conn.close()
