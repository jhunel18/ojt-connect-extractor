import psycopg2
from psycopg2 import OperationalError
import os
from dotenv import load_dotenv
load_dotenv()
try:
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),    
        database=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        sslmode = os.getenv("PG_SSLMODE")  # Secure SSL connection
    )
    print("Connected successfully!")
    conn.close()
except OperationalError as e:
    print("Connection failed:", e)
