import os
import psycopg2
import json

def get_db_conn():
    return psycopg2.connect(
        host="localhost",
        database="nv_vault",
        user="hive",
        password="hive"
    )

try:
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'ai_case_investigations' AND table_schema = 'public';")
    columns = cur.fetchall()
    print(json.dumps(columns, indent=2))
    conn.close()
except Exception as e:
    print(f"Error: {e}")
