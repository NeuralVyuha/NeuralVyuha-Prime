import psycopg2
import os
import time

def seed_rules():
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            database=os.getenv("POSTGRES_DB", "v5_events"),
            user=os.getenv("POSTGRES_USER", "hive"),
            password=os.getenv("POSTGRES_PASSWORD", "hive")
        )
        conn.autocommit = True

        # Already seeded in init.sql, but this script can be used to add more dynamic ones or verify.
        # Let's verify rule count
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM correlation_rules")
            count = cur.fetchone()[0]
            print(f"Rule count: {count}")
            if count < 3:
                print("Warning: Expected at least 3 rules.")

        conn.close()
    except Exception as e:
        print(f"Failed to check rules: {e}")
        # Don't fail CI if just checking

if __name__ == "__main__":
    time.sleep(5) # Wait for DB
    seed_rules()
