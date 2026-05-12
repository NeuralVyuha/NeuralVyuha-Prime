import psycopg2
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("migration")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_DB = os.getenv("POSTGRES_DB", "nv_vault")
POSTGRES_USER = os.getenv("POSTGRES_USER", "nv_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "nv_pass")

def run_migration():
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST, port=POSTGRES_PORT, database=POSTGRES_DB,
            user=POSTGRES_USER, password=POSTGRES_PASSWORD
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            logger.info("Updating correlation_rules table...")
            cur.execute("""
                ALTER TABLE correlation_rules 
                ADD COLUMN IF NOT EXISTS definition_cel TEXT,
                ADD COLUMN IF NOT EXISTS name_template TEXT;
            """)

            logger.info("Updating ingest_correlation_rules table...")
            cur.execute("""
                ALTER TABLE ingest_correlation_rules 
                ADD COLUMN IF NOT EXISTS definition_cel TEXT;
            """)

            logger.info("Creating ingest_extraction_rules table...")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ingest_extraction_rules (
                    id SERIAL PRIMARY KEY,
                    tenant_id VARCHAR(255) NOT NULL,
                    rule_name VARCHAR(255) NOT NULL,
                    attribute VARCHAR(255) NOT NULL,
                    regex TEXT NOT NULL,
                    condition TEXT,
                    priority INTEGER DEFAULT 0,
                    enabled BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            logger.info("Creating ingest_mapping_rules table...")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ingest_mapping_rules (
                    id SERIAL PRIMARY KEY,
                    tenant_id VARCHAR(255) NOT NULL,
                    rule_name VARCHAR(255) NOT NULL,
                    match_fields JSONB NOT NULL, -- list of fields to match against
                    mapping_data JSONB NOT NULL,  -- key-value pairs for enrichment
                    priority INTEGER DEFAULT 0,
                    enabled BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            logger.info("Migration completed successfully.")
        conn.close()
    except Exception as e:
        logger.error(f"Migration failed: {e}")

if __name__ == "__main__":
    run_migration()
