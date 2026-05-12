-- Migration: 003_add_input_hash_to_investigations.sql

ALTER TABLE ai_case_investigations 
ADD COLUMN IF NOT EXISTS input_hash VARCHAR(64);

CREATE INDEX IF NOT EXISTS idx_ai_investigations_input_hash ON ai_case_investigations (input_hash);
