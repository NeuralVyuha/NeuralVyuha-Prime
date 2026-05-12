ALTER TABLE cases ADD COLUMN IF NOT EXISTS custom_fields JSONB DEFAULT '{}'::jsonb;
