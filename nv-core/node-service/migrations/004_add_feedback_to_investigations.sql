-- Migration 004: Add explicit feedback columns to AI case investigations

ALTER TABLE ai_case_investigations
ADD COLUMN rating SMALLINT DEFAULT NULL CHECK (rating IN (1, -1, NULL)),
ADD COLUMN feedback_comment TEXT DEFAULT NULL;
