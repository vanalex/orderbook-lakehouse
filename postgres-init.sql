-- Run by the postgres container on first init (empty data volume) only.
-- Polaris doesn't issue CREATE SCHEMA itself; the admin tool's bootstrap
-- then creates its tables inside this schema.
CREATE SCHEMA IF NOT EXISTS polaris_schema;
