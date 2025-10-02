-- One-time initialization executed by the Postgres image on first startup
-- Ensures pgvector is available and the 'rag' schema exists.

CREATE EXTENSION IF NOT EXISTS vector;
CREATE SCHEMA IF NOT EXISTS rag;