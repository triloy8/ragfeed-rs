-- Feeds
CREATE TABLE IF NOT EXISTS rag.feed (
  feed_id    SERIAL PRIMARY KEY,
  url        TEXT UNIQUE NOT NULL,
  name       TEXT,
  added_at   TIMESTAMPTZ DEFAULT now(),
  is_active  BOOLEAN DEFAULT TRUE
);

-- Documents
CREATE TABLE IF NOT EXISTS rag.document (
  doc_id        BIGSERIAL PRIMARY KEY,
  feed_id       INTEGER REFERENCES rag.feed(feed_id),
  source_url    TEXT UNIQUE NOT NULL,
  source_title  TEXT,
  published_at  TIMESTAMPTZ,
  fetched_at    TIMESTAMPTZ,
  etag          TEXT,
  last_modified TEXT,
  content_hash  TEXT,
  raw_html      BYTEA,
  text_clean    TEXT,
  status        TEXT,   -- ingest|chunked|embedded|error
  error_msg     TEXT
);
CREATE INDEX IF NOT EXISTS document_pub_idx  ON rag.document (published_at DESC);
CREATE INDEX IF NOT EXISTS document_feed_idx ON rag.document (feed_id);

-- Chunks
CREATE TABLE IF NOT EXISTS rag.chunk (
  chunk_id     BIGSERIAL PRIMARY KEY,
  doc_id       BIGINT REFERENCES rag.document(doc_id) ON DELETE CASCADE,
  chunk_index  INTEGER,
  text         TEXT NOT NULL,
  token_count  INTEGER,
  md5          TEXT,
  heading_path TEXT,
  fts tsvector GENERATED ALWAYS AS (to_tsvector('english', coalesce(text,''))) STORED,
  UNIQUE(doc_id, chunk_index)
);
CREATE INDEX IF NOT EXISTS chunk_doc_idx ON rag.chunk(doc_id);
CREATE INDEX IF NOT EXISTS chunk_fts_idx ON rag.chunk USING GIN (fts);

-- Embeddings (assumes pgvector installed; you asked to omit extensions)
CREATE TABLE IF NOT EXISTS rag.embedding (
  chunk_id    BIGINT PRIMARY KEY REFERENCES rag.chunk(chunk_id) ON DELETE CASCADE,
  model       TEXT NOT NULL,
  dim         INTEGER NOT NULL,
  vec         vector(384) NOT NULL,
  created_at  TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS embedding_vec_ivf_idx
  ON rag.embedding USING ivfflat (vec vector_cosine_ops) WITH (lists = 150);

-- Pipeline Runs
CREATE TABLE IF NOT EXISTS rag.run (
  run_id      BIGSERIAL PRIMARY KEY,
  started_at  TIMESTAMPTZ DEFAULT now(),
  finished_at TIMESTAMPTZ,
  op          TEXT,     -- fetch|extract|chunk|embed|reindex|compose|gc|eval
  status      TEXT,     -- ok|error
  details     JSONB
);

-- Optional: evaluation & query log
CREATE TABLE IF NOT EXISTS rag.eval_set (
  eval_id     BIGSERIAL PRIMARY KEY,
  query       TEXT NOT NULL,
  expected    TEXT NOT NULL,
  notes       TEXT
);

CREATE TABLE IF NOT EXISTS rag.query_log (
  log_id      BIGSERIAL PRIMARY KEY,
  query       TEXT NOT NULL,
  retrieved_chunks JSONB,
  created_at  TIMESTAMPTZ DEFAULT now()
);
