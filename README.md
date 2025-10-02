# rag — Retrieval-Augmented Generation pipeline CLI

A fast, end‑to‑end RAG pipeline in Rust for:

- Managing RSS feeds and ingesting articles
- Cleaning and chunking text with an E5 tokenizer
- Embedding chunks with ONNX (CPU or CUDA)
- Querying with pgvector (ivfflat + cosine distance)
- Operational stats, reindexing, and garbage collection

The CLI is designed for both human‑friendly logs and machine‑readable JSON envelopes for automation.

## Prerequisites

- Rust toolchain (latest stable)
- PostgreSQL 14+ with the pgvector extension installed
- Network access for downloading models/tokenizers from Hugging Face (first run)
- A provisioned database and `DATABASE_URL` pointing to it

Database notes:
- Ensure `pgvector` is installed in your database: `CREATE EXTENSION IF NOT EXISTS vector;`
- This project uses `rag` schema. Create it once if not present: `CREATE SCHEMA IF NOT EXISTS rag;`

Build notes (sqlx):
- The code uses `sqlx::query!()` macros. At build time, either:
  - Provide a reachable `DATABASE_URL` so sqlx can validate queries, or
  - Use sqlx offline mode with a generated `sqlx-data.json` (not included in repo).

## Installation

- CPU build:
  ```bash
  cargo build --release
  ```
- CUDA build (optional):
  ```bash
  cargo build --release --features cuda
  ```
  Requires a compatible CUDA setup. The binary includes ONNX Runtime via the `ort` crate.

## Task Runner (just)

Use `just` to run common tasks (loads variables from `.env`).

- Install: `cargo install just` (or use your package manager)
- List tasks: `just` (or `just help`)
- Start DB: `just db-up`
- Tail logs: `just db-logs`
- Open psql: `just psql`
- Stop DB: `just db-down`

Migrations with sqlx-cli (install once: `cargo install sqlx-cli`)
- Run migrations: `just migrate`
- Show status: `just migrate-info`
- Revert last: `just migrate-revert`
- Reset DB (wipe volume): `just db-reset`

## Configuration

- `DATABASE_URL` — Postgres DSN (e.g., `postgres://user:pass@host:5432/db`)
- `RUST_LOG` — e.g., `info`, `debug`, `rag=debug,sqlx=warn`
- `RAG_LOG_FORMAT` — `json` for structured logs to stderr; default is compact text
- `HF_HOME` — optional, Hugging Face cache directory

Every command also accepts `--dsn` to override `DATABASE_URL`.

JSON mode:
- Pass global `--json` to emit a single JSON envelope to stdout (plan/result). Logs go to stderr.

## Quickstart

1) Initialize schema and indexes
```bash
# Start the database
just db-up

# Run migrations with sqlx-cli (install once: `cargo install sqlx-cli`)
just migrate
```
Notes:
- The container's init script creates the `vector` extension and `rag` schema on first start.
- If targeting an external DB, ensure once:
  - `CREATE EXTENSION IF NOT EXISTS vector;`
  - `CREATE SCHEMA IF NOT EXISTS rag;`

2) Add a feed (plan first, then apply)
```bash
rag feed add https://example.com/rss.xml
rag feed add https://example.com/rss.xml --name "Example" --apply
rag feed ls
```

3) Ingest articles
```bash
# Ingest all active feeds (insert-only)
rag ingest --apply

# Force re-fetch + upsert for a specific feed
rag ingest --feed 1 --force-refetch --apply
```

4) Chunk documents
```bash
# Chunk newly ingested content with 350-token windows and 80-token overlap
rag chunk --since 2d --apply
```

5) Embed chunks
```bash
# Embeds missing chunks using E5 small (384-dim), CPU
rag embed --apply --model-id intfloat/e5-small-v2 --dim 384

# Re-embed everything (dangerous if you care about consistency)
rag embed --force --apply --model-id intfloat/e5-small-v2 --dim 384
```

6) Query
```bash
rag query "how to deploy on k8s" --show-context

# Filters
rag query "rust tokio" --feed 1 --since 2025-01-01
```

7) Operational views
```bash
# Overview
rag stats

# Per-feed
rag stats --feed 1

# Snapshots
rag stats --doc 123
rag stats --chunk 456
```

8) Maintenance
```bash
# Reindex ivfflat; choose lists via heuristic (or override with --lists)
rag reindex --apply

# Garbage collection (plan-only by default)
rag gc --older-than 30d
rag gc --older-than 30d --apply

# Useful flags
rag gc --feed 1 --fix-status --vacuum analyze --apply
```

## Command Reference

- `rag feed add <url> [--name <str>] [--active <bool>] [--apply]` — upsert a feed
- `rag feed ls [--active-only]` — list feeds
- `rag ingest [--feed <id>] [--feed-url <url>] [--limit <n>] [--force-refetch] [--apply]` — fetch RSS, pull pages, extract text, write `rag.document`
- `rag chunk [--since <win|date>] [--doc-id <id>] [--tokens-target <n>] [--overlap <n>] [--max-chunks-per-doc <n>] [--force] [--apply]` — produce `rag.chunk`
- `rag embed [--model-id <id>] [--onnx-filename <path>] [--device cpu|cuda] [--dim <n>] [--batch <n>] [--max <n>] [--force] [--apply]` — write `rag.embedding`
- `rag query <text> [--top-n <n>] [--topk <n>] [--doc-cap <n>] [--probes <k>] [--feed <id>] [--since <date|win>] [--show-context]` — ANN over embeddings
- `rag stats [--feed <id>] [--doc <id>] [--chunk <id>]` — operational views
- `rag reindex [--lists <k>] [--apply]` — create/reindex/swap ivfflat index
- `rag gc [--older-than <win|date>] [--feed <id>] [--max <n>] [--vacuum analyze|full|off] [--fix-status] [--drop-temp-indexes] [--apply]` — cleanup

Migrations
- Use `just migrate` (with `sqlx-cli`) for database migrations. See the “Task Runner (just)” section.

## Data Flow & Cascades (Summary)

- Foreign keys and cascades
  - `rag.chunk.doc_id → rag.document.doc_id ON DELETE CASCADE`
  - `rag.embedding.chunk_id → rag.chunk.chunk_id ON DELETE CASCADE`
  - Deleting a document deletes its chunks, which deletes their embeddings. Deleting a chunk deletes its embedding.

- Ingestion (documents)
  - Insert‑only: ignores conflicts by `source_url` (no updates).
  - Upsert: on conflict by `source_url`, updates title/published_at/fetched_at/content_hash/raw_html/text_clean/status/error_msg; does not touch chunks/embeddings.

- Chunking (replace strategy)
  - For each doc: delete all existing chunks, then insert new chunks `(doc_id, chunk_index)`; set document `status='chunked'`.
  - Deleting chunks cascades to delete their embeddings.
  - Edge case: if tokenization yields zero tokens, status is set to `chunked` without deleting existing chunks.

- Embeddings (single row per chunk)
  - Schema has one embedding per chunk (PK on `chunk_id`).
  - Upsert overwrites existing row (model, dim, vec). Running a different model later replaces the previous one.
  - Planning can list “missing by model”, but writes still keep only one row per chunk.

- GC (cleanup)
  - Deletes orphan embeddings (no chunk), orphan chunks (no document), stale error docs, never‑chunked old docs, and bad chunks. Cascades ensure child rows are removed.

- Reindex/Query
  - Reindex modifies ivfflat index only. Query is read‑only.

Practical implications
- Re‑chunking invalidates embeddings for a document via cascade; re‑run `rag embed` after chunking.
- Alternating between models in `rag embed` will overwrite vectors due to single‑row design per chunk.
- For multi‑model support, change embeddings to PK `(chunk_id, model)` and update queries accordingly.

## Embedding Models

- Default: `intfloat/e5-small-v2` (384‑dim). The encoder downloads tokenizer and ONNX model from the Hugging Face Hub.
- You can override `--onnx-filename` to point to a specific ONNX file inside the model repo.
- Device: `--device cpu` (default) or `--device cuda` (requires CUDA build and system drivers).

## Telemetry & JSON

- Human logs are compact text. Set `RAG_LOG_FORMAT=json` to emit JSON logs to stderr.
- Add global `--json` to get a single machine‑readable plan/result envelope on stdout; ideal for scripting.

## Troubleshooting

- Build fails with sqlx macro errors:
  - SQLx validates queries at compile time. Bootstrap the schema first with `just migrate` so tables exist, or use SQLX offline with a generated `sqlx-data.json`.
  - To prepare offline data: `cargo install sqlx-cli && DATABASE_URL=... cargo sqlx prepare`.
- `No embeddings found. Run rag embed first.`
  - Create embeddings after chunking: `rag embed --apply`.
- Embedding dim mismatch (e.g., model produced 768 but `--dim 384`):
  - Set `--dim` to the actual model output or use the matching model.
- ONNX/CUDA issues:
  - Rebuild with `--features cuda` and verify CUDA toolchain/driver versions.
- Hugging Face download failures:
  - Ensure network access and optional `HF_HOME` cache path is writable.

## Notes

- The generic extractor uses simple CSS selectors with a paragraph fallback; site‑specific extractors can be added under `src/ingestion/extractor/`.
- Be mindful of target site policies; add delays or caching as needed for respectful ingestion.

---

Happy hacking! If you want, I can add an example `.env` and a minimal docker‑compose for Postgres + pgvector.
