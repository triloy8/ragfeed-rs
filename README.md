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
- Docker with the Compose plugin (runs the bundled Postgres 16 + pgvector service)
- Network access for downloading models/tokenizers from Hugging Face (first run)
- Optional: your own PostgreSQL 14+ with pgvector if you skip the bundled container
- Feed format: only RSS 2.0 feeds are supported

Database notes:
- `just db-up` starts the `docker-compose.yml` service (`pgvector/pgvector:pg16`) and runs `docker/db/init.sql`, which installs `pgvector` and creates the `rag` schema on first boot.
- Targeting an external database instead? Ensure once:
  - `CREATE EXTENSION IF NOT EXISTS vector;`
  - `CREATE SCHEMA IF NOT EXISTS rag;`

Build notes (sqlx):
- The code uses `sqlx::query!()` macros. At build time, either:
  - When using the bundled Docker DB, run `just db-up` so the `.env` default (`postgres://rag:rag@localhost:5433/rag`) is reachable.
  - Provide a reachable `DATABASE_URL` so sqlx can validate queries, or
  - Use sqlx offline mode with a generated `sqlx-data.json` (not included in repo).
  - Online build note: ensure the target database is alive and migrated before building, as sqlx validates against the live schema

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

- `DATABASE_URL` — Postgres DSN. Default `.env` points to the Docker DB (`postgres://rag:rag@localhost:5433/rag`).
- `RUST_LOG` — e.g., `info`, `debug`, `rag=debug,sqlx=warn`
- `RAG_LOG_FORMAT` — `json` for structured logs to stderr; default is compact text
- `RAG_OUTPUT_FORMAT` — `text|json|mcp` for outputs to stdout; default `text`
- `RAG_OUTPUT_PRETTY` — `true|false` pretty-prints outputs; default `false`
- `NO_COLOR` — set to disable ANSI colors in text output
- `HF_HOME` — optional, Hugging Face cache directory

Every command also accepts `--dsn` to override `DATABASE_URL`.

Outputs vs Logs
- Outputs (Plan/Result) go to stdout in the selected format (`RAG_OUTPUT_FORMAT`).
- Logs (operational) go to stderr via `tracing`, shaped by `RAG_LOG_FORMAT` and `RUST_LOG`.
- Examples:
  - `RAG_OUTPUT_FORMAT=json rag query 'x' | jq .`
  - `RAG_OUTPUT_FORMAT=json RAG_LOG_FORMAT=json rag ingest --apply > out.ndjson 2> logs.ndjson`
  - `RAG_OUTPUT_FORMAT=text RAG_LOG_FORMAT=json rag stats > out.txt 2> logs.ndjson`
  - More in `docs/20251003T075300_output_examples.md`.

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
- `rag feed ls [--active <bool>]` — list feeds (omit to show all)
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

## Telemetry & Outputs

- Logs go to stderr. Set `RAG_LOG_FORMAT=json` for structured logs; otherwise compact text is used. Control verbosity via `RUST_LOG`.
- Outputs go to stdout. Select presenter with `RAG_OUTPUT_FORMAT`:
  - `text` — human headings; with `RAG_OUTPUT_PRETTY=true`, pretty-print payloads.
  - `json` — NDJSON envelopes per Plan/Result.
  - `mcp` — NDJSON JSON-RPC notifications (`notifications/plan`, `notifications/result`).
- Errors: commands exit non-zero on failure; details are logged to stderr. No stdout Error envelope by default.

## ANN Internals (pgvector + ivfflat)

- Backend
  - Uses Postgres + `pgvector` with `ivfflat` over the embedding column and cosine distance (`vector_cosine_ops`).
  - Index DDL is created/swapped by reindex logic: `src/maintenance/reindex/db.rs`.

- Query Flow
  - Embed query text with E5 ONNX, then set probes and run ANN SQL:
    - Encoder and probes application: `src/query/mod.rs`.
    - Candidate fetch (ANN): `src/query/db.rs` uses `ORDER BY (e.vec <-> $1) ASC LIMIT $N`.
    - Post-filter/shape results (cap per document, topk): `src/query/post.rs`.

- Tuning Knobs
  - `lists` (index-time, ivfflat clusters): managed by `rag reindex`; see `src/maintenance/reindex/mod.rs` and `src/maintenance/reindex/db.rs`.
  - `probes` (query-time, clusters searched): set via `SET LOCAL ivfflat.probes = p`; default heuristic ≈ `lists/10`. Override with `--probes`.

- Filters and Distance
  - Optional filters on feed and time are applied in SQL while the ANN index drives ordering.
  - Cosine distance operator `<->` sorts ascending; smaller means closer.

- Maintenance
  - Reindex command can reindex in place or create a new index with different `lists`, drop old, and rename new. Analyze follows to refresh stats.

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

## Code Structure

- Each module separates database access into a `db.rs` where practical:
  - `feed/db.rs`, `ingestion/db.rs`, `stats/db.rs`, `pipeline/chunk/db.rs`, `maintenance/reindex/db.rs`.
  - Orchestration and telemetry stay in `mod.rs` or view files (e.g., `stats/{summary,feed,doc,chunk}.rs`).
  - Types for JSON envelopes live in `*/types.rs` and are reused across commands.
