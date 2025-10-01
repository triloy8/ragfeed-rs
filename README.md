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
rag init --apply --dsn "$DATABASE_URL"
```
If your DB is fresh, also ensure:
```sql
CREATE SCHEMA IF NOT EXISTS rag;
CREATE EXTENSION IF NOT EXISTS vector;
```

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

- `rag init [--apply]` — runs SQL migrations (plan by default)
- `rag feed add <url> [--name <str>] [--active <bool>] [--apply]` — upsert a feed
- `rag feed ls [--active-only]` — list feeds
- `rag ingest [--feed <id>] [--feed-url <url>] [--limit <n>] [--force-refetch] [--apply]` — fetch RSS, pull pages, extract text, write `rag.document`
- `rag chunk [--since <win|date>] [--doc-id <id>] [--tokens-target <n>] [--overlap <n>] [--max-chunks-per-doc <n>] [--force] [--apply]` — produce `rag.chunk`
- `rag embed [--model-id <id>] [--onnx-filename <path>] [--device cpu|cuda] [--dim <n>] [--batch <n>] [--max <n>] [--force] [--apply]` — write `rag.embedding`
- `rag query <text> [--top-n <n>] [--topk <n>] [--doc-cap <n>] [--probes <k>] [--feed <id>] [--since <date|win>] [--show-context]` — ANN over embeddings
- `rag stats [--feed <id>] [--doc <id>] [--chunk <id>]` — operational views
- `rag reindex [--lists <k>] [--apply]` — create/reindex/swap ivfflat index
- `rag gc [--older-than <win|date>] [--feed <id>] [--max <n>] [--vacuum analyze|full|off] [--fix-status] [--drop-temp-indexes] [--apply]` — cleanup

## Embedding Models

- Default: `intfloat/e5-small-v2` (384‑dim). The encoder downloads tokenizer and ONNX model from the Hugging Face Hub.
- You can override `--onnx-filename` to point to a specific ONNX file inside the model repo.
- Device: `--device cpu` (default) or `--device cuda` (requires CUDA build and system drivers).

## Telemetry & JSON

- Human logs are compact text. Set `RAG_LOG_FORMAT=json` to emit JSON logs to stderr.
- Add global `--json` to get a single machine‑readable plan/result envelope on stdout; ideal for scripting.

## Troubleshooting

- Build fails with sqlx macro errors:
  - Ensure `DATABASE_URL` points to a reachable DB during `cargo build`, or switch to sqlx offline with a generated `sqlx-data.json`.
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
