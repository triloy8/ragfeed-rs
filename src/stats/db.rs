use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::stats::types::*;

// -------- Summary helpers --------

pub async fn fetch_feeds(pool: &PgPool) -> Result<Vec<StatsFeedRow>> {
    let rows = sqlx::query!(
        r#"
        SELECT feed_id, name, url, is_active, added_at
        FROM rag.feed
        ORDER BY feed_id
        "#
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|r| StatsFeedRow { feed_id: r.feed_id, name: r.name, url: r.url, is_active: r.is_active, added_at: r.added_at }).collect())
}

pub async fn docs_by_status(pool: &PgPool) -> Result<Vec<StatsDocStatus>> {
    let rows = sqlx::query!(
        r#"
        SELECT COALESCE(status,'') AS status, COUNT(*)::bigint AS cnt
        FROM rag.document
        GROUP BY status
        ORDER BY status
        "#
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|r| StatsDocStatus { status: r.status.unwrap_or_default(), cnt: r.cnt.unwrap_or(0) }).collect())
}

pub async fn last_fetched(pool: &PgPool) -> Result<Option<DateTime<Utc>>> {
    let row = sqlx::query!("SELECT MAX(fetched_at) AS last_fetched FROM rag.document")
        .fetch_one(pool)
        .await?;
    Ok(row.last_fetched)
}

pub async fn chunks_summary(pool: &PgPool) -> Result<StatsChunksSummary> {
    let row = sqlx::query!(
        r#"
        SELECT COUNT(*)::bigint AS total,
               AVG(token_count)::float8 AS avg
        FROM rag.chunk
        "#
    )
    .fetch_one(pool)
    .await?;
    Ok(StatsChunksSummary { total: row.total.unwrap_or(0), avg_tokens: row.avg.unwrap_or(0.0) })
}

pub async fn embeddings_totals(pool: &PgPool) -> Result<StatsEmbeddings> {
    let total = sqlx::query!("SELECT COUNT(*)::bigint AS total FROM rag.embedding")
        .fetch_one(pool)
        .await?
        .total
        .unwrap_or(0);
    let models_rows = sqlx::query!(
        r#"
        SELECT model, COUNT(*)::bigint AS cnt, MAX(created_at) AS last
        FROM rag.embedding
        GROUP BY model
        ORDER BY cnt DESC
        "#
    )
    .fetch_all(pool)
    .await?;
    let models: Vec<StatsModelInfo> = models_rows.into_iter().map(|m| StatsModelInfo { model: m.model, cnt: m.cnt.unwrap_or(0), last: m.last }).collect();
    Ok(StatsEmbeddings { total, models })
}

pub async fn index_meta(pool: &PgPool) -> Result<StatsIndexMeta> {
    let idx_row = sqlx::query!(
        r#"
        SELECT substring(pg_get_indexdef(i.indexrelid) from 'lists = ([0-9]+)') AS lists
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid
        JOIN pg_namespace nsp ON nsp.oid = c.relnamespace
        WHERE nsp.nspname = 'rag' AND c.relname = 'embedding_vec_ivf_idx'
        "#
    )
    .fetch_optional(pool)
    .await?;
    let lists: Option<i32> = idx_row.as_ref().and_then(|r| r.lists.as_ref()).and_then(|s| s.parse::<i32>().ok());

    let size_row = sqlx::query!(r#"SELECT pg_size_pretty(pg_relation_size('rag.embedding_vec_ivf_idx')) AS size"#)
        .fetch_optional(pool)
        .await?;
    let size_pretty = size_row.and_then(|r| r.size);

    let analyze_row = sqlx::query!(
        r#"
        SELECT last_analyze
        FROM pg_stat_user_tables
        WHERE schemaname = 'rag' AND relname = 'embedding'
        "#
    )
    .fetch_optional(pool)
    .await?;
    let last_analyze = analyze_row.and_then(|r| r.last_analyze);
    Ok(StatsIndexMeta { lists, size_pretty, last_analyze })
}

pub async fn coverage(pool: &PgPool) -> Result<StatsCoverage> {
    let totals = sqlx::query!(
        r#"
        SELECT
          (SELECT COUNT(*)::bigint FROM rag.chunk) AS chunks,
          (SELECT COUNT(*)::bigint FROM rag.embedding) AS embedded
        "#
    )
    .fetch_one(pool)
    .await?;
    let chunks_i64 = totals.chunks.unwrap_or(0);
    let embedded_i64 = totals.embedded.unwrap_or(0);
    let pct = if chunks_i64 > 0 { (embedded_i64 as f64 / chunks_i64 as f64) * 100.0 } else { 0.0 };
    let missing = sqlx::query!(
        r#"
        SELECT COUNT(*)::bigint AS missing
        FROM rag.chunk c
        LEFT JOIN rag.embedding e
          ON e.chunk_id = c.chunk_id
        WHERE e.chunk_id IS NULL
        "#
    )
    .fetch_one(pool)
    .await?
    .missing
    .unwrap_or(0);
    Ok(StatsCoverage { chunks: chunks_i64, embedded: embedded_i64, pct, missing })
}

// -------- Feed page helpers --------

pub async fn feed_header(pool: &PgPool, feed_id: i32) -> Result<StatsFeedMeta> {
    let f = sqlx::query!(
        r#"
        SELECT feed_id, name, url, is_active, added_at
        FROM rag.feed
        WHERE feed_id = $1
        "#,
        feed_id
    )
    .fetch_one(pool)
    .await?;
    Ok(StatsFeedMeta { feed_id: f.feed_id, name: f.name, url: f.url, is_active: f.is_active, added_at: f.added_at })
}

pub async fn feed_docs_by_status(pool: &PgPool, feed_id: i32) -> Result<Vec<StatsDocStatus>> {
    let rows = sqlx::query!(
        r#"
        SELECT COALESCE(status,'') AS status, COUNT(*)::bigint AS cnt
        FROM rag.document
        WHERE feed_id = $1
        GROUP BY status
        ORDER BY status
        "#,
        feed_id
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|r| StatsDocStatus { status: r.status.unwrap_or_default(), cnt: r.cnt.unwrap_or(0) }).collect())
}

pub async fn feed_last_fetched(pool: &PgPool, feed_id: i32) -> Result<Option<DateTime<Utc>>> {
    let row = sqlx::query!(
        r#"SELECT MAX(fetched_at) AS last_fetched FROM rag.document WHERE feed_id = $1"#,
        feed_id
    )
    .fetch_one(pool)
    .await?;
    Ok(row.last_fetched)
}

pub async fn feed_chunks_summary(pool: &PgPool, feed_id: i32) -> Result<StatsChunksSummary> {
    let row = sqlx::query!(
        r#"
        SELECT COUNT(*)::bigint AS total_chunks,
               AVG(c.token_count)::float8 AS avg_tokens
        FROM rag.chunk c
        JOIN rag.document d ON d.doc_id = c.doc_id
        WHERE d.feed_id = $1
        "#,
        feed_id
    )
    .fetch_one(pool)
    .await?;
    Ok(StatsChunksSummary { total: row.total_chunks.unwrap_or(0), avg_tokens: row.avg_tokens.unwrap_or(0.0) })
}

pub async fn feed_coverage(pool: &PgPool, feed_id: i32) -> Result<StatsFeedCoverage> {
    let cov = sqlx::query!(
        r#"
        SELECT
          (SELECT COUNT(*)::bigint
           FROM rag.chunk c JOIN rag.document d ON d.doc_id = c.doc_id
           WHERE d.feed_id = $1) AS chunks,
          (SELECT COUNT(*)::bigint
           FROM rag.embedding e
           JOIN rag.chunk c ON c.chunk_id = e.chunk_id
           JOIN rag.document d ON d.doc_id = c.doc_id
           WHERE d.feed_id = $1) AS embedded,
          (SELECT MAX(e.created_at)
           FROM rag.embedding e
           JOIN rag.chunk c ON c.chunk_id = e.chunk_id
           JOIN rag.document d ON d.doc_id = c.doc_id
           WHERE d.feed_id = $1) AS last
        "#,
        feed_id
    )
    .fetch_one(pool)
    .await?;
    let chunks = cov.chunks.unwrap_or(0) as f64;
    let embedded = cov.embedded.unwrap_or(0) as f64;
    let pct = if chunks > 0.0 { (embedded / chunks) * 100.0 } else { 0.0 };
    Ok(StatsFeedCoverage { chunks: cov.chunks.unwrap_or(0), embedded: cov.embedded.unwrap_or(0), pct, last: cov.last })
}

pub async fn feed_missing_count(pool: &PgPool, feed_id: i32) -> Result<i64> {
    let missing = sqlx::query!(
        r#"
        SELECT COUNT(*)::bigint AS missing
        FROM rag.chunk c
        JOIN rag.document d ON d.doc_id = c.doc_id
        LEFT JOIN rag.embedding e ON e.chunk_id = c.chunk_id
        WHERE d.feed_id = $1 AND e.chunk_id IS NULL
        "#,
        feed_id
    )
    .fetch_one(pool)
    .await?
    .missing
    .unwrap_or(0);
    Ok(missing)
}

pub async fn feed_models(pool: &PgPool, feed_id: i32) -> Result<Vec<StatsModelInfo>> {
    let rows = sqlx::query!(
        r#"
        SELECT e.model, COUNT(*)::bigint AS cnt, MAX(e.created_at) AS last
        FROM rag.embedding e
        JOIN rag.chunk c ON c.chunk_id = e.chunk_id
        JOIN rag.document d ON d.doc_id = c.doc_id
        WHERE d.feed_id = $1
        GROUP BY e.model
        ORDER BY cnt DESC
        "#,
        feed_id
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|m| StatsModelInfo { model: m.model, cnt: m.cnt.unwrap_or(0), last: m.last }).collect())
}

pub async fn feed_pending_top_docs(pool: &PgPool, feed_id: i32, limit: i64) -> Result<Vec<StatsPendingTopDoc>> {
    let rows = sqlx::query!(
        r#"
        SELECT d.doc_id, d.source_title, COUNT(*)::bigint AS pending
        FROM rag.chunk c
        JOIN rag.document d ON d.doc_id = c.doc_id
        LEFT JOIN rag.embedding e ON e.chunk_id = c.chunk_id
        WHERE d.feed_id = $1 AND e.chunk_id IS NULL
        GROUP BY d.doc_id, d.source_title
        ORDER BY pending DESC
        LIMIT $2
        "#,
        feed_id,
        limit
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|r| StatsPendingTopDoc { doc_id: r.doc_id, source_title: r.source_title, pending: r.pending.unwrap_or(0) }).collect())
}

pub async fn latest_docs(pool: &PgPool, feed_id: i32, limit: i64) -> Result<Vec<StatsLatestDoc>> {
    let rows = sqlx::query!(
        r#"
        SELECT doc_id, status, fetched_at, source_title
        FROM rag.document
        WHERE feed_id = $1
        ORDER BY fetched_at DESC NULLS LAST, doc_id DESC
        LIMIT $2
        "#,
        feed_id,
        limit
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|r| StatsLatestDoc { doc_id: r.doc_id, status: r.status, fetched_at: r.fetched_at, source_title: r.source_title }).collect())
}

// -------- Snapshots --------

pub async fn chunk_snap(pool: &PgPool, id: i64) -> Result<StatsChunkSnap> {
    let row = sqlx::query!(
        r#"
        SELECT chunk_id, doc_id, chunk_index, token_count,
               substring(text, 1, 400) AS preview
        FROM rag.chunk
        WHERE chunk_id = $1
        "#,
        id
    )
    .fetch_one(pool)
    .await?;
    Ok(StatsChunkSnap { chunk_id: row.chunk_id, doc_id: row.doc_id, chunk_index: row.chunk_index, token_count: row.token_count, preview: row.preview })
}

pub async fn doc_snapshot(pool: &PgPool, id: i64, chunk_limit: i64) -> Result<StatsDocSnapshot> {
    let row = sqlx::query!(
        r#"
        SELECT doc_id, feed_id, source_url, source_title, published_at,
               fetched_at, status, error_msg,
               substring(text_clean, 1, 400) AS preview
        FROM rag.document
        WHERE doc_id = $1
        "#,
        id
    )
    .fetch_one(pool)
    .await?;
    let doc = StatsDocInfo {
        doc_id: row.doc_id,
        feed_id: row.feed_id,
        source_url: row.source_url,
        source_title: row.source_title,
        published_at: row.published_at,
        fetched_at: row.fetched_at,
        status: row.status,
        error_msg: row.error_msg,
        preview: row.preview,
    };
    let chunks_rows = sqlx::query!(
        r#"
        SELECT chunk_id, chunk_index, token_count
        FROM rag.chunk
        WHERE doc_id = $1
        ORDER BY chunk_index ASC
        LIMIT $2
        "#,
        id,
        chunk_limit
    )
    .fetch_all(pool)
    .await?;
    let chunks = chunks_rows.into_iter().map(|r| StatsDocChunkInfo { chunk_id: r.chunk_id, chunk_index: r.chunk_index, token_count: r.token_count }).collect();
    Ok(StatsDocSnapshot { doc, chunks })
}

