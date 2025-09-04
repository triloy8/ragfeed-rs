use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

pub async fn count_orphan_embeddings(pool: &PgPool) -> Result<i64> {
    let n = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*)::bigint
        FROM rag.embedding e
        WHERE NOT EXISTS (SELECT 1 FROM rag.chunk c WHERE c.chunk_id = e.chunk_id)
        "#
    )
    .fetch_one(pool)
    .await?;
    Ok(n.unwrap_or(0))
}

pub async fn count_orphan_chunks(pool: &PgPool, feed: Option<i32>) -> Result<i64> {
    let n = match feed {
        None => sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)::bigint
            FROM rag.chunk c
            WHERE NOT EXISTS (SELECT 1 FROM rag.document d WHERE d.doc_id = c.doc_id)
            "#
        )
        .fetch_one(pool)
        .await?,
        Some(fid) => sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)::bigint
            FROM rag.chunk c
            WHERE NOT EXISTS (SELECT 1 FROM rag.document d WHERE d.doc_id = c.doc_id)
              AND EXISTS (SELECT 1 FROM rag.document d2 WHERE d2.doc_id = c.doc_id AND d2.feed_id = $1)
            "#,
            fid
        )
        .fetch_one(pool)
        .await?,
    };
    Ok(n.unwrap_or(0))
}

pub async fn count_error_docs(pool: &PgPool, cutoff: Option<DateTime<Utc>>, feed: Option<i32>) -> Result<i64> {
    let n = match (cutoff, feed) {
        (Some(ts), None) => sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)::bigint FROM rag.document d
            WHERE d.status = 'error' AND d.fetched_at < $1
            "#,
            ts
        )
        .fetch_one(pool)
        .await?,
        (Some(ts), Some(fid)) => sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)::bigint FROM rag.document d
            WHERE d.status = 'error' AND d.fetched_at < $1 AND d.feed_id = $2
            "#,
            ts,
            fid
        )
        .fetch_one(pool)
        .await?,
        (None, None) => sqlx::query_scalar!(
            r#"SELECT COUNT(*)::bigint FROM rag.document d WHERE d.status = 'error'"#
        )
        .fetch_one(pool)
        .await?,
        (None, Some(fid)) => sqlx::query_scalar!(
            r#"SELECT COUNT(*)::bigint FROM rag.document d WHERE d.status = 'error' AND d.feed_id = $1"#,
            fid
        )
        .fetch_one(pool)
        .await?,
    };
    Ok(n.unwrap_or(0))
}

pub async fn count_never_chunked_docs(pool: &PgPool, cutoff: Option<DateTime<Utc>>, feed: Option<i32>) -> Result<i64> {
    let n = match (cutoff, feed) {
        (Some(ts), None) => sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)::bigint FROM rag.document d
            WHERE d.status = 'ingest' AND d.fetched_at < $1
              AND NOT EXISTS (SELECT 1 FROM rag.chunk c WHERE c.doc_id = d.doc_id)
            "#,
            ts
        )
        .fetch_one(pool)
        .await?,
        (Some(ts), Some(fid)) => sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)::bigint FROM rag.document d
            WHERE d.status = 'ingest' AND d.fetched_at < $1 AND d.feed_id = $2
              AND NOT EXISTS (SELECT 1 FROM rag.chunk c WHERE c.doc_id = d.doc_id)
            "#,
            ts,
            fid
        )
        .fetch_one(pool)
        .await?,
        (None, None) => sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)::bigint FROM rag.document d
            WHERE d.status = 'ingest'
              AND NOT EXISTS (SELECT 1 FROM rag.chunk c WHERE c.doc_id = d.doc_id)
            "#
        )
        .fetch_one(pool)
        .await?,
        (None, Some(fid)) => sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)::bigint FROM rag.document d
            WHERE d.status = 'ingest' AND d.feed_id = $1
              AND NOT EXISTS (SELECT 1 FROM rag.chunk c WHERE c.doc_id = d.doc_id)
            "#,
            fid
        )
        .fetch_one(pool)
        .await?,
    };
    Ok(n.unwrap_or(0))
}

pub async fn count_bad_chunks(pool: &PgPool, feed: Option<i32>) -> Result<i64> {
    let n = match feed {
        None => sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)::bigint FROM rag.chunk c
            WHERE (c.text IS NULL OR btrim(c.text) = '' OR c.token_count <= 0)
            "#
        )
        .fetch_one(pool)
        .await?,
        Some(fid) => sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)::bigint FROM rag.chunk c
            JOIN rag.document d ON d.doc_id = c.doc_id
            WHERE d.feed_id = $1 AND (c.text IS NULL OR btrim(c.text) = '' OR c.token_count <= 0)
            "#,
            fid
        )
        .fetch_one(pool)
        .await?,
    };
    Ok(n.unwrap_or(0))
}

