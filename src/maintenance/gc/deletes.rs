use anyhow::Result;
use sqlx::PgPool;

use crate::out;
use crate::util::sql::paged_loop;

pub async fn delete_orphan_embeddings(pool: &PgPool, max: i64) -> Result<()> {
    paged_loop(
        pool,
        |limit| {
            sqlx::query(
                r#"
                DELETE FROM rag.embedding e
                WHERE e.ctid IN (
                    SELECT e2.ctid
                    FROM rag.embedding e2
                    WHERE NOT EXISTS (
                        SELECT 1 FROM rag.chunk c WHERE c.chunk_id = e2.chunk_id
                    )
                    LIMIT $1
                )
                "#,
            )
            .bind(limit)
        },
        max,
        |n| {
            let log = out::gc();
            log.info(format!("  🗑️ Deleted {} orphan embeddings", n));
        },
    )
    .await
}

pub async fn delete_orphan_chunks(pool: &PgPool, feed: Option<i32>, max: i64) -> Result<()> {
    match feed {
        None => paged_loop(
            pool,
            |limit| {
                sqlx::query(
                    r#"
                    DELETE FROM rag.chunk c
                    WHERE c.ctid IN (
                        SELECT c2.ctid
                        FROM rag.chunk c2
                        WHERE NOT EXISTS (
                            SELECT 1 FROM rag.document d WHERE d.doc_id = c2.doc_id
                        )
                        LIMIT $1
                    )
                    "#,
                )
                .bind(limit)
            },
            max,
            |n| { let log = out::gc(); log.info(format!("  🗑️ Deleted {} orphan chunks", n)); },
        )
        .await,
        Some(fid) => paged_loop(
            pool,
            move |limit| {
                sqlx::query(
                    r#"
                    DELETE FROM rag.chunk c
                    WHERE c.ctid IN (
                        SELECT c2.ctid
                        FROM rag.chunk c2
                        JOIN rag.document d2 ON d2.doc_id = c2.doc_id
                        WHERE d2.feed_id = $1
                          AND NOT EXISTS (
                            SELECT 1 FROM rag.document d WHERE d.doc_id = c2.doc_id
                          )
                        LIMIT $2
                    )
                    "#,
                )
                .bind(fid)
                .bind(limit)
            },
            max,
            |n| { let log = out::gc(); log.info(format!("  🗑️ Deleted {} orphan chunks", n)); },
        )
        .await,
    }
}

use chrono::{DateTime, Utc};

pub async fn delete_error_docs(pool: &PgPool, cutoff: Option<DateTime<Utc>>, feed: Option<i32>, max: i64) -> Result<()> {
    match (cutoff, feed) {
        (Some(ts), None) => paged_loop(
            pool,
            move |limit| {
                sqlx::query(
                    r#"
                    DELETE FROM rag.document d
                    WHERE d.ctid IN (
                        SELECT d2.ctid FROM rag.document d2
                        WHERE d2.status = 'error' AND d2.fetched_at < $1
                        LIMIT $2
                    )
                    "#,
                )
                .bind(ts)
                .bind(limit)
            },
            max,
            |n| { let log = out::gc(); log.info(format!("  🗑️ Deleted {} error docs", n)); },
        )
        .await,
        (Some(ts), Some(fid)) => paged_loop(
            pool,
            move |limit| {
                sqlx::query(
                    r#"
                    DELETE FROM rag.document d
                    WHERE d.ctid IN (
                        SELECT d2.ctid FROM rag.document d2
                        WHERE d2.status = 'error' AND d2.fetched_at < $1 AND d2.feed_id = $2
                        LIMIT $3
                    )
                    "#,
                )
                .bind(ts)
                .bind(fid)
                .bind(limit)
            },
            max,
            |n| { let log = out::gc(); log.info(format!("  🗑️ Deleted {} error docs", n)); },
        )
        .await,
        (None, None) => paged_loop(
            pool,
            |limit| {
                sqlx::query(
                    r#"
                    DELETE FROM rag.document d
                    WHERE d.ctid IN (
                        SELECT d2.ctid FROM rag.document d2
                        WHERE d2.status = 'error'
                        LIMIT $1
                    )
                    "#,
                )
                .bind(limit)
            },
            max,
            |n| { let log = out::gc(); log.info(format!("  🗑️ Deleted {} error docs", n)); },
        )
        .await,
        (None, Some(fid)) => paged_loop(
            pool,
            move |limit| {
                sqlx::query(
                    r#"
                    DELETE FROM rag.document d
                    WHERE d.ctid IN (
                        SELECT d2.ctid FROM rag.document d2
                        WHERE d2.status = 'error' AND d2.feed_id = $1
                        LIMIT $2
                    )
                    "#,
                )
                .bind(fid)
                .bind(limit)
            },
            max,
            |n| { let log = out::gc(); log.info(format!("  🗑️ Deleted {} error docs", n)); },
        )
        .await,
    }
}

pub async fn delete_never_chunked_docs(pool: &PgPool, cutoff: Option<DateTime<Utc>>, feed: Option<i32>, max: i64) -> Result<()> {
    match (cutoff, feed) {
        (Some(ts), None) => paged_loop(
            pool,
            move |limit| {
                sqlx::query(
                    r#"
                    DELETE FROM rag.document d
                    WHERE d.ctid IN (
                        SELECT d2.ctid FROM rag.document d2
                        WHERE d2.status = 'ingest' AND d2.fetched_at < $1
                          AND NOT EXISTS (SELECT 1 FROM rag.chunk c WHERE c.doc_id = d2.doc_id)
                        LIMIT $2
                    )
                    "#,
                )
                .bind(ts)
                .bind(limit)
            },
            max,
            |n| { let log = out::gc(); log.info(format!("  🗑️ Deleted {} never-chunked docs", n)); },
        )
        .await,
        (Some(ts), Some(fid)) => paged_loop(
            pool,
            move |limit| {
                sqlx::query(
                    r#"
                    DELETE FROM rag.document d
                    WHERE d.ctid IN (
                        SELECT d2.ctid FROM rag.document d2
                        WHERE d2.status = 'ingest' AND d2.fetched_at < $1 AND d2.feed_id = $2
                          AND NOT EXISTS (SELECT 1 FROM rag.chunk c WHERE c.doc_id = d2.doc_id)
                        LIMIT $3
                    )
                    "#,
                )
                .bind(ts)
                .bind(fid)
                .bind(limit)
            },
            max,
            |n| { let log = out::gc(); log.info(format!("  🗑️ Deleted {} never-chunked docs", n)); },
        )
        .await,
        (None, None) => paged_loop(
            pool,
            |limit| {
                sqlx::query(
                    r#"
                    DELETE FROM rag.document d
                    WHERE d.ctid IN (
                        SELECT d2.ctid FROM rag.document d2
                        WHERE d2.status = 'ingest'
                          AND NOT EXISTS (SELECT 1 FROM rag.chunk c WHERE c.doc_id = d2.doc_id)
                        LIMIT $1
                    )
                    "#,
                )
                .bind(limit)
            },
            max,
            |n| { let log = out::gc(); log.info(format!("  🗑️ Deleted {} never-chunked docs", n)); },
        )
        .await,
        (None, Some(fid)) => paged_loop(
            pool,
            move |limit| {
                sqlx::query(
                    r#"
                    DELETE FROM rag.document d
                    WHERE d.ctid IN (
                        SELECT d2.ctid FROM rag.document d2
                        WHERE d2.status = 'ingest' AND d2.feed_id = $1
                          AND NOT EXISTS (SELECT 1 FROM rag.chunk c WHERE c.doc_id = d2.doc_id)
                        LIMIT $2
                    )
                    "#,
                )
                .bind(fid)
                .bind(limit)
            },
            max,
            |n| { let log = out::gc(); log.info(format!("  🗑️ Deleted {} never-chunked docs", n)); },
        )
        .await,
    }
}

pub async fn delete_bad_chunks(pool: &PgPool, feed: Option<i32>, max: i64) -> Result<()> {
    match feed {
        None => paged_loop(
            pool,
            |limit| {
                sqlx::query(
                    r#"
                    DELETE FROM rag.chunk c
                    WHERE c.ctid IN (
                        SELECT c2.ctid FROM rag.chunk c2
                        WHERE (c2.text IS NULL OR btrim(c2.text) = '' OR c2.token_count <= 0)
                        LIMIT $1
                    )
                    "#,
                )
                .bind(limit)
            },
            max,
            |n| { let log = out::gc(); log.info(format!("  🗑️ Deleted {} bad chunks", n)); },
        )
        .await,
        Some(fid) => paged_loop(
            pool,
            move |limit| {
                sqlx::query(
                    r#"
                    DELETE FROM rag.chunk c
                    WHERE c.ctid IN (
                        SELECT c2.ctid FROM rag.chunk c2
                        JOIN rag.document d ON d.doc_id = c2.doc_id
                        WHERE d.feed_id = $1
                          AND (c2.text IS NULL OR btrim(c2.text) = '' OR c2.token_count <= 0)
                        LIMIT $2
                    )
                    "#,
                )
                .bind(fid)
                .bind(limit)
            },
            max,
            |n| { let log = out::gc(); log.info(format!("  🗑️ Deleted {} bad chunks", n)); },
        )
        .await,
    }
}
