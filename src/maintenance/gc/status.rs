use anyhow::Result;
use sqlx::PgPool;

use crate::telemetry;

pub async fn fix_statuses(pool: &PgPool, feed: Option<i32>) -> Result<()> {
    // embedded
    let res = match feed {
        None => sqlx::query!(
            r#"
            UPDATE rag.document d SET status='embedded'
            WHERE EXISTS (SELECT 1 FROM rag.chunk c WHERE c.doc_id = d.doc_id)
              AND NOT EXISTS (
                SELECT 1 FROM rag.chunk c
                LEFT JOIN rag.embedding e ON e.chunk_id = c.chunk_id
                WHERE c.doc_id = d.doc_id AND e.chunk_id IS NULL
              )
              AND (d.status IS DISTINCT FROM 'embedded')
            "#
        )
        .execute(pool)
        .await?,
        Some(fid) => sqlx::query!(
            r#"
            UPDATE rag.document d SET status='embedded'
            WHERE d.feed_id = $1
              AND EXISTS (SELECT 1 FROM rag.chunk c WHERE c.doc_id = d.doc_id)
              AND NOT EXISTS (
                SELECT 1 FROM rag.chunk c
                LEFT JOIN rag.embedding e ON e.chunk_id = c.chunk_id
                WHERE c.doc_id = d.doc_id AND e.chunk_id IS NULL
              )
              AND (d.status IS DISTINCT FROM 'embedded')
            "#,
            fid
        )
        .execute(pool)
        .await?,
    };
    let log = telemetry::gc();
    log.info(format!("✅ Set status=embedded on {} doc(s)", res.rows_affected()));

    // chunked
    let res = match feed {
        None => sqlx::query!(
            r#"
            UPDATE rag.document d SET status='chunked'
            WHERE EXISTS (SELECT 1 FROM rag.chunk c WHERE c.doc_id = d.doc_id)
              AND EXISTS (
                SELECT 1 FROM rag.chunk c
                LEFT JOIN rag.embedding e ON e.chunk_id = c.chunk_id
                WHERE c.doc_id = d.doc_id AND e.chunk_id IS NULL
              )
              AND (d.status IS DISTINCT FROM 'chunked')
            "#
        )
        .execute(pool)
        .await?,
        Some(fid) => sqlx::query!(
            r#"
            UPDATE rag.document d SET status='chunked'
            WHERE d.feed_id = $1
              AND EXISTS (SELECT 1 FROM rag.chunk c WHERE c.doc_id = d.doc_id)
              AND EXISTS (
                SELECT 1 FROM rag.chunk c
                LEFT JOIN rag.embedding e ON e.chunk_id = c.chunk_id
                WHERE c.doc_id = d.doc_id AND e.chunk_id IS NULL
              )
              AND (d.status IS DISTINCT FROM 'chunked')
            "#,
            fid
        )
        .execute(pool)
        .await?,
    };
    let log = telemetry::gc();
    log.info(format!("✅ Set status=chunked on {} doc(s)", res.rows_affected()));

    // ingest
    let res = match feed {
        None => sqlx::query!(
            r#"
            UPDATE rag.document d SET status='ingest'
            WHERE NOT EXISTS (SELECT 1 FROM rag.chunk c WHERE c.doc_id = d.doc_id)
              AND (d.status IS DISTINCT FROM 'ingest')
            "#
        )
        .execute(pool)
        .await?,
        Some(fid) => sqlx::query!(
            r#"
            UPDATE rag.document d SET status='ingest'
            WHERE d.feed_id = $1
              AND NOT EXISTS (SELECT 1 FROM rag.chunk c WHERE c.doc_id = d.doc_id)
              AND (d.status IS DISTINCT FROM 'ingest')
            "#,
            fid
        )
        .execute(pool)
        .await?,
    };
    let log = telemetry::gc();
    log.info(format!("✅ Set status=ingest on {} doc(s)", res.rows_affected()));

    Ok(())
}
