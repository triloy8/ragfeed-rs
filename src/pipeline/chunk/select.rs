use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};

// Select candidate documents to chunk based on optional filters.
// Mirrors the previous logic in crate::chunk::select_docs.
pub async fn select_docs(
    pool: &PgPool,
    doc_id: Option<i64>,
    since: Option<DateTime<Utc>>,
    force: bool,
) -> Result<Vec<(i64, Option<String>)>> {
    let rows = sqlx::query(
        r#"
        SELECT doc_id, text_clean
        FROM rag.document
        WHERE ($3::bool OR status = 'ingest')
          AND ($1::bigint      IS NULL OR doc_id = $1)
          AND ($2::timestamptz IS NULL OR fetched_at >= $2)
        ORDER BY doc_id DESC
        LIMIT 1000
        "#,
    )
    .bind(doc_id)
    .bind(since)
    .bind(force)
    .fetch_all(pool)
    .await?;

    let docs = rows
        .into_iter()
        .map(|row| (row.get::<i64, _>("doc_id"), row.get::<Option<String>, _>("text_clean")))
        .collect();
    Ok(docs)
}

