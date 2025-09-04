use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

pub async fn upsert_document(
    pool: &PgPool,
    feed_id: i32,
    link: &str,
    title: Option<&str>,
    published_at: Option<DateTime<Utc>>,
    text: &str,
    raw_html: &[u8],
    status: &str,
    error_msg: Option<&str>,
) -> Result<bool> {
    let res = sqlx::query!(
        r#"
        INSERT INTO rag.document (feed_id, source_url, source_title,
            published_at, fetched_at, content_hash, raw_html, text_clean, status, error_msg)
        VALUES ($1, $2, $3, $4, now(), md5($5), $6, $7, $8, $9)
        ON CONFLICT (source_url) DO UPDATE
          SET source_title = EXCLUDED.source_title,
              published_at = COALESCE(EXCLUDED.published_at, rag.document.published_at),
              fetched_at   = now(),
              content_hash = EXCLUDED.content_hash,
              raw_html     = EXCLUDED.raw_html,
              text_clean   = EXCLUDED.text_clean,
              status       = EXCLUDED.status,
              error_msg    = EXCLUDED.error_msg
        RETURNING (xmax = 0) AS inserted
        "#,
        feed_id,
        link,
        title,
        published_at,
        text,
        raw_html,
        text,
        status,
        error_msg
    )
    .fetch_one(pool)
    .await?;
    Ok(res.inserted.unwrap_or(false))
}

pub async fn insert_document(
    pool: &PgPool,
    feed_id: i32,
    link: &str,
    title: Option<&str>,
    published_at: Option<DateTime<Utc>>,
    text: &str,
    raw_html: &[u8],
    status: &str,
    error_msg: Option<&str>,
) -> Result<bool> {
    let exec = sqlx::query!(
        r#"
        INSERT INTO rag.document (feed_id, source_url, source_title,
            published_at, fetched_at, content_hash, raw_html, text_clean, status, error_msg)
        VALUES ($1, $2, $3, $4, now(), md5($5), $6, $7, $8, $9)
        ON CONFLICT (source_url) DO NOTHING
        "#,
        feed_id,
        link,
        title,
        published_at,
        text,
        raw_html,
        text,
        status,
        error_msg
    )
    .execute(pool)
    .await?;
    Ok(exec.rows_affected() == 1)
}

