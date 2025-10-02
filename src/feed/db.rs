use anyhow::Result;
use sqlx::PgPool;

use crate::stats::types::StatsFeedRow;

pub async fn upsert_feed(pool: &PgPool, url: &str, name: Option<&str>, active: bool) -> Result<bool> {
    let rec = sqlx::query!(
        r#"
        INSERT INTO rag.feed (url, name, is_active)
        VALUES ($1, $2, $3)
        ON CONFLICT (url)
        DO UPDATE SET name = EXCLUDED.name, is_active = EXCLUDED.is_active
        RETURNING (xmax = 0) AS "inserted!: bool"
        "#,
        url,
        name,
        active
    )
    .fetch_one(pool)
    .await?;
    Ok(rec.inserted)
}

pub async fn list_feeds(pool: &PgPool, active: Option<bool>) -> Result<Vec<StatsFeedRow>> {
    let rows = sqlx::query!(
        r#"
        SELECT feed_id,
               url,
               name,
               COALESCE(is_active, TRUE) AS "is_active!: bool",
               added_at
        FROM rag.feed
        WHERE ($1::bool IS NULL OR is_active = $1)
        ORDER BY feed_id
        "#,
        active
    )
    .fetch_all(pool)
    .await?;

    let feeds = rows
        .into_iter()
        .map(|r| StatsFeedRow {
            feed_id: r.feed_id,
            name: r.name,
            url: r.url,
            is_active: Some(r.is_active),
            added_at: r.added_at,
        })
        .collect();
    Ok(feeds)
}
