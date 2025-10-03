use anyhow::Result;
use sqlx::PgPool;

pub struct IngestFeedRow {
    pub feed_id: i32,
    pub url: String,
    pub name: Option<String>,
}

pub async fn select_feeds(pool: &PgPool, feed: Option<i32>, feed_url: Option<&str>) -> Result<Vec<IngestFeedRow>> {
    let rows = sqlx::query!(
        r#"
        SELECT feed_id, url, name
        FROM rag.feed
        WHERE
          ($1::INT4 IS NULL OR feed_id = $1::INT4) AND
          ($2::TEXT IS NULL OR url     = $2::TEXT) AND
          ($1::INT4 IS NOT NULL OR $2::TEXT IS NOT NULL OR is_active = TRUE)
        ORDER BY feed_id
        "#,
        feed,
        feed_url
    )
    .fetch_all(pool)
    .await?;

    let out = rows
        .into_iter()
        .map(|r| IngestFeedRow { feed_id: r.feed_id, url: r.url, name: r.name })
        .collect();
    Ok(out)
}

