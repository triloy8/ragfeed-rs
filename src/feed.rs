use anyhow::Result;
use sqlx::{postgres::PgPoolOptions};

pub async fn add_feed(dsn: &str, url: &str) -> Result<()> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(dsn)
        .await?;

    // Check if feed already exists
    let existing_feed = sqlx::query!(
        "SELECT * FROM rag.feed WHERE url = $1",
        url
    )
    .fetch_one(&pool)
    .await;

    match existing_feed {
        Ok(_) => println!("Feed with URL {} already exists", url),
        Err(sqlx::Error::RowNotFound) => {
            // Insert new feed
            let _result = sqlx::query!(
                "INSERT INTO rag.feed (url, name) VALUES ($1, $2)",
                url,
                url.split('/').last().unwrap_or_default()
            )
            .execute(&pool)
            .await?;
            println!("Feed with URL {} added successfully", url);
        }
        Err(e) => return Err(e.into()),
    }

    Ok(())
}

pub async fn list_feeds(dsn: &str) -> Result<()> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(dsn)
        .await?;

    // List all feeds
    let feeds = sqlx::query!(
        "SELECT * FROM rag.feed"
    )
    .fetch_all(&pool)
    .await?;

    for feed in feeds {
        println!("Feed ID: {}, URL: {}", feed.feed_id, feed.url);
    }

    Ok(())
}