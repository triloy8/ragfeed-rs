// src/ingest.rs
use anyhow::Result;
use sqlx::{PgPool};
use rss::Channel;
use reqwest::Client;
use scraper::{Html, Selector};
use std::time::Duration;
use chrono::{Utc};

#[derive(clap::Args)]
pub struct IngestCmd {
    #[arg(long)] feed: Option<i32>,
    #[arg(long)] feed_url: Option<String>,
    #[arg(long, default_value_t=200)] limit: usize,
    #[arg(long, default_value_t=2)] concurrency: usize,
    #[arg(long)] since: Option<String>, // parse into chrono::DateTime
    #[arg(long)] force_refetch: bool,
}

pub async fn run(pool: &PgPool, args: IngestCmd) -> Result<()> {
    let client = Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;

    // resolve feeds — single parameterized query (no branching)
    let feeds = sqlx::query!(
        r#"
        SELECT feed_id, url, name
        FROM rag.feed
        WHERE
        ($1::INT4 IS NULL OR feed_id = $1::INT4) AND
        ($2::TEXT IS NULL OR url     = $2::TEXT) AND
        -- if neither id nor url is provided, default to active feeds
        ($1::INT4 IS NOT NULL OR $2::TEXT IS NOT NULL OR is_active = TRUE)
        ORDER BY feed_id
        "#,
        args.feed,                       // Option<i64> OK with ::INT4 cast
        args.feed_url.as_deref()         // Option<&str> for TEXT
    )
    .fetch_all(pool)
    .await?;

    // fetch + parse each feed
    for f in feeds {
        let xml = client.get(&f.url).send().await?.bytes().await?;
        let channel = Channel::read_from(&xml[..])?;

        for item in channel.items().iter().take(args.limit) {
            if let Some(link) = item.link() {
                // fetch article
                let html = client.get(link).send().await?.text().await?;
                let doc = Html::parse_document(&html);
                let sel = Selector::parse("p").unwrap();
                let text: String = doc.select(&sel)
                    .map(|p| p.text().collect::<String>())
                    .collect::<Vec<_>>()
                    .join("\n");

                let published_at = item.pub_date()
                    .and_then(|s| chrono::DateTime::parse_from_rfc2822(s).ok())
                    .map(|dt| dt.with_timezone(&Utc));

                // insert into rag.document
                sqlx::query!(
                    r#"
                    INSERT INTO rag.document (feed_id, source_url, source_title,
                        published_at, fetched_at, content_hash, raw_html, text_clean, status)
                    VALUES ($1, $2, $3, $4, now(), md5($5), $6, $7, 'ingest')
                    ON CONFLICT (source_url) DO NOTHING
                    "#,
                    f.feed_id,
                    link,
                    item.title(),
                    published_at,   // Option<DateTime<Utc>>
                    text,
                    html.as_bytes(),
                    text
                )
                .execute(pool)
                .await?;
            }
        }
    }

    Ok(())
}
