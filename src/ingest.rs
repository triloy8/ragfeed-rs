use anyhow::Result;
use sqlx::PgPool;
use rss::Channel;
use reqwest::Client;
use chrono::Utc;
use url::Url;

use crate::extractor;

#[derive(clap::Args)]
pub struct IngestCmd {
    #[arg(long)] feed: Option<i32>,
    #[arg(long)] feed_url: Option<String>,
    #[arg(long, default_value_t=200)] limit: usize,
    #[arg(long)] force_refetch: bool,
    #[arg(long, default_value_t=false)] apply: bool, // default is plan-only (no network, no writes)
    #[arg(long, default_value_t=10)] plan_limit: usize, // how many feeds to list in plan
    #[arg(long, default_value_t=false)] verbose: bool, // print per-item actions in --apply mode
}

pub async fn run(pool: &PgPool, args: IngestCmd) -> Result<()> {
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

    // plan-only default: do not perform network calls or DB writes
    if !args.apply {
        let mode = if args.force_refetch { "upsert" } else { "insert-only" };
        println!(
            "📝 Ingest plan — feeds={} mode={} limit={}",
            feeds.len(), mode, args.limit
        );
        for f in feeds.iter().take(args.plan_limit) {
            println!("  feed_id={} url={} name={}", f.feed_id, f.url, f.name.clone().unwrap_or_default());
        }
        if feeds.len() > args.plan_limit { println!("  ... ({} more)", feeds.len() - args.plan_limit); }
        println!("   Use --apply to fetch and write.");
        return Ok(());
    }

    // APPLY: perform network calls and writes
    let client = Client::new();

    // fetch + parse each feed
    let mut total_inserted = 0usize;
    let mut total_updated = 0usize;
    let mut total_skipped = 0usize;
    let mut total_errors  = 0usize;

    for f in feeds {
        let mut inserted = 0usize;
        let mut updated  = 0usize;
        let mut skipped  = 0usize;
        let mut errors   = 0usize;

        let xml = client.get(&f.url).send().await?.bytes().await?;
        let channel = Channel::read_from(&xml[..])?;

        for item in channel.items().iter().take(args.limit) {
            if let Some(link) = item.link() {
                // fetch article
                let html = client.get(link).send().await?.text().await?;

                // per-host extraction with fallback
                let host = Url::parse(link).ok().and_then(|u| u.host_str().map(|s| s.to_string())).unwrap_or_default();
                let extracted = extractor::extract(&host, &html);
                let (text, status, error_msg) = match extracted {
                    Some(t) if !t.trim().is_empty() => (t, "ingest", None),
                    _ => (String::new(), "error", Some(String::from("extraction-empty"))),
                };
                if status == "error" { errors += 1; }

                let published_at = item.pub_date()
                    .and_then(|s| chrono::DateTime::parse_from_rfc2822(s).ok())
                    .map(|dt| dt.with_timezone(&Utc));

                // insert or upsert into rag.document
                if args.force_refetch {
                    // Refresh existing rows on conflict
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
                        f.feed_id,
                        link,
                        item.title(),
                        published_at,   // Option<DateTime<Utc>>
                        text,
                        html.as_bytes(),
                        text,
                        status,
                        error_msg
                    )
                    .fetch_one(pool)
                    .await?;
                    if res.inserted.unwrap_or(false) {
                        inserted += 1;
                        if args.verbose { println!("➕ inserted: {} ({})", item.title().unwrap_or(""), link); }
                    } else {
                        updated += 1;
                        if args.verbose { println!("♻️  updated: {} ({})", item.title().unwrap_or(""), link); }
                    }
                } else {
                    // Insert only new rows; ignore duplicates
                    let exec = sqlx::query!(
                        r#"
                        INSERT INTO rag.document (feed_id, source_url, source_title,
                            published_at, fetched_at, content_hash, raw_html, text_clean, status, error_msg)
                        VALUES ($1, $2, $3, $4, now(), md5($5), $6, $7, $8, $9)
                        ON CONFLICT (source_url) DO NOTHING
                        "#,
                        f.feed_id,
                        link,
                        item.title(),
                        published_at,   // Option<DateTime<Utc>>
                        text,
                        html.as_bytes(),
                        text,
                        status,
                        error_msg
                    )
                    .execute(pool)
                    .await?;
                    if exec.rows_affected() == 1 {
                        inserted += 1;
                        if args.verbose { println!("➕ inserted: {} ({})", item.title().unwrap_or(""), link); }
                    } else {
                        skipped += 1;
                        if args.verbose { println!("↩️  skipped existing: {}", item.title().unwrap_or("")); }
                    }
                }
            } else {
                // item without link — skip
                skipped += 1;
                if args.verbose { println!("↩️  skipped item with no link"); }
            }
        }

        total_inserted += inserted;
        total_updated  += updated;
        total_skipped  += skipped;
        total_errors   += errors;
        println!(
            "✅ Feed {} — inserted={} updated={} skipped={} errors={}",
            f.feed_id,
            inserted, updated, skipped, errors
        );
    }
    println!(
        "📊 Ingest totals — inserted={} updated={} skipped={} errors={}",
        total_inserted, total_updated, total_skipped, total_errors
    );
    Ok(())
}
