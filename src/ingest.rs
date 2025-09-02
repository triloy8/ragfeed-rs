use anyhow::Result;
use sqlx::PgPool;
use rss::Channel;
use reqwest::Client;
use chrono::Utc;
use url::Url;
use serde::Serialize;

use crate::out::{self};
use crate::out::ingest::{Phase as IngestPhase};

use crate::extractor;

#[derive(clap::Args)]
pub struct IngestCmd {
    #[arg(long)] feed: Option<i32>,
    #[arg(long)] feed_url: Option<String>,
    #[arg(long, default_value_t=200)] limit: usize,
    #[arg(long)] force_refetch: bool,
    #[arg(long, default_value_t=false)] apply: bool, // default is plan-only (no network, no writes)
    #[arg(long, default_value_t=10)] plan_limit: usize, // how many feeds to list in plan
}

pub async fn run(pool: &PgPool, args: IngestCmd) -> Result<()> {
    let log = out::ingest();
    let _g = log
        .root_span_kv([
            ("apply", args.apply.to_string()),
            ("limit", (args.limit as i64).to_string()),
            ("plan_limit", (args.plan_limit as i64).to_string()),
            ("force_refetch", args.force_refetch.to_string()),
            ("feed", format!("{:?}", args.feed)),
            ("feed_url", format!("{:?}", args.feed_url)),
        ])
        .entered();
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
        if out::json_mode() {
            #[derive(Serialize)]
            struct FeedSample { feed_id: i32, url: String, name: Option<String> }
            #[derive(Serialize)]
            struct IngestPlan { feeds: usize, mode: String, limit: usize, sample_feeds: Vec<FeedSample> }
            let samples: Vec<FeedSample> = feeds
                .iter()
                .take(args.plan_limit)
                .map(|f| FeedSample { feed_id: f.feed_id, url: f.url.clone(), name: f.name.clone() })
                .collect();
            let plan = IngestPlan { feeds: feeds.len(), mode: mode.to_string(), limit: args.limit, sample_feeds: samples };
            log.plan(&plan)?;
        } else {
            log.info(format!("📝 Ingest plan — feeds={} mode={} limit={}", feeds.len(), mode, args.limit));
            for f in feeds.iter().take(args.plan_limit) {
                log.info(format!("  feed_id={} url={} name={:?}", f.feed_id, f.url, f.name));
            }
            if feeds.len() > args.plan_limit {
                log.info(format!("  ... ({} more)", feeds.len() - args.plan_limit));
            }
            log.info("   Use --apply to fetch and write.");
        }
        return Ok(());
    }

    // APPLY: perform network calls and writes
    let client = Client::new();

    // fetch + parse each feed
    let mut total_inserted = 0usize;
    let mut total_updated = 0usize;
    let mut total_skipped = 0usize;
    let mut total_errors  = 0usize;

    #[derive(Serialize)]
    struct FeedSummary { feed_id: i32, inserted: usize, updated: usize, skipped: usize, errors: usize }
    let mut per_feed: Vec<FeedSummary> = Vec::new();

    for f in feeds {
        let _feed_span = log.span_kv(&IngestPhase::Feed, [("feed_id", f.feed_id.to_string()), ("url", f.url.clone())]).entered();
        let mut inserted = 0usize;
        let mut updated  = 0usize;
        let mut skipped  = 0usize;
        let mut errors   = 0usize;

        let _rss_span = log.span(&IngestPhase::FetchRss).entered();
        let xml = client.get(&f.url).send().await?.bytes().await?;
        drop(_rss_span);
        let _parse_span = log.span(&IngestPhase::ParseRss).entered();
        let channel = Channel::read_from(&xml[..])?;
        drop(_parse_span);

        for item in channel.items().iter().take(args.limit) {
            if let Some(link) = item.link() {
                // fetch article
                let _fetch_span = log.span_kv(&IngestPhase::FetchItem, [("url", link.to_string())]).entered();
                let html = client.get(link).send().await?.text().await?;
                drop(_fetch_span);

                // per-host extraction with fallback
                let host = Url::parse(link).ok().and_then(|u| u.host_str().map(|s| s.to_string())).unwrap_or_default();
                let _extract_span = log.span_kv(&IngestPhase::Extract, [("host", host.clone())]).entered();
                let extracted = extractor::extract(&host, &html);
                drop(_extract_span);
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
                    let _write_span = log.span_kv(&IngestPhase::WriteDoc, [("mode", "upsert".to_string())]).entered();
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
                    if res.inserted.unwrap_or(false) { inserted += 1; log.info_kv("➕ insert", [("url", link.to_string()), ("title", item.title().unwrap_or("").to_string())]); }
                    else { updated += 1; log.info_kv("♻️ update", [("url", link.to_string()), ("title", item.title().unwrap_or("").to_string())]); }
                    drop(_write_span);
                } else {
                    // Insert only new rows; ignore duplicates
                    let _write_span = log.span_kv(&IngestPhase::WriteDoc, [("mode", "insert".to_string())]).entered();
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
                        log.info_kv("➕ insert", [("url", link.to_string()), ("title", item.title().unwrap_or("").to_string())]);
                    } else {
                        skipped += 1;
                        log.info_kv("↩️ skip", [("title", item.title().unwrap_or("").to_string())]);
                    }
                    drop(_write_span);
                }
            } else {
                // item without link — skip
                skipped += 1;
                log.info_kv("↩️ skip", [("reason", "no-link".to_string())]);
            }
        }

        total_inserted += inserted;
        total_updated  += updated;
        total_skipped  += skipped;
        total_errors   += errors;
        log.feed_summary(f.feed_id, inserted, updated, skipped, errors);
        per_feed.push(FeedSummary { feed_id: f.feed_id, inserted, updated, skipped, errors });
    }
    // Always log human-readable totals at info level
    log.totals(total_inserted, total_updated, total_skipped, total_errors);

    // Optionally emit the machine-readable result envelope to stdout
    if out::json_mode() {
        #[derive(Serialize)]
        struct IngestTotals { inserted: usize, updated: usize, skipped: usize, errors: usize }
        #[derive(Serialize)]
        struct IngestApply { totals: IngestTotals, per_feed: Vec<FeedSummary> }
        let result = IngestApply {
            totals: IngestTotals { inserted: total_inserted, updated: total_updated, skipped: total_skipped, errors: total_errors },
            per_feed,
        };
        log.result(&result)?;
    }
    Ok(())
}
