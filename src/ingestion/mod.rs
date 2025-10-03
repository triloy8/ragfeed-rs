use anyhow::Result;
use clap::Args;
use serde::Serialize;
use sqlx::PgPool;
use reqwest::Client;
use chrono::{DateTime, Utc};
use url::Url;

use crate::telemetry::{self};
use crate::telemetry::ops::ingest::Phase as IngestPhase;

mod fetch;
mod parse;
mod write;
mod types;
mod db;
pub mod extractor;

#[derive(Args)]
pub struct IngestCmd {
    #[arg(long)] pub feed: Option<i32>,
    #[arg(long)] pub feed_url: Option<String>,
    #[arg(long, default_value_t=200)] pub limit: usize,
    #[arg(long)] pub force_refetch: bool,
    #[arg(long, default_value_t=false)] pub apply: bool,
    #[arg(long, default_value_t=10)] pub plan_limit: usize,
}

pub async fn run(pool: &PgPool, args: IngestCmd) -> Result<()> {
    let log = telemetry::ingest();
    let _g = log.root_span_kv([
        ("apply", args.apply.to_string()),
        ("limit", (args.limit as i64).to_string()),
        ("plan_limit", (args.plan_limit as i64).to_string()),
        ("force_refetch", args.force_refetch.to_string()),
        ("feed", format!("{:?}", args.feed)),
        ("feed_url", format!("{:?}", args.feed_url)),
    ]).entered();

    // resolve feeds to process
    let feeds = db::select_feeds(pool, args.feed, args.feed_url.as_deref()).await?;

    if !args.apply {
        let mode = if args.force_refetch { "upsert" } else { "insert-only" };
        if telemetry::config::json_mode() {
            use types::{FeedSample, IngestPlan};
            let samples: Vec<FeedSample> = feeds.iter().take(args.plan_limit)
                .map(|f| FeedSample { feed_id: f.feed_id, url: f.url.clone(), name: f.name.clone() })
                .collect();
            let plan = IngestPlan { feeds: feeds.len(), mode: mode.to_string(), limit: args.limit, sample_feeds: samples };
            log.plan(&plan)?;
        } else {
            log.info(format!("📝 Ingest plan — feeds={} mode={} limit={}", feeds.len(), mode, args.limit));
            for f in feeds.iter().take(args.plan_limit) { log.info(format!("  feed_id={} url={} name={:?}", f.feed_id, f.url, f.name)); }
            if feeds.len() > args.plan_limit { log.info(format!("  ... ({} more)", feeds.len() - args.plan_limit)); }
            log.info("   Use --apply to execute.");
        }
        return Ok(());
    }

    let client = Client::new();

    let mut total_inserted = 0usize;
    let mut total_updated = 0usize;
    let mut total_skipped = 0usize;
    let mut total_errors  = 0usize;

    use types::FeedSummary;
    let mut per_feed: Vec<FeedSummary> = Vec::new();

    for f in feeds {
        let _feed_span = log.span_kv(&IngestPhase::Feed, [("feed_id", f.feed_id.to_string()), ("url", f.url.clone())]).entered();
        let mut inserted = 0usize;
        let mut updated  = 0usize;
        let mut skipped  = 0usize;
        let mut errors   = 0usize;

        // fetch and parse RSS channel
        let xml = { let _s = log.span(&IngestPhase::FetchRss).entered(); fetch::fetch_rss(&client, &f.url).await? };
        let channel = { let _s = log.span(&IngestPhase::ParseRss).entered(); parse::parse_channel(&xml)? };

        for item in channel.items().iter().take(args.limit) {
            if let Some(link) = item.link() {
                // fetch article
                let html = { let _s = log.span_kv(&IngestPhase::FetchItem, [("url", link.to_string())]).entered(); fetch::fetch_article(&client, link).await? };

                // per-host extraction with fallback
                let host = Url::parse(link).ok().and_then(|u| u.host_str().map(|s| s.to_string())).unwrap_or_default();
                let extracted = { let _s = log.span_kv(&IngestPhase::Extract, [("host", host.clone())]).entered(); extractor::extract(&host, &html) };
                let (text, status, error_msg) = match extracted {
                    Some(t) if !t.trim().is_empty() => (t, "ingest", None),
                    _ => ("".to_string(), "error", Some("extract-failed".to_string())),
                };

                let published_at: Option<DateTime<Utc>> = parse::extract_published_at(item);

                if args.force_refetch {
                    let _ws = log.span_kv(&IngestPhase::WriteDoc, [("mode", "upsert".to_string())]).entered();
                    let inserted_row = write::upsert_document(pool, f.feed_id, link, item.title(), published_at, &text, html.as_bytes(), status, error_msg.as_deref()).await?;
                    if inserted_row { inserted += 1; log.info_kv("➕ insert", [("url", link.to_string()), ("title", item.title().unwrap_or("").to_string())]); }
                    else { updated += 1; log.info_kv("♻️ update", [("url", link.to_string()), ("title", item.title().unwrap_or("").to_string())]); }
                } else {
                    let _ws = log.span_kv(&IngestPhase::WriteDoc, [("mode", "insert".to_string())]).entered();
                    let did_insert = write::insert_document(pool, f.feed_id, link, item.title(), published_at, &text, html.as_bytes(), status, error_msg.as_deref()).await?;
                    if did_insert { inserted += 1; log.info_kv("➕ insert", [("url", link.to_string()), ("title", item.title().unwrap_or("").to_string())]); }
                    else { skipped += 1; log.info_kv("↩️ skip", [("title", item.title().unwrap_or("").to_string())]); }
                }
            } else {
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

    log.totals(total_inserted, total_updated, total_skipped, total_errors);

    if telemetry::config::json_mode() {
        use types::{IngestTotals, IngestApply};
        let result = IngestApply {
            totals: IngestTotals { inserted: total_inserted, updated: total_updated, skipped: total_skipped, errors: total_errors },
            per_feed,
        };
        log.result(&result)?;
    }
    Ok(())
}
