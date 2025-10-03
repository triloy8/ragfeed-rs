use anyhow::Result;
use sqlx::PgPool;

use crate::telemetry::{self};
use crate::telemetry::ops::stats::Phase as StatsPhase;
use crate::stats::types::*;
use crate::stats::db;

pub async fn feed_stats(pool: &PgPool, feed_id: i32, doc_limit: i64) -> Result<()> {
    let log = telemetry::stats();
    let _s = log.span(&StatsPhase::FeedStats).entered();

    // feed header
    let f = db::feed_header(pool, feed_id).await?;
    log.info(format!("📡 Feed #{}:", f.feed_id));
    log.info(format!("  Name: {}", f.name.clone().unwrap_or_default()));
    log.info(format!("  URL: {}", f.url));
    log.info(format!("  Active: {}", f.is_active.unwrap_or(true)));
    log.info(format!("  Added: {:?}", f.added_at));

    // documents by status within this feed
    log.info("📄 Documents by status:");
    let docs = db::feed_docs_by_status(pool, feed_id).await?;
    for r in &docs { log.info(format!("  {:10} {}", r.status, r.cnt)); }
    if let Ok(last) = db::feed_last_fetched(pool, feed_id).await { log.info(format!("  Last fetched: {:?}", last)); }

    // chunks for this feed
    if let Ok(cs) = db::feed_chunks_summary(pool, feed_id).await { log.info(format!("🧩 Chunks: total={} avg_tokens={:.1}", cs.total, cs.avg_tokens)); }

    // embedding coverage for this feed
    let cov = db::feed_coverage(pool, feed_id).await?;
    log.info(format!("📈 Coverage: {}/{} ({:.1}%)  last_embedded={:?}", cov.embedded, cov.chunks, cov.pct, cov.last));

    // missing per-feed
    let missing = db::feed_missing_count(pool, feed_id).await?;
    log.info(format!("   Missing embeddings: {}", missing));

    // model(s) present for this feed
    let feed_models = db::feed_models(pool, feed_id).await?;
    match feed_models.len() {
        0 => log.info("   Model: (none)"),
        1 => {
            let m = &feed_models[0];
            log.info(format!("   Model: {} ({} vectors, last={:?})", m.model, m.cnt, m.last));
        }
        _ => {
            let mut labels: Vec<String> = Vec::new();
            for m in feed_models.iter().take(3) {
                labels.push(format!("{} ({} )", m.model, m.cnt));
            }
            if feed_models.len() > 3 { labels.push("...".to_string()); }
            log.info(format!("   Models: {}", labels.join(", ")));
        }
    }

    // top documents in this feed with pending embeddings
    if missing > 0 {
        log.info("   Top docs with pending embeddings:");
        let rows = db::feed_pending_top_docs(pool, feed_id, 10).await?;
        for r in rows {
            log.info(format!("     {:>6}  doc={}  {}", r.pending, r.doc_id, r.source_title.unwrap_or_default()));
        }
    }

    // latest docs (IDs visible)
    let rows = db::latest_docs(pool, feed_id, doc_limit).await?;
    if !rows.is_empty() {
        log.info(format!("📜 Docs (latest {}):", rows.len()));
        for r in rows {
            log.info(format!(
                "  doc_id={}  status={}  fetched={:?}  {}",
                r.doc_id,
                r.status.clone().unwrap_or_default(),
                r.fetched_at,
                r.source_title.clone().unwrap_or_default()
            ));
        }
    }

    // JSON envelope
    if telemetry::config::json_mode() {
        let last_fetched = db::feed_last_fetched(pool, feed_id).await?;
        let chunks = db::feed_chunks_summary(pool, feed_id).await?;
        let models = db::feed_models(pool, feed_id).await?;
        let pending_top_docs = db::feed_pending_top_docs(pool, feed_id, 10).await?;
        let latest_docs_rows = db::latest_docs(pool, feed_id, doc_limit).await?;

        let result = StatsFeedStats {
            feed: f,
            documents_by_status: docs,
            last_fetched,
            chunks,
            coverage: cov,
            missing,
            models,
            pending_top_docs,
            latest_docs: latest_docs_rows,
        };
        log.result(&result)?;
    }

    Ok(())
}
