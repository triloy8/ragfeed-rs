use anyhow::Result;
use sqlx::PgPool;

use crate::telemetry::{self};
use crate::telemetry::ops::stats::Phase as StatsPhase;
use crate::stats::types::*;
use crate::stats::db;

pub async fn summary(pool: &PgPool) -> Result<()> {
    let log = telemetry::stats();
    let _s = log.span(&StatsPhase::Summary).entered();

    // feeds listing
    log.info("📡 Feeds:");
    let feeds = db::fetch_feeds(pool).await?;
    for f in &feeds {
        log.info(format!(
            "  #{}  active={}  name={}  url={}  added_at={:?}",
            f.feed_id,
            f.is_active.unwrap_or(true),
            f.name.clone().unwrap_or_default(),
            f.url,
            f.added_at
        ));
    }

    // documents by status
    log.info("📄 Documents by status:");
    let docs = db::docs_by_status(pool).await?;
    for r in &docs {
        log.info(format!("  {:10} {}", r.status, r.cnt));
    }
    if let Ok(last) = db::last_fetched(pool).await { log.info(format!("  Last fetched: {:?}", last)); }

    // chunks summary
    if let Ok(cs) = db::chunks_summary(pool).await {
        log.info(format!("🧩 Chunks: total={} avg_tokens={:.1}", cs.total, cs.avg_tokens));
    }

    // embeddings summary
    let embeddings = db::embeddings_totals(pool).await?;
    log.info(format!("🔢 Embeddings: total={}", embeddings.total));

    // model metadata
    let models = &embeddings.models;
    match models.len() {
        0 => log.info("   Model: (none)"),
        1 => {
            let m = &models[0];
            log.info(format!("   Model: {} ({} vectors, last={:?})", m.model, m.cnt, m.last));
        }
        _ => {
            let mut labels: Vec<String> = Vec::new();
            for m in models.iter().take(3) { labels.push(format!("{} ({} )", m.model, m.cnt)); }
            if models.len() > 3 { labels.push("...".to_string()); }
            log.info(format!("   Models: {}", labels.join(", ")));
        }
    }

    // index metadata
    let idx = db::index_meta(pool).await?;
    let lists_val = idx.lists;
    let size_pretty = idx.size_pretty.clone();
    let analyze_row_last = idx.last_analyze.clone();

    let mut line = String::from("ivfflat");
    if let Some(k) = lists_val { line.push_str(&format!(" lists={}", k)); }
    if let Some(s) = size_pretty.as_deref() { line.push_str(&format!(" size={}", s)); }
    if let Some(ts) = analyze_row_last.as_ref() { line.push_str(&format!(" last_analyze={:?}", ts)); }
    log.info(format!("🧭 Index: {}", line));

    // coverage
    let cov = db::coverage(pool).await?;
    log.info(format!("📈 Coverage: {}/{} ({:.1}%)", cov.embedded, cov.chunks, cov.pct));
    log.info(format!("   Missing embeddings: {}", cov.missing));

    // JSON envelope
    if telemetry::config::json_mode() {
        let feeds_out = feeds;
        let docs_out = docs;
        let last_fetched = db::last_fetched(pool).await?;
        let chunks_out = db::chunks_summary(pool).await?;
        let embeddings_out = db::embeddings_totals(pool).await?;
        let index_out = db::index_meta(pool).await?;
        let coverage_out = db::coverage(pool).await?;
        let result = StatsSummary { feeds: feeds_out, documents_by_status: docs_out, last_fetched, chunks: chunks_out, embeddings: embeddings_out, index: index_out, coverage: coverage_out };
        log.result(&result)?;
    }

    Ok(())
}
