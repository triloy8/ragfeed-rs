use anyhow::Result;
use sqlx::PgPool;

use crate::telemetry::{self};
use crate::telemetry::ops::stats::Phase as StatsPhase;
use crate::stats::types::*;
use crate::stats::db;

pub async fn snapshot_doc(pool: &PgPool, id: i64, chunk_limit: i64) -> Result<()> {
    let log = telemetry::stats();
    let _s = log.span(&StatsPhase::DocSnapshot).entered();
    let snap = db::doc_snapshot(pool, id, chunk_limit).await?;

    log.info(format!("📄 Document {}:", snap.doc.doc_id));
    log.info(format!("  Feed ID: {:?}", snap.doc.feed_id));
    log.info(format!("  URL: {}", snap.doc.source_url));
    log.info(format!("  Title: {:?}", snap.doc.source_title));
    log.info(format!("  Published: {:?}", snap.doc.published_at));
    log.info(format!("  Fetched: {:?}", snap.doc.fetched_at));
    log.info(format!("  Status: {:?}", snap.doc.status));
    log.info(format!("  Error: {:?}", snap.doc.error_msg));
    log.info(format!("  Preview: {:?}", snap.doc.preview));

    // list chunks (IDs visible)
    if !snap.chunks.is_empty() {
        log.info(format!("  Chunks (first {}):", snap.chunks.len()));
        for r in &snap.chunks {
            log.info(format!(
                "    chunk_id={}  idx={:?}  tokens={:?}",
                r.chunk_id, r.chunk_index, r.token_count
            ));
        }
    }

    // Output envelope
    log.result(&snap)?;

    Ok(())
}
