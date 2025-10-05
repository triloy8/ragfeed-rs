use anyhow::Result;
use sqlx::PgPool;

use crate::telemetry::{self};
use crate::telemetry::ops::stats::Phase as StatsPhase;
use crate::stats::db;

pub async fn snapshot_chunk(pool: &PgPool, id: i64) -> Result<()> {
    let log = telemetry::stats();
    let _s = log.span(&StatsPhase::ChunkSnapshot).entered();
    let row = db::chunk_snap(pool, id).await?;

    log.info(format!("🧩 Chunk {} (Doc {:?}):", row.chunk_id, row.doc_id));
    log.info(format!("  Index: {:?}", row.chunk_index));
    log.info(format!("  Tokens: {:?}", row.token_count));
    log.info(format!("  Preview: {:?}", row.preview));

    log.result(&row)?;

    Ok(())
}
