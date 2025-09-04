use anyhow::Result;
use sqlx::PgPool;

use crate::telemetry::{self};
use crate::telemetry::ops::stats::Phase as StatsPhase;
use crate::stats::types::StatsChunkSnap;

pub async fn snapshot_chunk(pool: &PgPool, id: i64) -> Result<()> {
    let log = telemetry::stats();
    let _s = log.span(&StatsPhase::ChunkSnapshot).entered();
    let row = sqlx::query!(
        r#"
        SELECT chunk_id, doc_id, chunk_index, token_count,
               substring(text, 1, 400) AS preview
        FROM rag.chunk
        WHERE chunk_id = $1
        "#,
        id
    )
    .fetch_one(pool)
    .await?;

    log.info(format!("🧩 Chunk {} (Doc {:?}):", row.chunk_id, row.doc_id));
    log.info(format!("  Index: {:?}", row.chunk_index));
    log.info(format!("  Tokens: {:?}", row.token_count));
    log.info(format!("  Preview: {:?}", row.preview));

    if telemetry::config::json_mode() {
        log.result(&StatsChunkSnap {
            chunk_id: row.chunk_id,
            doc_id: row.doc_id,
            chunk_index: row.chunk_index,
            token_count: row.token_count,
            preview: row.preview,
        })?;
    }

    Ok(())
}
