use anyhow::Result;
use sqlx::PgPool;

use crate::out::{self};
use crate::out::stats::Phase as StatsPhase;
use crate::stats::types::*;

pub async fn snapshot_doc(pool: &PgPool, id: i64, chunk_limit: i64) -> Result<()> {
    let log = out::stats();
    let _s = log.span(&StatsPhase::DocSnapshot).entered();
    let row = sqlx::query!(
        r#"
        SELECT doc_id, feed_id, source_url, source_title, published_at,
               fetched_at, status, error_msg,
               substring(text_clean, 1, 400) AS preview
        FROM rag.document
        WHERE doc_id = $1
        "#,
        id
    )
    .fetch_one(pool)
    .await?;

    log.info(format!("📄 Document {}:", row.doc_id));
    log.info(format!("  Feed ID: {:?}", row.feed_id));
    log.info(format!("  URL: {}", row.source_url));
    log.info(format!("  Title: {:?}", row.source_title));
    log.info(format!("  Published: {:?}", row.published_at));
    log.info(format!("  Fetched: {:?}", row.fetched_at));
    log.info(format!("  Status: {:?}", row.status));
    log.info(format!("  Error: {:?}", row.error_msg));
    log.info(format!("  Preview: {:?}", row.preview));

    // list chunks (IDs visible)
    let rows = sqlx::query!(
        r#"
        SELECT chunk_id, chunk_index, token_count
        FROM rag.chunk
        WHERE doc_id = $1
        ORDER BY chunk_index ASC
        LIMIT $2
        "#,
        id,
        chunk_limit
    )
    .fetch_all(pool)
    .await?;
    if !rows.is_empty() {
        log.info(format!("  Chunks (first {}):", rows.len()));
        for r in &rows {
            log.info(format!(
                "    chunk_id={}  idx={:?}  tokens={:?}",
                r.chunk_id, r.chunk_index, r.token_count
            ));
        }
    }

    // JSON envelope
    if out::json_mode() {
        let doc = StatsDocInfo {
            doc_id: row.doc_id,
            feed_id: row.feed_id,
            source_url: row.source_url,
            source_title: row.source_title,
            published_at: row.published_at,
            fetched_at: row.fetched_at,
            status: row.status,
            error_msg: row.error_msg,
            preview: row.preview,
        };
        let chunks: Vec<StatsDocChunkInfo> = rows
            .into_iter()
            .map(|r| StatsDocChunkInfo { chunk_id: r.chunk_id, chunk_index: r.chunk_index, token_count: r.token_count })
            .collect();
        log.result(&StatsDocSnapshot { doc, chunks })?;
    }

    Ok(())
}
