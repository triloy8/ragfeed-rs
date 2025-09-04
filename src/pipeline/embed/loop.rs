use anyhow::{bail, Result};
use sqlx::PgPool;

use crate::encoder::traits::Embedder;
use crate::telemetry::{self};
use crate::telemetry::ops::embed::Phase as EmbedPhase;

use super::db;

pub async fn embed_force_once(
    pool: &PgPool,
    encoder: &mut dyn Embedder,
    model_tag: &str,
    dim_expect: usize,
    batch: usize,
    max: Option<i64>,
) -> Result<i64> {
    let log = telemetry::embed();
    let rows = { let _fb = log.span(&EmbedPhase::FetchBatch).entered(); db::fetch_all_chunks(pool, max).await? };
    if rows.is_empty() { return Ok(0); }

    let mut total = 0i64;
    for chunk in rows.chunks(batch) {
        let chunk_ids: Vec<i64> = chunk.iter().map(|(id, _)| *id).collect();
        let texts: Vec<String> = chunk.iter().map(|(_, t)| t.clone()).collect();

        let _enc = log.span(&EmbedPhase::Encode).entered();
        let embeddings = encoder.embed_passages(&texts)?;
        drop(_enc);

        let dim = embeddings.get(0).map(|v| v.len()).unwrap_or(0);
        if dim == 0 { bail!("empty embedding dimension"); }
        if dim as i32 != dim_expect as i32 { bail!("model produced dim={} but --dim={} was specified", dim, dim_expect); }

        for (chunk_id, vec) in chunk_ids.into_iter().zip(embeddings.into_iter()) {
            let _ins = log.span(&EmbedPhase::InsertEmbedding).entered();
            db::insert_embedding(pool, chunk_id, model_tag, dim_expect as i32, vec).await?;
            drop(_ins);
        }

        total += texts.len() as i64;
        log.info(format!("✅ embedded {} chunk(s) (total={})", texts.len(), total));
    }
    Ok(total)
}

pub async fn embed_missing_paged(
    pool: &PgPool,
    encoder: &mut dyn Embedder,
    model_tag: &str,
    dim_expect: usize,
    batch: usize,
    max: Option<i64>,
) -> Result<i64> {
    let log = telemetry::embed();
    let mut total = 0i64;
    let mut remaining = max.unwrap_or(i64::MAX);
    loop {
        let n = remaining.min(batch as i64) as i64;
        if n <= 0 { break; }

        let rows = { let _fb = log.span(&EmbedPhase::FetchBatch).entered(); db::fetch_chunks(pool, model_tag, false, n).await? };
        if rows.is_empty() { break; }

        let chunk_ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
        let texts: Vec<String> = rows.into_iter().map(|(_, t)| t).collect();

        let _enc = log.span(&EmbedPhase::Encode).entered();
        let embeddings = encoder.embed_passages(&texts)?;
        drop(_enc);

        let dim = embeddings.get(0).map(|v| v.len()).unwrap_or(0);
        if dim == 0 { bail!("empty embedding dimension"); }
        if dim as i32 != dim_expect as i32 { bail!("model produced dim={} but --dim={} was specified", dim, dim_expect); }

        for (chunk_id, vec) in chunk_ids.into_iter().zip(embeddings.into_iter()) {
            let _ins = log.span(&EmbedPhase::InsertEmbedding).entered();
            db::insert_embedding(pool, chunk_id, model_tag, dim_expect as i32, vec).await?;
            drop(_ins);
        }

        total += texts.len() as i64;
        remaining -= n;
        log.info(format!("✅ embedded {} chunk(s) (total={})", texts.len(), total));
    }
    Ok(total)
}
