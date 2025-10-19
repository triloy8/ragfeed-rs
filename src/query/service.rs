use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use sqlx::{Acquire, PgPool};
use std::collections::HashMap;
use tracing::span::EnteredSpan;

use crate::encoder::{traits::Embedder, Device, E5Encoder};
use crate::telemetry::ctx::LogCtx;
use crate::telemetry::ops::query::{Phase as QueryPhase, Query as QueryOp};

use super::db::{self, CandRow, FetchOpts};
use super::post;
use super::QueryResultRow;

pub struct QueryRequest<'a> {
    pub query: &'a str,
    pub top_n: i64,
    pub topk: usize,
    pub doc_cap: usize,
    pub probes: Option<i32>,
    pub feed: Option<i32>,
    pub since: Option<DateTime<Utc>>,
    pub include_preview: bool,
    pub include_text: bool,
    pub model_id: &'a str,
    pub onnx_filename: Option<&'a str>,
    pub device: Device,
}

pub struct QueryHit {
    pub rank: usize,
    pub distance: f32,
    pub chunk_id: i64,
    pub doc_id: i64,
    pub title: Option<String>,
    pub preview: Option<String>,
    pub text: Option<String>,
}

pub struct QueryOutcome {
    pub rows: Vec<QueryResultRow>,
    pub hits: Vec<QueryHit>,
    pub probes: Option<i32>,
}

pub async fn execute(
    pool: &PgPool,
    req: QueryRequest<'_>,
    log: Option<&LogCtx<QueryOp>>,
) -> Result<QueryOutcome> {
    // ensure embeddings exist to learn dim
    let _prepare_span = enter_span(log, &QueryPhase::Prepare);
    let dim_row = sqlx::query!("SELECT dim FROM rag.embedding LIMIT 1")
        .fetch_optional(pool)
        .await?;
    if dim_row.is_none() {
        if let Some(ctx) = log {
            ctx.info("ℹ️  No embeddings found. Run `rag embed` first.");
        }
        return Ok(QueryOutcome { rows: Vec::new(), hits: Vec::new(), probes: None });
    }
    let db_dim = dim_row.unwrap().dim as usize;
    drop(_prepare_span);

    // build encoder and embed the query
    let _encoder_span = enter_span(log, &QueryPhase::Prepare);
    let mut enc: Box<dyn Embedder> = Box::new(
        E5Encoder::new(req.model_id, req.onnx_filename, req.device).context("init encoder")?,
    );
    drop(_encoder_span);

    let _embed_span = enter_span(log, &QueryPhase::EmbedQuery);
    let qvec = enc.embed_query(req.query).context("embed query")?;
    if qvec.len() != db_dim {
        bail!("query embedding dim={} != DB dim={}", qvec.len(), db_dim);
    }
    drop(_embed_span);

    // set probes
    let probes = match req.probes {
        Some(p) => Some(p.max(1)),
        None => db::recommend_probes(pool).await?,
    };
    let mut conn = pool.acquire().await?;
    let mut tx = conn.begin().await?;

    if let Some(p) = probes {
        let _set_probes_span = enter_span(log, &QueryPhase::SetProbes);
        let sql = format!("SET LOCAL ivfflat.probes = {}", p);
        sqlx::query(&sql).execute(&mut *tx).await?;
        drop(_set_probes_span);
    }

    let _fetch_span = enter_span(log, &QueryPhase::FetchCandidates);
    let candidates = db::fetch_ann_candidates(
        &mut *tx,
        &qvec,
        req.top_n.max(1),
        &FetchOpts {
            feed: req.feed,
            since: req.since,
            include_preview: req.include_preview,
            include_text: req.include_text,
        },
    )
    .await?;
    drop(_fetch_span);

    tx.commit().await?;

    if candidates.is_empty() {
        if let Some(ctx) = log {
            ctx.info("ℹ️  No results");
        }
        return Ok(QueryOutcome { rows: Vec::new(), hits: Vec::new(), probes });
    }

    let _post_span = enter_span(log, &QueryPhase::PostFilter);
    let shaped_rows: Vec<QueryResultRow> =
        post::shape_results(candidates.clone(), req.topk, req.doc_cap);
    drop(_post_span);

    let mut by_chunk: HashMap<i64, CandRow> = HashMap::new();
    for cand in candidates {
        by_chunk.insert(cand.chunk_id, cand);
    }

    let hits = build_hits(&shaped_rows, &by_chunk);

    Ok(QueryOutcome { rows: shaped_rows, hits, probes })
}

fn enter_span<'a>(
    log: Option<&'a LogCtx<QueryOp>>,
    phase: &QueryPhase,
) -> Option<EnteredSpan> {
    log.map(|ctx| ctx.span(phase).entered())
}

fn build_hits(rows: &[QueryResultRow], candidates: &HashMap<i64, CandRow>) -> Vec<QueryHit> {
    rows.iter()
        .filter_map(|row| {
            candidates.get(&row.chunk_id).map(|cand| QueryHit {
                rank: row.rank,
                distance: row.distance,
                chunk_id: row.chunk_id,
                doc_id: row.doc_id,
                title: row.title.clone(),
                preview: row.preview.clone(),
                text: cand.text.clone(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::db::CandRow;

    #[test]
    fn build_hits_includes_chunk_text() {
        let rows = vec![QueryResultRow {
            rank: 1,
            distance: 0.12,
            chunk_id: 42,
            doc_id: 7,
            title: Some("Doc".into()),
            preview: Some("prev".into()),
        }];
        let mut candidates = HashMap::new();
        candidates.insert(
            42,
            CandRow {
                chunk_id: 42,
                doc_id: 7,
                title: Some("Doc".into()),
                preview: Some("prev".into()),
                text: Some("full text".into()),
                distance: 0.12,
            },
        );

        let hits = build_hits(&rows, &candidates);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].text.as_deref(), Some("full text"));
        assert_eq!(hits[0].rank, 1);
    }
}
