use anyhow::{bail, Context, Result};
use clap::Args;
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::PgPool;

use crate::encoder::{Device, E5Encoder};
use crate::encoder::traits::Embedder;
use crate::util::time::parse_since_opt;

use crate::telemetry::{self};
use crate::telemetry::ops::query::Phase as QueryPhase;

mod db;
mod post;

pub use post::QueryResultRow;

#[derive(Args, Debug)]
pub struct QueryCmd {
    query: String,
    #[arg(long, default_value_t = 100)] top_n: i64,
    #[arg(long, default_value_t = 6)] topk: usize,
    #[arg(long, default_value_t = 2)] doc_cap: usize,
    #[arg(long)] probes: Option<i32>,
    #[arg(long)] feed: Option<i32>,
    #[arg(long)] since: Option<String>,
    #[arg(long, default_value_t = false)] show_context: bool,

    // E5Encoder config
    #[arg(long, default_value = "intfloat/e5-small-v2")] pub model_id: String,
    #[arg(long)] pub onnx_filename: Option<String>,
    #[arg(long, value_enum, default_value_t = Device::Cpu)] pub device: Device,
}

pub async fn run(pool: &PgPool, args: QueryCmd) -> Result<()> {
    let log = telemetry::query();
    let _g = log
        .root_span_kv([
            ("top_n", args.top_n.to_string()),
            ("topk", args.topk.to_string()),
            ("doc_cap", args.doc_cap.to_string()),
            ("probes", format!("{:?}", args.probes)),
            ("feed", format!("{:?}", args.feed)),
            ("since", format!("{:?}", args.since)),
            ("show_context", args.show_context.to_string()),
            ("json", telemetry::config::json_mode().to_string()),
            ("model_id", args.model_id.clone()),
            ("device", format!("{:?}", args.device)),
        ])
        .entered();

    // ensure embeddings exist to learn dim
    let _prep = log.span(&QueryPhase::Prepare).entered();
    let dim_row = sqlx::query!("SELECT dim FROM rag.embedding LIMIT 1")
        .fetch_optional(pool)
        .await?;
    if dim_row.is_none() {
        log.info("ℹ️  No embeddings found. Run `rag embed` first.");
        return Ok(());
    }
    let db_dim = dim_row.unwrap().dim as usize;

    // build encoder and embed the query
    let mut enc: Box<dyn Embedder> = {
        let _s = log.span(&QueryPhase::Prepare).entered();
        Box::new(E5Encoder::new(&args.model_id, args.onnx_filename.as_deref(), args.device)
            .context("init encoder")?)
    };
    let qvec = {
        let _s = log.span(&QueryPhase::EmbedQuery).entered();
        enc.embed_query(&args.query).context("embed query")?
    };
    if qvec.len() != db_dim {
        bail!("query embedding dim={} != DB dim={}", qvec.len(), db_dim);
    }

    // set probes
    let probes = match args.probes {
        Some(p) => Some(p.max(1)),
        None => db::recommend_probes(pool).await?,
    };
    if let Some(p) = probes {
        let _s = log.span(&QueryPhase::SetProbes).entered();
        let sql = format!("SET LOCAL ivfflat.probes = {}", p);
        sqlx::query(&sql).execute(pool).await?;
    }

    // filters
    let since_ts: Option<DateTime<Utc>> = parse_since_opt(&args.since)?;

    // fetch ANN candidates
    let _fetch = log.span(&QueryPhase::FetchCandidates).entered();
    let candidates = db::fetch_ann_candidates(
        pool,
        &qvec,
        args.top_n.max(1),
        args.feed,
        since_ts,
        args.show_context,
    )
    .await?;
    drop(_fetch);

    if candidates.is_empty() {
        log.info("ℹ️  No results");
        return Ok(());
    }

    // post-filter and format
    let _pf = log.span(&QueryPhase::PostFilter).entered();
    let out_rows: Vec<QueryResultRow> = post::shape_results(candidates, args.topk, args.doc_cap);
    drop(_pf);

    // output
    let _out_span = log.span(&QueryPhase::Output).entered();
    if telemetry::config::json_mode() {
        log.result(&out_rows)?;
    } else {
        log.info("🔍 Results:");
        for r in &out_rows {
            log.info(format!(
                "#{}  dist={:.4}  chunk={} doc={}  {:?}",
                r.rank, r.distance, r.chunk_id, r.doc_id, r.title
            ));
            if args.show_context {
                if let Some(p) = &r.preview { log.info(format!("  {}", p.replace('\n', " "))); }
            }
        }
    }

    Ok(())
}
