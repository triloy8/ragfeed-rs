use std::fmt;

use anyhow::{bail, Context, Result};
use clap::Args;
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::PgPool;
use tokio::select;
use tokio_util::sync::CancellationToken;

use crate::encoder::{Device, E5Encoder};
use crate::encoder::traits::Embedder;
use crate::util::time::parse_since_opt;

use crate::telemetry::{self};

mod db;
mod post;

pub use post::QueryResultRow;

#[derive(Args, Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct QueryRequest {
    pub text: String,
    pub top_n: i64,
    pub topk: usize,
    pub doc_cap: usize,
    pub probes: Option<i32>,
    pub feed: Option<i32>,
    pub since: Option<String>,
    pub show_context: bool,
    pub model_id: String,
    pub onnx_filename: Option<String>,
    pub device: Device,
}

impl From<QueryCmd> for QueryRequest {
    fn from(cmd: QueryCmd) -> Self {
        QueryRequest {
            text: cmd.query,
            top_n: cmd.top_n,
            topk: cmd.topk,
            doc_cap: cmd.doc_cap,
            probes: cmd.probes,
            feed: cmd.feed,
            since: cmd.since,
            show_context: cmd.show_context,
            model_id: cmd.model_id,
            onnx_filename: cmd.onnx_filename,
            device: cmd.device,
        }
    }
}

#[cfg(feature = "mcp-server")]
impl QueryRequest {
    pub fn from_mcp_params(params: &crate::mcp::types::QueryRunParams) -> Self {
        QueryRequest {
            text: params.text.clone(),
            top_n: params.top_n.unwrap_or(100) as i64,
            topk: params.topk.unwrap_or(6) as usize,
            doc_cap: params.doc_cap.unwrap_or(2) as usize,
            probes: params.probes,
            feed: params.feed,
            since: params.since.clone(),
            show_context: params.show_context.unwrap_or(false),
            model_id: "intfloat/e5-small-v2".to_string(),
            onnx_filename: None,
            device: Device::Cpu,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryPlanSummary {
    pub top_n: i64,
    pub topk: usize,
    pub doc_cap: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub probes_requested: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub probes_applied: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub feed: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub since: Option<String>,
    pub show_context: bool,
    pub embeddings_available: bool,
    pub model_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryExecutionResult {
    pub plan: QueryPlanSummary,
    pub rows: Vec<QueryResultRow>,
}

#[derive(Debug, Clone)]
pub struct QueryCancelled;

impl fmt::Display for QueryCancelled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "query cancelled")
    }
}

impl std::error::Error for QueryCancelled {}

pub fn is_cancelled_error(err: &anyhow::Error) -> bool {
    err.downcast_ref::<QueryCancelled>().is_some()
}

pub async fn run(pool: &PgPool, args: QueryCmd) -> Result<()> {
    let log = telemetry::query();
    log.info(format!(
        "🔍 query top_n={} topk={} doc_cap={}, probes={:?}, feed={:?}, since={:?}, show_context={}, model={}",
        args.top_n, args.topk, args.doc_cap, args.probes, args.feed, args.since, args.show_context, args.model_id
    ));

    let req: QueryRequest = args.clone().into();
    let execution = execute_query(pool, req, None).await?;

    log.plan(&execution.plan)?;

    if !execution.plan.embeddings_available {
        log.info("ℹ️  No embeddings found. Run `rag embed` first.");
        return Ok(());
    }

    if execution.rows.is_empty() {
        log.info("ℹ️  No results");
        return Ok(());
    }

    log.info("🔍 Results:");
    for r in &execution.rows {
        log.info(format!(
            "#{}  dist={:.4}  chunk={} doc={}  {:?}",
            r.rank, r.distance, r.chunk_id, r.doc_id, r.title
        ));
        if execution.plan.show_context {
            if let Some(p) = &r.preview { log.info(format!("  {}", p.replace('\n', " "))); }
        }
    }
    log.result(&execution.rows)?;

    Ok(())
}

pub async fn execute_query(
    pool: &PgPool,
    mut req: QueryRequest,
    ct: Option<&CancellationToken>,
) -> Result<QueryExecutionResult> {
    req.top_n = req.top_n.max(1);
    req.topk = req.topk.max(1);
    req.doc_cap = req.doc_cap.max(1);

    let mut plan = QueryPlanSummary {
        top_n: req.top_n,
        topk: req.topk,
        doc_cap: req.doc_cap,
        probes_requested: req.probes,
        probes_applied: None,
        feed: req.feed,
        since: req.since.clone(),
        show_context: req.show_context,
        embeddings_available: false,
        model_id: req.model_id.clone(),
    };

    let dim_row = select_with_cancel(ct, sqlx::query!("SELECT dim FROM rag.embedding LIMIT 1").fetch_optional(pool)).await?;

    if dim_row.is_none() {
        return Ok(QueryExecutionResult { plan, rows: Vec::new() });
    }

    plan.embeddings_available = true;
    let db_dim = dim_row.unwrap().dim as usize;

    let qvec = {
        let mut enc: Box<dyn Embedder> = Box::new(
            E5Encoder::new(&req.model_id, req.onnx_filename.as_deref(), req.device)
                .context("init encoder")?,
        );
        let embedded = enc.embed_query(&req.text).context("embed query")?;
        drop(enc);
        embedded
    };

    if qvec.len() != db_dim {
        bail!("query embedding dim={} != DB dim={}", qvec.len(), db_dim);
    }

    let probes = match req.probes {
        Some(p) => Some(p.max(1)),
        None => select_with_cancel(ct, db::recommend_probes(pool)).await?,
    };

    if let Some(p) = probes {
        plan.probes_applied = Some(p);
        let sql = format!("SET LOCAL ivfflat.probes = {}", p);
        select_with_cancel(ct, sqlx::query(&sql).execute(pool)).await?;
    }

    let since_ts: Option<DateTime<Utc>> = parse_since_opt(&req.since)?;

    let candidates = select_with_cancel(
        ct,
        db::fetch_ann_candidates(
            pool,
            &qvec,
            req.top_n,
            req.feed,
            since_ts,
            req.show_context,
        ),
    )
    .await?;

    if candidates.is_empty() {
        return Ok(QueryExecutionResult { plan, rows: Vec::new() });
    }

    let rows = post::shape_results(candidates, req.topk, req.doc_cap);
    Ok(QueryExecutionResult { plan, rows })
}

async fn select_with_cancel<T, F, E>(ct: Option<&CancellationToken>, fut: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T, E>>,
    E: Into<anyhow::Error> + Send + 'static,
{
    if let Some(token) = ct {
        select! {
            _ = token.cancelled() => Err(QueryCancelled.into()),
            res = fut => Ok(res.map_err(|e| e.into())?)
        }
    } else {
        Ok(fut.await.map_err(|e| e.into())?)
    }
}
