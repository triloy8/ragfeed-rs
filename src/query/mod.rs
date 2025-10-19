use anyhow::Result;
use clap::Args;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::util::time::parse_since_opt;

use crate::encoder::Device;
use crate::telemetry::{self};
use crate::telemetry::ops::query::Phase as QueryPhase;

mod db;
mod post;
pub mod service;

pub use post::QueryResultRow;

use self::service::QueryRequest;

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
            ("model_id", args.model_id.clone()),
            ("device", format!("{:?}", args.device)),
        ])
        .entered();

    let since_ts: Option<DateTime<Utc>> = parse_since_opt(&args.since)?;

    let outcome = service::execute(
        pool,
        QueryRequest {
            query: &args.query,
            top_n: args.top_n,
            topk: args.topk,
            doc_cap: args.doc_cap,
            probes: args.probes,
            feed: args.feed,
            since: since_ts,
            include_preview: args.show_context,
            include_text: false,
            model_id: &args.model_id,
            onnx_filename: args.onnx_filename.as_deref(),
            device: args.device,
        },
        Some(&log),
    )
    .await?;

    if outcome.rows.is_empty() {
        return Ok(());
    }

    // output
    let _out_span = log.span(&QueryPhase::Output).entered();
    // Always log human-readable results
    log.info("🔍 Results:");
    for r in &outcome.rows {
        log.info(format!(
            "#{}  dist={:.4}  chunk={} doc={}  {:?}",
            r.rank, r.distance, r.chunk_id, r.doc_id, r.title
        ));
        if args.show_context {
            if let Some(p) = &r.preview { log.info(format!("  {}", p.replace('\n', " "))); }
        }
    }
    // Emit structured result to stdout (presenter-selected)
    log.result(&outcome.rows)?;

    Ok(())
}
