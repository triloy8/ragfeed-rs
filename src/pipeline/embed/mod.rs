use anyhow::{bail, Context, Result};
use clap::Args;
use serde::Serialize;
use sqlx::PgPool;

use crate::encoder::{Device, E5Encoder};
use crate::telemetry::{self};
use crate::telemetry::ops::embed::Phase as EmbedPhase;

mod db;
mod r#loop;

#[derive(Args, Debug)]
pub struct EmbedCmd {
    #[arg(long, default_value = "intfloat/e5-small-v2")] model_id: String,
    #[arg(long)] onnx_filename: Option<String>,
    #[arg(long, value_enum, default_value_t = Device::Cpu)] device: Device,
    #[arg(long, default_value_t = 384)] dim: usize,
    #[arg(long, default_value_t = 128)] batch: usize,
    #[arg(long)] max: Option<i64>,
    #[arg(long, default_value_t = false)] force: bool,
    #[arg(long, default_value_t = false)] apply: bool,
    #[arg(long, default_value_t = 10)] plan_limit: usize,
}

pub async fn run(pool: &PgPool, args: EmbedCmd) -> Result<()> {
    let log = telemetry::embed();
    let _g = log
        .root_span_kv([
            ("model_id", args.model_id.clone()),
            ("onnx_filename", format!("{:?}", args.onnx_filename)),
            ("device", format!("{:?}", args.device)),
            ("dim", args.dim.to_string()),
            ("batch", args.batch.to_string()),
            ("max", format!("{:?}", args.max)),
            ("force", args.force.to_string()),
            ("apply", args.apply.to_string()),
            ("plan_limit", args.plan_limit.to_string()),
        ])
        .entered();

    let model_tag = format!(
        "{}@onnx-{}",
        args.model_id,
        match args.device { Device::Cpu => "cpu", Device::Cuda => "cuda" }
    );

    let batch = args.batch.max(1);

    // Plan-only
    if !args.apply {
        let _sp = log.span(&EmbedPhase::Plan).entered();
        let total_candidates = { let _s = log.span(&EmbedPhase::CountCandidates).entered(); db::count_candidates(pool, &model_tag, args.force).await? };
        let planned = match args.max { Some(m) => total_candidates.min(m), None => total_candidates };
        let ids = db::list_candidate_chunk_ids(pool, &model_tag, args.force, args.plan_limit as i64).await?;
        if telemetry::config::json_mode() {
            #[derive(Serialize)]
            struct EmbedPlan { model: String, dim: usize, batch: usize, force: bool, candidates: i64, planned: i64, sample_chunk_ids: Vec<i64> }
            let plan = EmbedPlan { model: model_tag.clone(), dim: args.dim, batch, force: args.force, candidates: total_candidates, planned, sample_chunk_ids: ids };
            log.plan(&plan)?;
        } else {
            log.info(format!(
                "📝 Embed plan — model={} dim={} batch={} force={} candidates={} planned={}",
                model_tag, args.dim, batch, args.force, total_candidates, planned
            ));
            for id in &ids { log.info(format!("  chunk_id={}", id)); }
            if (args.plan_limit as i64) < planned { log.info("  ... (more up to planned count)"); }
            log.info("   Use --apply to run embedding.");
        }
        return Ok(());
    }

    // APPLY: Build encoder
    let _lm = log.span(&EmbedPhase::LoadModel).entered();
    let mut encoder = E5Encoder::new(&args.model_id, args.onnx_filename.as_deref(), args.device)?;
    drop(_lm);

    let total = if args.force {
        r#loop::embed_force_once(pool, &mut encoder, &model_tag, args.dim, batch, args.max).await?
    } else {
        r#loop::embed_missing_paged(pool, &mut encoder, &model_tag, args.dim, batch, args.max).await?
    };

    if total == 0 {
        log.info(format!("ℹ️  No chunks to embed (force={} model={})", args.force, model_tag));
    }

    if telemetry::config::json_mode() {
        #[derive(Serialize)]
        struct EmbedResult { total_embedded: i64 }
        log.result(&EmbedResult { total_embedded: total })?;
    }

    Ok(())
}
