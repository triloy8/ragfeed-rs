use anyhow::{Result};
use clap::Args;
use serde::Serialize;
use sqlx::PgPool;

use crate::telemetry::{self};
use crate::telemetry::ops::reindex::Phase as ReindexPhase;

mod heuristics;
mod db;

#[derive(Args, Debug)]
pub struct ReindexCmd {
    #[arg(long)] pub lists: Option<i32>,
    #[arg(long, default_value_t = false)] pub apply: bool,
}

pub async fn run(pool: &PgPool, args: ReindexCmd) -> Result<()> {
    let log = telemetry::reindex();
    let _g = log.root_span_kv([
        ("lists", format!("{:?}", args.lists)),
        ("apply", args.apply.to_string()),
    ]).entered();

    // count embeddings to drive heuristic
    let n = db::embedding_count(pool).await?;

    // discover index existence and current lists from index definition
    let index_exists = db::index_exists(pool, "embedding_vec_ivf_idx").await?;
    let current_lists = db::index_lists(pool, "embedding_vec_ivf_idx").await?;

    // if base index is missing, do not create it here — migrations own schema
    if !index_exists {
        if !args.apply {
            let _sp = log.span(&ReindexPhase::Plan).entered();
            // Always log human message
            log.info("❌ Index rag.embedding_vec_ivf_idx not found. Run `just migrate` to create it.");
            // Emit structured plan to stdout
            #[derive(Serialize)]
            struct MissingPlan { rows: i64, index: &'static str, message: &'static str }
            let plan = MissingPlan {
                rows: n as i64,
                index: "rag.embedding_vec_ivf_idx",
                message: "Index missing. Run migrations (just migrate) to create it.",
            };
            log.plan(&plan)?;
            return Ok(());
        } else {
            anyhow::bail!("Index rag.embedding_vec_ivf_idx not found. Run migrations (just migrate) to create it.");
        }
    }

    // choose desired lists
    let desired_lists = args.lists.map(|k| k.max(1)).unwrap_or_else(|| heuristics::heuristic_lists(n as i64));

    // decide action (no Create path; only Reindex or Swap)
    let action = if let Some(k) = current_lists {
        if k == desired_lists { Action::Reindex } else { Action::Swap(desired_lists) }
    } else {
        Action::Reindex
    };

    // plan-only output
    if !args.apply {
        let _sp = log.span(&ReindexPhase::Plan).entered();
        // Always log plan summary
        log.info(format!(
            "📝 Reindex plan — rows={} current_lists={:?} desired_lists={} action={:?} analyze=TRUE",
            n, current_lists, desired_lists, action
        ));
        log.info("   Use --apply to execute.");
        // Emit structured plan to stdout
        #[derive(Serialize)]
        struct ReindexPlan { rows: i64, current_lists: Option<i32>, desired_lists: i32, action: String, analyze: bool }
        let action_s = match action { Action::Reindex => "reindex", Action::Swap(_) => "swap" };
        let plan = ReindexPlan { rows: n as i64, current_lists, desired_lists, action: action_s.to_string(), analyze: true };
        log.plan(&plan)?;
        return Ok(());
    }

    // execute
    match action {
        Action::Reindex => {
            let _s = log.span(&ReindexPhase::Reindex).entered();
            let mut conn = pool.acquire().await?;
            db::set_search_path(conn.as_mut()).await?;
            db::reindex_index_ex(conn.as_mut(), "embedding_vec_ivf_idx").await?;
        }
        Action::Swap(k) => {
            let _s1 = log.span(&ReindexPhase::CreateIndex).entered();
            let mut conn = pool.acquire().await?;
            db::set_search_path(conn.as_mut()).await?;
            db::create_new_index_ex(conn.as_mut(), k).await?;
            drop(_s1);
            let _s2 = log.span(&ReindexPhase::Swap).entered();
            db::drop_index_ex(conn.as_mut(), "embedding_vec_ivf_idx").await?;
            db::rename_index_ex(conn.as_mut(), "embedding_vec_ivf_idx_new", "embedding_vec_ivf_idx").await?;
        }
    }

    // analyze after
    let _a = log.span(&ReindexPhase::Analyze).entered();
    let mut conn = pool.acquire().await?;
    db::set_search_path(conn.as_mut()).await?;
    db::analyze_embedding_ex(conn.as_mut()).await?;
    drop(_a);
    log.info("📊 Analyzed rag.embedding");
    log.info("✅ Reindex completed.");

    #[derive(Serialize)]
    struct ReindexResult { action: String, analyzed: bool, desired_lists: i32, current_lists: Option<i32> }
    let action_s = match action { Action::Reindex => "reindex", Action::Swap(_) => "swap" };
    log.result(&ReindexResult { action: action_s.to_string(), analyzed: true, desired_lists, current_lists })?;
    Ok(())
}

#[derive(Debug)]
enum Action { Reindex, Swap(i32) }
