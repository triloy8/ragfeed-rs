use anyhow::{Context, Result};
use clap::Args;
use serde::Serialize;
use sqlx::PgPool;

use crate::out::{self};
use crate::out::reindex::Phase as ReindexPhase;

mod heuristics;
mod ops;

#[derive(Args, Debug)]
pub struct ReindexCmd {
    #[arg(long)] pub lists: Option<i32>,
    #[arg(long, default_value_t = false)] pub apply: bool,
}

pub async fn run(pool: &PgPool, args: ReindexCmd) -> Result<()> {
    let log = out::reindex();
    let _g = log.root_span_kv([
        ("lists", format!("{:?}", args.lists)),
        ("apply", args.apply.to_string()),
    ]).entered();

    // count embeddings to drive heuristic
    let n = sqlx::query!("SELECT COUNT(*)::bigint AS n FROM rag.embedding")
        .fetch_one(pool)
        .await?
        .n
        .unwrap_or(0);

    // discover index existence and current lists from index definition
    let idx_row = sqlx::query!(
        r#"
        SELECT substring(pg_get_indexdef(i.indexrelid) from 'lists = ([0-9]+)') AS lists
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid
        JOIN pg_namespace nsp ON nsp.oid = c.relnamespace
        WHERE nsp.nspname = 'rag' AND c.relname = 'embedding_vec_ivf_idx'
        "#
    )
    .fetch_optional(pool)
    .await?;
    let index_exists = idx_row.is_some();
    let current_lists = idx_row.and_then(|r| r.lists).and_then(|s| s.parse::<i32>().ok());

    // choose desired lists
    let desired_lists = args.lists.map(|k| k.max(1)).unwrap_or_else(|| heuristics::heuristic_lists(n as i64));

    // decide action
    let action = if !index_exists {
        Action::Create(desired_lists)
    } else if let Some(k) = current_lists {
        if k == desired_lists { Action::Reindex } else { Action::Swap(desired_lists) }
    } else {
        Action::Reindex
    };

    // plan-only output
    if !args.apply {
        if out::json_mode() {
            #[derive(Serialize)]
            struct ReindexPlan { rows: i64, current_lists: Option<i32>, desired_lists: i32, action: String, analyze: bool }
            let action_s = match action { Action::Create(_) => "create", Action::Reindex => "reindex", Action::Swap(_) => "swap" };
            let plan = ReindexPlan { rows: n as i64, current_lists, desired_lists, action: action_s.to_string(), analyze: true };
            log.plan(&plan)?;
        } else {
            log.info(format!(
                "📝 Reindex plan — rows={} current_lists={:?} desired_lists={} action={:?} analyze=TRUE",
                n, current_lists, desired_lists, action
            ));
            log.info("   Use --apply to execute.");
        }
        return Ok(());
    }

    // execute
    match action {
        Action::Create(k) => {
            let _s = log.span(&ReindexPhase::CreateIndex).entered();
            ops::create_new_index(pool, k).await?;
            sqlx::query("ALTER INDEX rag.embedding_vec_ivf_idx_new RENAME TO embedding_vec_ivf_idx")
                .execute(pool)
                .await?;
        }
        Action::Reindex => {
            let _s = log.span(&ReindexPhase::Reindex).entered();
            sqlx::query("REINDEX INDEX CONCURRENTLY rag.embedding_vec_ivf_idx")
                .execute(pool)
                .await?;
        }
        Action::Swap(k) => {
            let _s1 = log.span(&ReindexPhase::CreateIndex).entered();
            ops::create_new_index(pool, k).await?;
            drop(_s1);
            let _s2 = log.span(&ReindexPhase::Swap).entered();
            sqlx::query("DROP INDEX CONCURRENTLY IF EXISTS rag.embedding_vec_ivf_idx")
                .execute(pool)
                .await?;
            sqlx::query("ALTER INDEX rag.embedding_vec_ivf_idx_new RENAME TO embedding_vec_ivf_idx")
                .execute(pool)
                .await?;
        }
    }

    // analyze after
    let _a = log.span(&ReindexPhase::Analyze).entered();
    sqlx::query("ANALYZE rag.embedding").execute(pool).await?;
    drop(_a);
    log.info("📊 Analyzed rag.embedding");
    log.info("✅ Reindex completed.");

    if out::json_mode() {
        #[derive(Serialize)]
        struct ReindexResult { action: String, analyzed: bool, desired_lists: i32, current_lists: Option<i32> }
        let action_s = match action { Action::Create(_) => "create", Action::Reindex => "reindex", Action::Swap(_) => "swap" };
        log.result(&ReindexResult { action: action_s.to_string(), analyzed: true, desired_lists, current_lists })?;
    }
    Ok(())
}

#[derive(Debug)]
enum Action { Create(i32), Reindex, Swap(i32) }

