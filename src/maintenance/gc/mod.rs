pub mod counts;
pub mod deletes;
pub mod status;
pub mod vacuum;

use anyhow::Result;
use clap::Args;
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::PgPool;

use crate::telemetry::{self};
use crate::telemetry::ops::gc::Phase as GcPhase;
use crate::util::time::parse_cutoff_str;

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum VacuumMode {
    #[value(name = "analyze")] Analyze,
    #[value(name = "full")] Full,
    #[value(name = "off")] Off,
}

#[derive(Args, Debug)]
pub struct GcCmd {
    #[arg(long, default_value_t = false)] pub apply: bool,
    #[arg(long, default_value = "30d")] pub older_than: String,
    #[arg(long, default_value_t = 10_000)] pub max: i64,
    #[arg(long)] pub feed: Option<i32>,
    #[arg(long, value_enum, default_value_t = VacuumMode::Analyze)] pub vacuum: VacuumMode,
    #[arg(long, default_value_t = false)] pub drop_temp_indexes: bool,
    #[arg(long, default_value_t = false)] pub fix_status: bool,
}

pub async fn run(pool: &PgPool, args: GcCmd) -> Result<()> {
    let cutoff = parse_cutoff_str(&args.older_than);
    let execute = args.apply;
    let mode = if execute { "apply" } else { "plan" };

    let log = telemetry::gc();
    let _g = log.root_span_kv([
        ("mode", mode.to_string()),
        ("feed", format!("{:?}", args.feed)),
        ("cutoff", format!("{:?}", cutoff)),
        ("max", args.max.to_string()),
        ("vacuum", format!("{:?}", args.vacuum)),
        ("fix_status", args.fix_status.to_string()),
        ("drop_temp_indexes", args.drop_temp_indexes.to_string()),
    ]).entered();
    let _p = log.span(&GcPhase::Plan).entered();
    log.info(format!(
        "📝 GC plan — mode={} feed={:?} cutoff={:?} max={} vacuum={:?} fix_status={} drop_temp_indexes={}",
        mode, args.feed, cutoff, args.max, args.vacuum, args.fix_status, args.drop_temp_indexes
    ));
    if !execute { log.info("   Use --apply to execute."); }

    // orphan chunks
    let orphan_chunks = { let _s = log.span(&GcPhase::Count).entered(); crate::maintenance::gc::counts::count_orphan_chunks(pool, args.feed).await? };
    log.info(format!("🧱 Orphan chunks: {}", orphan_chunks));
    if execute && orphan_chunks > 0 { crate::maintenance::gc::deletes::delete_orphan_chunks(pool, args.feed, args.max).await?; }

    // orphan embeddings (note: FK should prevent these; no feed scope possible)
    let orphan_emb = { let _s = log.span(&GcPhase::Count).entered(); crate::maintenance::gc::counts::count_orphan_embeddings(pool).await? };
    log.info(format!("🧬 Orphan embeddings: {}", orphan_emb));
    if execute && orphan_emb > 0 { crate::maintenance::gc::deletes::delete_orphan_embeddings(pool, args.max).await?; }

    // error docs older than cutoff
    let err_docs = { let _s = log.span(&GcPhase::Count).entered(); crate::maintenance::gc::counts::count_error_docs(pool, cutoff, args.feed).await? };
    log.info(format!("⚠️  Error docs (> cutoff): {}", err_docs));
    if execute && err_docs > 0 { crate::maintenance::gc::deletes::delete_error_docs(pool, cutoff, args.feed, args.max).await?; }

    // never-chunked docs older than cutoff
    let stale_docs = { let _s = log.span(&GcPhase::Count).entered(); crate::maintenance::gc::counts::count_never_chunked_docs(pool, cutoff, args.feed).await? };
    log.info(format!("⏳ Never-chunked docs (> cutoff): {}", stale_docs));
    if execute && stale_docs > 0 { crate::maintenance::gc::deletes::delete_never_chunked_docs(pool, cutoff, args.feed, args.max).await?; }

    // bad chunks
    let bad_chunks = { let _s = log.span(&GcPhase::Count).entered(); crate::maintenance::gc::counts::count_bad_chunks(pool, args.feed).await? };
    log.info(format!("🧹 Bad chunks (empty/≤0 tokens): {}", bad_chunks));
    if execute && bad_chunks > 0 { crate::maintenance::gc::deletes::delete_bad_chunks(pool, args.feed, args.max).await?; }

    // fix status
    if args.fix_status {
        if execute { let _s = log.span(&GcPhase::FixStatus).entered(); crate::maintenance::gc::status::fix_statuses(pool, args.feed).await?; }
        else { log.info("🔎 Would normalize document.status based on chunk/embedding presence"); }
    }

    // drop temp indexes
    if args.drop_temp_indexes {
        if execute { let _s = log.span(&GcPhase::DropTemp).entered(); crate::maintenance::gc::vacuum::drop_temp_indexes(pool).await?; }
        else { log.info("🔎 Would DROP INDEX CONCURRENTLY rag.embedding_vec_ivf_idx_new if exists"); }
    }

    // vacuum/Analyze
    match args.vacuum {
        VacuumMode::Off => {}
        VacuumMode::Analyze => {
            if execute { let _s = log.span(&GcPhase::Analyze).entered(); crate::maintenance::gc::vacuum::analyze_tables(pool).await?; }
            else { log.info("🔎 Would ANALYZE rag.document, rag.chunk, rag.embedding"); }
        }
        VacuumMode::Full => {
            if execute { let _s = log.span(&GcPhase::Vacuum).entered(); crate::maintenance::gc::vacuum::vacuum_full(pool).await?; }
            else { log.info("🔎 Would VACUUM (ANALYZE, FULL) rag.document, rag.chunk, rag.embedding"); }
        }
    }

    if !execute && telemetry::config::json_mode() {
        #[derive(Serialize)]
        struct Counts { orphan_chunks: i64, orphan_embeddings: i64, error_docs: i64, never_chunked_docs: i64, bad_chunks: i64 }
        #[derive(Serialize)]
        struct GcPlanOut {
            mode: String,
            feed: Option<i32>,
            cutoff: Option<DateTime<Utc>>,
            max: i64,
            vacuum: String,
            fix_status: bool,
            drop_temp_indexes: bool,
            counts: Counts,
        }
        let plan = GcPlanOut {
            mode: mode.to_string(),
            feed: args.feed,
            cutoff,
            max: args.max,
            vacuum: format!("{:?}", args.vacuum),
            fix_status: args.fix_status,
            drop_temp_indexes: args.drop_temp_indexes,
            counts: Counts { orphan_chunks, orphan_embeddings: orphan_emb, error_docs: err_docs, never_chunked_docs: stale_docs, bad_chunks },
        };
        let log = telemetry::gc();
        log.plan(&plan)?;
    } else if execute && telemetry::config::json_mode() {
        #[derive(Serialize)]
        struct Counts { orphan_chunks: i64, orphan_embeddings: i64, error_docs: i64, never_chunked_docs: i64, bad_chunks: i64 }
        #[derive(Serialize)]
        struct GcResultOut { counts_before: Counts, fix_status: bool, drop_temp_indexes: bool, vacuum: String }
        let res = GcResultOut {
            counts_before: Counts { orphan_chunks, orphan_embeddings: orphan_emb, error_docs: err_docs, never_chunked_docs: stale_docs, bad_chunks },
            fix_status: args.fix_status,
            drop_temp_indexes: args.drop_temp_indexes,
            vacuum: format!("{:?}", args.vacuum),
        };
        let log = telemetry::gc();
        log.result(&res)?;
    }

    Ok(())
}
