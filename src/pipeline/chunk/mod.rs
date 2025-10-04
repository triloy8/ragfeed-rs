pub mod select;
pub mod logic;
mod db;

use anyhow::{Context, Result};
use clap::Args;
use serde::Serialize;
use sqlx::PgPool;

use crate::telemetry::{self};
use crate::telemetry::ops::chunk::Phase as ChunkPhase;
use crate::tokenizer::E5Tokenizer;
use crate::util::time::parse_since_opt;

use self::select::select_docs;
use self::logic::chunk_token_ids;

#[derive(Args)]
pub struct ChunkCmd {
    #[arg(long)] since: Option<String>,
    #[arg(long)] doc_id: Option<i64>,
    #[arg(long, default_value_t = 350)] tokens_target: usize,
    #[arg(long, default_value_t = 80)]  overlap: usize,
    #[arg(long, default_value_t = 24)]  max_chunks_per_doc: usize,
    #[arg(long, default_value_t = false)] force: bool,
    #[arg(long, default_value_t = false)] apply: bool,
    #[arg(long, default_value_t = 10)] plan_limit: usize,
}

pub async fn run(pool: &PgPool, args: ChunkCmd) -> Result<()> {
    let log = telemetry::chunk();
    let _g = log.root_span_kv([
        ("since", format!("{:?}", args.since)),
        ("doc_id", format!("{:?}", args.doc_id)),
        ("tokens_target", args.tokens_target.to_string()),
        ("overlap", args.overlap.to_string()),
        ("max_chunks_per_doc", args.max_chunks_per_doc.to_string()),
        ("force", args.force.to_string()),
        ("apply", args.apply.to_string()),
        ("plan_limit", args.plan_limit.to_string()),
    ]).entered();

    let _s = log.span(&ChunkPhase::SelectDocs).entered();
    let since_ts = parse_since_opt(&args.since)?;
    let docs = select_docs(pool, args.doc_id, since_ts, args.force).await?;
    drop(_s);
    if docs.is_empty() {
        log.info(format!(
            "ℹ️  No documents to chunk (status='ingest'{}{})",
            if args.doc_id.is_some() { ", --doc-id" } else { "" },
            if args.since.is_some() { ", --since" } else { "" }
        ));
        return Ok(());
    }

    if !args.apply {
        let _sp = log.span(&ChunkPhase::Plan).entered();
        // Always log plan summary
        log.info(format!(
            "📝 Chunk plan — docs={} force={} tokens_target={} overlap={} max_chunks_per_doc={}",
            docs.len(), args.force, args.tokens_target, args.overlap, args.max_chunks_per_doc
        ));
        for (doc_id, _text_clean) in docs.iter().take(args.plan_limit) {
            log.info(format!("  doc_id={}", doc_id));
        }
        if docs.len() > args.plan_limit { log.info(format!("  ... ({} more)", docs.len() - args.plan_limit)); }
        log.info("   Use --apply to execute.");
        // Emit structured plan when in JSON mode (stdout)
        if telemetry::config::json_mode() {
            #[derive(Serialize)]
            struct ChunkPlan { docs: usize, force: bool, tokens_target: usize, overlap: usize, max_chunks_per_doc: usize, sample_doc_ids: Vec<i64> }
            let sample_doc_ids: Vec<i64> = docs.iter().take(args.plan_limit).map(|(id, _)| *id).collect();
            let plan = ChunkPlan {
                docs: docs.len(),
                force: args.force,
                tokens_target: args.tokens_target,
                overlap: args.overlap,
                max_chunks_per_doc: args.max_chunks_per_doc,
                sample_doc_ids,
            };
            log.plan(&plan)?;
        }
        return Ok(());
    }

    let tok: E5Tokenizer = E5Tokenizer::new()
        .context("init E5 tokenizer")?;

    #[derive(Serialize)]
    struct DocResult { doc_id: i64, inserted: usize }
    let mut per_doc: Vec<DocResult> = Vec::new();

    for (doc_id, text_clean) in docs {
        let Some(text) = text_clean.as_deref() else { continue; };
        if text.trim().is_empty() { continue; }

        let _sp = log.span(&ChunkPhase::Tokenize).entered();
        let ids: Vec<u32> = tok
            .ids_passage(text)
            .with_context(|| format!("tokenize doc_id={}", doc_id))?;
        drop(_sp);

        if ids.is_empty() {
            let _us = log.span(&ChunkPhase::UpdateStatus).entered();
            db::mark_chunked(pool, doc_id).await?;
            drop(_us);
            log.info(format!("✅ doc_id={} → 0 chunks (no tokens)", doc_id));
            per_doc.push(DocResult { doc_id, inserted: 0 });
            continue;
        }

        let slices = chunk_token_ids(&ids, args.tokens_target, args.overlap, args.max_chunks_per_doc);

        let _ic = log.span(&ChunkPhase::InsertChunk).entered();
        db::delete_chunks(pool, doc_id).await?;

        let mut inserted = 0usize;
        for (i, id_slice) in slices.into_iter().enumerate() {
            let chunk_text = tok.decode_ids(id_slice)
                .with_context(|| format!("decode chunk {} for doc_id={}", i, doc_id))?;
            if chunk_text.trim().is_empty() { continue; }

            let token_count = id_slice.len() as i32;

            let _ = db::insert_chunk(pool, doc_id, i as i32, &chunk_text, token_count).await?;

            inserted += 1;
        }
        drop(_ic);

        if inserted > 0 {
            let _us = log.span(&ChunkPhase::UpdateStatus).entered();
            db::mark_chunked(pool, doc_id).await?;
            drop(_us);
        }

        log.info(format!("✅ doc_id={} → {} chunk(s)", doc_id, inserted));
        per_doc.push(DocResult { doc_id, inserted });
    }

    if telemetry::config::json_mode() {
        #[derive(Serialize)]
        struct ChunkResult { totals: usize, per_doc: Vec<DocResult> }
        let totals = per_doc.iter().map(|d| d.inserted).sum();
        let res = ChunkResult { totals, per_doc };
        let log = telemetry::chunk();
        log.result(&res)?;
    }
    Ok(())
}
