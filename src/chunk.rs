use sqlx::Row; // .get()
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use sqlx::PgPool;

use crate::tokenizer::E5Tokenizer;

#[derive(clap::Args)]
pub struct ChunkCmd {
    #[arg(long)] since: Option<String>,                 // "2d" | "YYYY-MM-DD" | RFC3339
    #[arg(long)] doc_id: Option<i64>,                   // only this doc
    #[arg(long, default_value_t = 350)] tokens_target: usize,
    #[arg(long, default_value_t = 80)]  overlap: usize,
    #[arg(long, default_value_t = 24)]  max_chunks_per_doc: usize,
    #[arg(long, default_value_t = false)] force: bool,
    #[arg(long, default_value_t = false)] apply: bool, // default is plan-only
    #[arg(long, default_value_t = 10)] plan_limit: usize, // how many doc IDs to list in plan
}

pub async fn run(pool: &PgPool, args: ChunkCmd) -> Result<()> {
    // select candidate docs
    let docs = select_docs(pool, &args).await?;
    if docs.is_empty() {
        println!("ℹ️  No documents to chunk (status='ingest'{}{})",
            if args.doc_id.is_some() { ", --doc-id" } else { "" },
            if args.since.is_some() { ", --since" } else { "" });
        return Ok(());
    }

    // Plan-only by default: show plan and exit (no tokenization, no writes)
    if !args.apply {
        println!(
            "📝 Chunk plan — docs={} force={} tokens_target={} overlap={} max_chunks_per_doc={}",
            docs.len(), args.force, args.tokens_target, args.overlap, args.max_chunks_per_doc
        );
        for (doc_id, _text_clean) in docs.iter().take(args.plan_limit) {
            println!("  doc_id={}", doc_id);
        }
        if docs.len() > args.plan_limit { println!("  ... ({} more)", docs.len() - args.plan_limit); }
        println!("   Use --apply to execute chunking.");
        return Ok(());
    }

    // APPLY: build tokenizer (env overrides, sensible defaults)
    let tok: E5Tokenizer = E5Tokenizer::new()
        .context("init E5 tokenizer")?;

    // process each doc
    for (doc_id, text_clean) in docs {
        let Some(text) = text_clean.as_deref() else { continue; };
        if text.trim().is_empty() { continue; }

        // tokenize once per doc (encode needs &mut self)
        let ids: Vec<u32> = tok.ids_passage(text)
            .with_context(|| format!("tokenize doc_id={}", doc_id))?;

        if ids.is_empty() {
            sqlx::query!("UPDATE rag.document SET status='chunked' WHERE doc_id=$1", doc_id)
                .execute(pool).await?;
            println!("✅ doc_id={} → 0 chunks (no tokens)", doc_id);
            continue;
        }

        let slices = chunk_token_ids(&ids, args.tokens_target, args.overlap, args.max_chunks_per_doc);

        // idempotent: clear any previous chunks for this doc
        sqlx::query!("DELETE FROM rag.chunk WHERE doc_id = $1", doc_id)
            .execute(pool).await?;

        // insert chunks (no md5 for now, optional in schema)
        let mut inserted = 0usize;
        for (i, id_slice) in slices.into_iter().enumerate() {
            let chunk_text = tok.decode_ids(id_slice)
                .with_context(|| format!("decode chunk {} for doc_id={}", i, doc_id))?;
            if chunk_text.trim().is_empty() { continue; }

            let token_count = id_slice.len() as i32;

            sqlx::query!(
                r#"
                INSERT INTO rag.chunk (doc_id, chunk_index, text, token_count)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (doc_id, chunk_index) DO UPDATE
                  SET text = EXCLUDED.text, token_count = EXCLUDED.token_count
                "#,
                doc_id,
                i as i32,
                chunk_text,
                token_count
            )
            .execute(pool).await?;

            inserted += 1;
        }

        if inserted > 0 {
            sqlx::query!("UPDATE rag.document SET status='chunked' WHERE doc_id=$1", doc_id)
                .execute(pool).await?;
        }

        println!("✅ doc_id={} → {} chunk(s)", doc_id, inserted);
    }

    Ok(())
}

async fn select_docs(pool: &PgPool, args: &ChunkCmd) -> Result<Vec<(i64, Option<String>)>> {
    let ts = parse_since(&args.since)?;
    let doc_id = args.doc_id;
    let force = args.force;

    // one query that handles both optional filters
    let rows = sqlx::query(
        r#"
        SELECT doc_id, text_clean
        FROM rag.document
        WHERE ($3::bool OR status = 'ingest')        -- NEW: ignore status when forced
          AND ($1::bigint      IS NULL OR doc_id = $1)
          AND ($2::timestamptz IS NULL OR fetched_at >= $2)
        ORDER BY doc_id DESC
        LIMIT 1000
        "#
    )
    .bind(doc_id) // $1
    .bind(ts)     // $2
    .bind(force)  // $3
    .fetch_all(pool)
    .await?;

    // map raw rows into something simple: (doc_id, text_clean)
    let docs = rows.into_iter()
        .map(|row| (row.get::<i64,_>("doc_id"), row.get::<Option<String>,_>("text_clean")))
        .collect();

    Ok(docs)
}

fn parse_since(since: &Option<String>) -> Result<Option<DateTime<Utc>>> {
    let Some(s) = since.as_ref() else { return Ok(None); };

    // "2d" → now - 2 days
    if let Some(stripped) = s.strip_suffix('d') {
        if let Ok(days) = stripped.parse::<i64>() {
            if days > 0 { return Ok(Some(Utc::now() - Duration::days(days))); }
        }
    }
    // "YYYY-MM-DD"
    if let Ok(nd) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let dt = nd.and_hms_opt(0,0,0).unwrap();
        return Ok(Some(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc)));
    }
    // RFC3339
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(Some(dt.with_timezone(&Utc)));
    }
    // if unparseable -> ignore filter
    Ok(None)
}

pub fn chunk_token_ids<'a>(
    ids: &'a [u32],
    target: usize,
    overlap: usize,
    max_chunks: usize,
) -> Vec<&'a [u32]> {
    let target = target.max(1);
    let overlap = overlap.min(target.saturating_sub(1));

    let mut out = Vec::new();
    let mut start = 0usize;

    while start < ids.len() && out.len() < max_chunks {
        let end = (start + target).min(ids.len());
        out.push(&ids[start..end]);
        if end == ids.len() { break; }
        start = end.saturating_sub(overlap);
    }
    out
}
