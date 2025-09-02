use anyhow::{bail, Context, Result};
use clap::Args;
use serde::Serialize;
use sqlx::PgPool;

use crate::encoder::{Device, E5Encoder};

// pgvector type for sqlx
use pgvector::Vector as PgVector;

// onnx session and tokenizer handled by encoder module

#[derive(Args, Debug)]
pub struct EmbedCmd {
    #[arg(long, default_value = "intfloat/e5-small-v2")] model_id: String,
    #[arg(long)] onnx_filename: Option<String>, // fallback to common names if not provided
    #[arg(long, value_enum, default_value_t = Device::Cpu)] device: Device,
    #[arg(long, default_value_t = 384)] dim: usize, // embedding output dimension (must match DB schema and model)
    #[arg(long, default_value_t = 128)] batch: usize,
    #[arg(long)] max: Option<i64>,
    #[arg(long, default_value_t = false)] force: bool,
    #[arg(long, default_value_t = false)] apply: bool, // default is plan-only (no model calls, no writes)
    #[arg(long, default_value_t = 10)] plan_limit: usize, // how many chunk IDs to list in plan
}

pub async fn run(pool: &PgPool, args: EmbedCmd) -> Result<()> {
    use crate::out::{self};
    use crate::out::embed::Phase as EmbedPhase;
    let log = out::embed();
    let _g = log.root_span_kv([
        ("model_id", args.model_id.clone()),
        ("onnx_filename", format!("{:?}", args.onnx_filename)),
        ("device", format!("{:?}", args.device)),
        ("dim", args.dim.to_string()),
        ("batch", args.batch.to_string()),
        ("max", format!("{:?}", args.max)),
        ("force", args.force.to_string()),
        ("apply", args.apply.to_string()),
        ("plan_limit", args.plan_limit.to_string()),
    ]).entered();
    // effective model tag (stored in DB); plan-only should not build model
    let model_tag = format!(
        "{}@onnx-{}",
        args.model_id,
        match args.device { Device::Cpu => "cpu", Device::Cuda => "cuda" }
    );

    let mut total = 0i64;
    let batch = args.batch.max(1);

    // Plan-only default: compute counts via SQL; do not build encoder or write
    if !args.apply {
        let _sp = log.span(&EmbedPhase::Plan).entered();
        let total_candidates = {
            let _s = log.span(&EmbedPhase::CountCandidates).entered();
            count_candidates(pool, &model_tag, args.force).await?
        };
        let planned = match args.max { Some(m) => total_candidates.min(m), None => total_candidates };
        let ids = list_candidate_chunk_ids(pool, &model_tag, args.force, args.plan_limit as i64).await?;
        if out::json_mode() {
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

    // APPLY: Build encoder (tokenizer + ONNX session)
    let _lm = log.span(&EmbedPhase::LoadModel).entered();
    let mut encoder = E5Encoder::new(&args.model_id, args.onnx_filename.as_deref(), args.device)?;
    drop(_lm);

    // In --force mode, fetch all once (optionally limited by --max) and process in batches, then exit.
    if args.force {
        let rows = {
            let _fb = log.span(&EmbedPhase::FetchBatch).entered();
            fetch_all_chunks(pool, args.max).await?
        };
        if rows.is_empty() {
            log.info(format!("ℹ️  No chunks to embed (force={} model={})", args.force, model_tag));
            if out::json_mode() {
                #[derive(Serialize)]
                struct EmbedResult { total_embedded: i64 }
                log.result(&EmbedResult { total_embedded: 0 })?;
            }
            return Ok(());
        }

        for chunk in rows.chunks(batch) {
            let chunk_ids: Vec<i64> = chunk.iter().map(|(id, _)| *id).collect();
            let texts: Vec<String> = chunk.iter().map(|(_, t)| t.clone()).collect();

            let _enc = log.span(&EmbedPhase::Encode).entered();
            let embeddings = encoder.embed_passages(&texts)?; // Vec<Vec<f32>>
            drop(_enc);

            // all vectors should have same dim
            let dim = embeddings.get(0).map(|v| v.len()).unwrap_or(0);
            if dim == 0 { bail!("empty embedding dimension"); }
            if dim as i32 != args.dim as i32 {
                bail!("model produced dim={} but --dim={} was specified", dim, args.dim);
            }

            // insert embeddings
            for (chunk_id, vec) in chunk_ids.into_iter().zip(embeddings.into_iter()) {
                let _ins = log.span(&EmbedPhase::InsertEmbedding).entered();
                sqlx::query(
                    r#"
                    INSERT INTO rag.embedding (chunk_id, model, dim, vec)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (chunk_id) DO UPDATE
                      SET model = EXCLUDED.model,
                          dim   = EXCLUDED.dim,
                          vec   = EXCLUDED.vec
                    "#
                )
                .bind(chunk_id)
                .bind(&model_tag)
                .bind(args.dim as i32)
                .bind(PgVector::from(vec))
                .execute(pool)
                .await?;
                drop(_ins);
            }

            total += texts.len() as i64;
            log.info(format!("✅ embedded {} chunk(s) (total={})", texts.len(), total));
        }

        if out::json_mode() {
            #[derive(Serialize)]
            struct EmbedResult { total_embedded: i64 }
            log.result(&EmbedResult { total_embedded: total })?;
        }
        return Ok(());
    }

    // Default mode: page only missing embeddings until done.
    let mut remaining = args.max.unwrap_or(i64::MAX);
    loop {
        let n = remaining.min(batch as i64) as i64;
        if n <= 0 { break; }

        let rows = {
            let _fb = log.span(&EmbedPhase::FetchBatch).entered();
            fetch_chunks(pool, &model_tag, false, n).await?
        };
        if rows.is_empty() { break; }

        let chunk_ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
        let texts: Vec<String> = rows.into_iter().map(|(_, t)| t).collect();

        let _enc = log.span(&EmbedPhase::Encode).entered();
        let embeddings = encoder.embed_passages(&texts)?; // Vec<Vec<f32>>
        drop(_enc);

        // all vectors should have same dim
        let dim = embeddings.get(0).map(|v| v.len()).unwrap_or(0);
        if dim == 0 { bail!("empty embedding dimension"); }
        if dim as i32 != args.dim as i32 {
            bail!("model produced dim={} but --dim={} was specified", dim, args.dim);
        }

        // insert embeddings
        for (chunk_id, vec) in chunk_ids.into_iter().zip(embeddings.into_iter()) {
            let _ins = log.span(&EmbedPhase::InsertEmbedding).entered();
            sqlx::query(
                r#"
                INSERT INTO rag.embedding (chunk_id, model, dim, vec)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (chunk_id) DO UPDATE
                  SET model = EXCLUDED.model,
                      dim   = EXCLUDED.dim,
                      vec   = EXCLUDED.vec
                "#
            )
            .bind(chunk_id)
            .bind(&model_tag)
            .bind(args.dim as i32)
            .bind(PgVector::from(vec))
            .execute(pool)
            .await?;
            drop(_ins);
        }

        total += texts.len() as i64;
        remaining -= n;
        log.info(format!("✅ embedded {} chunk(s) (total={})", texts.len(), total));
    }

    if total == 0 {
        log.info(format!("ℹ️  No chunks to embed (force={} model={})", args.force, model_tag));
    }

    if out::json_mode() {
        #[derive(Serialize)]
        struct EmbedResult { total_embedded: i64 }
        log.result(&EmbedResult { total_embedded: total })?;
    }

    Ok(())
}

async fn fetch_chunks(pool: &PgPool, model_tag: &str, force: bool, limit: i64) -> Result<Vec<(i64, String)>> {
    if force {
        let rows = sqlx::query!(
            r#"
            SELECT c.chunk_id, c.text
            FROM rag.chunk c
            ORDER BY c.chunk_id
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(pool)
        .await?;
        return Ok(rows.into_iter().map(|r| (r.chunk_id, r.text)).collect());
    }

    let rows = sqlx::query!(
        r#"
        SELECT c.chunk_id, c.text
        FROM rag.chunk c
        LEFT JOIN rag.embedding e
          ON e.chunk_id = c.chunk_id AND e.model = $1
        WHERE e.chunk_id IS NULL
        ORDER BY c.chunk_id
        LIMIT $2
        "#,
        model_tag,
        limit
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|r| (r.chunk_id, r.text)).collect())
}

async fn fetch_all_chunks(pool: &PgPool, limit: Option<i64>) -> Result<Vec<(i64, String)>> {
    if let Some(limit) = limit {
        let rows = sqlx::query!(
            r#"
            SELECT c.chunk_id, c.text
            FROM rag.chunk c
            ORDER BY c.chunk_id
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(pool)
        .await?;
        return Ok(rows.into_iter().map(|r| (r.chunk_id, r.text)).collect());
    }

    let rows = sqlx::query!(
        r#"
        SELECT c.chunk_id, c.text
        FROM rag.chunk c
        ORDER BY c.chunk_id
        "#
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|r| (r.chunk_id, r.text)).collect())
}

async fn count_candidates(pool: &PgPool, model_tag: &str, force: bool) -> Result<i64> {
    let n = if force {
        sqlx::query_scalar!(
            r#"SELECT COUNT(*)::bigint FROM rag.chunk"#
        )
        .fetch_one(pool)
        .await?
    } else {
        sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)::bigint
            FROM rag.chunk c
            LEFT JOIN rag.embedding e
              ON e.chunk_id = c.chunk_id AND e.model = $1
            WHERE e.chunk_id IS NULL
            "#,
            model_tag
        )
        .fetch_one(pool)
        .await?
    };
    Ok(n.unwrap_or(0))
}

async fn list_candidate_chunk_ids(pool: &PgPool, model_tag: &str, force: bool, limit: i64) -> Result<Vec<i64>> {
    if limit <= 0 { return Ok(vec![]); }
    if force {
        let rows = sqlx::query!(
            r#"
            SELECT c.chunk_id
            FROM rag.chunk c
            ORDER BY c.chunk_id
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(pool)
        .await?;
        return Ok(rows.into_iter().map(|r| r.chunk_id).collect());
    }

    let rows = sqlx::query!(
        r#"
        SELECT c.chunk_id
        FROM rag.chunk c
        LEFT JOIN rag.embedding e
          ON e.chunk_id = c.chunk_id AND e.model = $1
        WHERE e.chunk_id IS NULL
        ORDER BY c.chunk_id
        LIMIT $2
        "#,
        model_tag,
        limit
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|r| r.chunk_id).collect())
}
