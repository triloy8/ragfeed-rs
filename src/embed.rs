use anyhow::{bail, Context, Result};
use clap::Args;
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
}

pub async fn run(pool: &PgPool, args: EmbedCmd) -> Result<()> {
    // Build encoder (tokenizer + ONNX session)
    let mut encoder = E5Encoder::new(&args.model_id, args.onnx_filename.as_deref(), args.device)?;

    // effective model tag (stored in DB)
    let model_tag = format!("{}@onnx-{}", args.model_id, match args.device { Device::Cpu => "cpu", Device::Cuda => "cuda" });

    let mut total = 0i64;
    let batch = args.batch.max(1);

    // In --force mode, fetch all once (optionally limited by --max) and process in batches, then exit.
    if args.force {
        let rows = fetch_all_chunks(pool, args.max).await?;
        if rows.is_empty() {
            println!("ℹ️  No chunks to embed (force={} model={})", args.force, model_tag);
            return Ok(());
        }

        for chunk in rows.chunks(batch) {
            let chunk_ids: Vec<i64> = chunk.iter().map(|(id, _)| *id).collect();
            let texts: Vec<String> = chunk.iter().map(|(_, t)| t.clone()).collect();

            let embeddings = encoder.embed_passages(&texts)?; // Vec<Vec<f32>>

            // all vectors should have same dim
            let dim = embeddings.get(0).map(|v| v.len()).unwrap_or(0);
            if dim == 0 { bail!("empty embedding dimension"); }
            if dim as i32 != args.dim as i32 {
                bail!("model produced dim={} but --dim={} was specified", dim, args.dim);
            }

            // insert embeddings
            for (chunk_id, vec) in chunk_ids.into_iter().zip(embeddings.into_iter()) {
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
            }

            total += texts.len() as i64;
            println!("✅ embedded {} chunk(s) (total={})", texts.len(), total);
        }

        return Ok(());
    }

    // Default mode: page only missing embeddings until done.
    let mut remaining = args.max.unwrap_or(i64::MAX);
    loop {
        let n = remaining.min(batch as i64) as i64;
        if n <= 0 { break; }

        let rows = fetch_chunks(pool, &model_tag, false, n).await?;
        if rows.is_empty() { break; }

        let chunk_ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
        let texts: Vec<String> = rows.into_iter().map(|(_, t)| t).collect();

        let embeddings = encoder.embed_passages(&texts)?; // Vec<Vec<f32>>

        // all vectors should have same dim
        let dim = embeddings.get(0).map(|v| v.len()).unwrap_or(0);
        if dim == 0 { bail!("empty embedding dimension"); }
        if dim as i32 != args.dim as i32 {
            bail!("model produced dim={} but --dim={} was specified", dim, args.dim);
        }

        // insert embeddings
        for (chunk_id, vec) in chunk_ids.into_iter().zip(embeddings.into_iter()) {
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
        }

        total += texts.len() as i64;
        remaining -= n;
        println!("✅ embedded {} chunk(s) (total={})", texts.len(), total);
    }

    if total == 0 {
        println!("ℹ️  No chunks to embed (force={} model={})", args.force, model_tag);
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
