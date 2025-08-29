use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, ValueEnum};
use hf_hub::api::sync::Api;
use ndarray::{s, Array2, Array3, ArrayD, Axis};
use sqlx::PgPool;

use crate::tokenizer::E5Tokenizer;

// pgvector type for sqlx
use pgvector::Vector as PgVector;

// onnx runtime (ORT)
use ort::session::Session;
use ort::session::builder::{GraphOptimizationLevel, SessionBuilder};
use ort::inputs;
use ort::value::Value;

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum Device {
    #[value(name = "cpu")] Cpu,
    #[value(name = "cuda")] Cuda,
}

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
    let tok = E5Tokenizer::new().context("init E5 tokenizer")?;

    // resolve ONNX file from HF Hub
    let onnx_path = resolve_onnx(&args.model_id, args.onnx_filename.as_deref())
        .context("resolve ONNX model via HF Hub")?;

    // build ORT session with CPU or CUDA
    let mut session = build_session(&onnx_path, args.device)?;

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

            let embeddings = embed_batch(&mut session, &tok, &texts)?; // Vec<Vec<f32>>

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

        let embeddings = embed_batch(&mut session, &tok, &texts)?; // Vec<Vec<f32>>

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

fn resolve_onnx(model_id: &str, onnx_filename: Option<&str>) -> Result<std::path::PathBuf> {
    let api = Api::new()?;
    let repo = api.model(model_id.to_string());

    // try explicit filename if provided
    if let Some(name) = onnx_filename {
        let p = repo.get(name)?;
        return Ok(p);
    }

    // try a few common filenames
    let candidates = [
        "onnx/model.onnx",
        "model.onnx",
        "e5-small-v2.onnx",
    ];
    for name in candidates {
        if let Ok(p) = repo.get(name) {
            return Ok(p);
        }
    }

    bail!("Could not find an ONNX file in {model_id}. Pass --onnx-filename to override.")
}

fn build_session(onnx_path: &std::path::Path, device: Device) -> Result<Session> {
    let builder = SessionBuilder::new()
        .map_err(|e| anyhow!("{}", e))?
        .with_optimization_level(GraphOptimizationLevel::Level3)
        .map_err(|e| anyhow!("{}", e))?;

    #[allow(unreachable_code)]
    let builder = match device {
        Device::Cpu => builder,
        Device::Cuda => {
            #[cfg(feature = "cuda")]
            {
                use ort::execution_providers::CUDAExecutionProvider;
                builder
                    .with_execution_providers([CUDAExecutionProvider::default().into()])
                    .map_err(|e| anyhow!("{}", e))?
            }
            #[cfg(not(feature = "cuda"))]
            {
                bail!("Binary built without CUDA support. Rebuild with `--features cuda` and ensure CUDA is available.")
            }
        }
    };

    let model_bytes = std::fs::read(onnx_path).map_err(|e| anyhow!("{}", e))?;
    let session = builder
        .commit_from_memory(&model_bytes)
        .map_err(|e| anyhow!("{}", e))?;
    Ok(session)
}

fn embed_batch(session: &mut Session, tok: &E5Tokenizer, texts: &[String]) -> Result<Vec<Vec<f32>>> {
    if texts.is_empty() { return Ok(vec![]); }

    // prepare prefixed inputs for passages and encode using tokenizer helper
    let inputs: Vec<String> = texts.iter().map(|t| format!("passage: {}", t)).collect();
    let (ids_vecs, attn_vecs, type_vecs) = tok.raw_batch_encode_ids(&inputs)?;
    let batch = ids_vecs.len();
    if batch == 0 { bail!("tokenizer returned empty encodings"); }
    let max_len = ids_vecs.iter().map(|v| v.len()).max().unwrap_or(0);
    if max_len == 0 { bail!("tokenizer produced zero-length sequences"); }

    // build input tensors
    let mut ids = Array2::<i64>::zeros((batch, max_len));
    let mut mask = Array2::<i64>::zeros((batch, max_len));
    let mut type_ids = Array2::<i64>::zeros((batch, max_len));
    for i in 0..batch {
        let li = ids_vecs[i].len();
        for j in 0..li {
            ids[[i, j]] = ids_vecs[i][j];
            mask[[i, j]] = attn_vecs[i][j];
            type_ids[[i, j]] = type_vecs[i][j];
        }
        // remaining positions stay zero-padded
    }

    // feed standard BERT-style names (most E5 exports use these)
    let input_ids_val = Value::from_array(ids.clone())
        .map_err(|e| anyhow!("{}", e))?;
    let attn_mask_val = Value::from_array(mask.clone())
        .map_err(|e| anyhow!("{}", e))?;
    let type_ids_val = Value::from_array(type_ids.clone())
        .map_err(|e| anyhow!("{}", e))?;

    // many BERT-style ONNX models require token_type_ids; some omit it.
    let outputs = session
        .run(inputs! {
            "input_ids" => &input_ids_val,
            "attention_mask" => &attn_mask_val,
            "token_type_ids" => &type_ids_val,
        })
        .map_err(|e| anyhow!("{}", e))?;

    // try to interpret first output
    let first = outputs
        .iter()
        .next()
        .map(|(_name, value)| value)
        .ok_or_else(|| anyhow!("no outputs from ONNX session"))?;

    // as ndarray
    let arr_view = first.try_extract_array().map_err(|e| anyhow!("{}", e))?;
    let arr: ArrayD<f32> = arr_view.to_owned();
    let embed = match arr.ndim() {
        2 => {
            // [batch, dim]
            let (b, _d) = (arr.shape()[0], arr.shape()[1]);
            let mut out = Vec::with_capacity(b);
            for i in 0..b {
                let v = arr.slice(s![i, ..]).to_owned().to_vec();
                out.push(l2_normalize(v));
            }
            out
        }
        3 => {
            // [batch, seq, dim] -> mean pool using attention_mask
            let (b, _s, d) = (arr.shape()[0], arr.shape()[1], arr.shape()[2]);
            // expand mask to [b, s, 1]
            let mask3 = mask
                .map(|&m| m as f32)
                .insert_axis(Axis(2));
            let arr3: Array3<f32> = arr.into_dimensionality().map_err(|_| anyhow!("expect 3D output"))?;

            let mut out = Vec::with_capacity(b);
            for i in 0..b {
                let hs = arr3.slice(s![i, .., ..]); // [s, d]
                let m = mask3.slice(s![i, .., ..]); // [s, 1]
                let num = (&hs * &m).sum_axis(Axis(0)); // [d]
                let denom = m.sum_axis(Axis(0))[[0]].max(1e-6);
                let mut v = (num / denom).to_vec();
                v = l2_normalize(v);
                if v.len() != d { bail!("pooled dim mismatch"); }
                out.push(v);
            }
            out
        }
        n => bail!("unexpected output rank {n}; expected 2 or 3"),
    };

    Ok(embed)
}

fn l2_normalize(mut v: Vec<f32>) -> Vec<f32> {
    let norm = v.iter().map(|x| (*x as f64) * (*x as f64)).sum::<f64>().sqrt() as f32;
    if norm > 0.0 {
        for x in &mut v { *x /= norm; }
    }
    v
}
