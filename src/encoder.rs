use anyhow::{anyhow, bail, Context, Result};
use hf_hub::api::sync::Api;
use ndarray::{s, Array2, Array3, ArrayD, Axis};

use crate::tokenizer::E5Tokenizer;

// onnx runtime (ORT)
use ort::session::Session;
use ort::session::builder::{GraphOptimizationLevel, SessionBuilder};
use ort::inputs;
use ort::value::Value;

#[derive(Copy, Clone, Debug, clap::ValueEnum)]
pub enum Device {
    #[value(name = "cpu")] Cpu,
    #[value(name = "cuda")] Cuda,
}

pub struct E5Encoder {
    tok: E5Tokenizer,
    session: Session,
}

impl E5Encoder {
    pub fn new(model_id: &str, onnx_filename: Option<&str>, device: Device) -> Result<Self> {
        let tok = E5Tokenizer::new().context("init E5 tokenizer")?;
        let onnx_path = resolve_onnx(model_id, onnx_filename).context("resolve ONNX model via HF Hub")?;
        let session = build_session(&onnx_path, device)?;
        Ok(Self { tok, session })
    }

    pub fn embed_queries(&mut self, queries: &[String]) -> Result<Vec<Vec<f32>>> {
        self.embed_with_prefix(queries, "query: ")
    }

    pub fn embed_passages(&mut self, passages: &[String]) -> Result<Vec<Vec<f32>>> {
        self.embed_with_prefix(passages, "passage: ")
    }

    pub fn embed_query(&mut self, query: &str) -> Result<Vec<f32>> {
        let out = self.embed_queries(&[query.to_string()])?;
        out.into_iter().next().ok_or_else(|| anyhow!("no vector produced"))
    }

    fn embed_with_prefix(&mut self, texts: &[String], prefix: &str) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() { return Ok(vec![]); }

        // Prepare inputs with E5 prefix
        let inputs: Vec<String> = texts.iter().map(|t| format!("{}{}", prefix, t)).collect();
        let (ids_vecs, attn_vecs, type_vecs) = self.tok.raw_batch_encode_ids(&inputs)?;
        let batch = ids_vecs.len();
        if batch == 0 { bail!("tokenizer returned empty encodings"); }
        let max_len = ids_vecs.iter().map(|v| v.len()).max().unwrap_or(0);
        if max_len == 0 { bail!("tokenizer produced zero-length sequences"); }

        // Build input tensors
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
        }

        // Feed standard BERT-style names
        let input_ids_val = Value::from_array(ids.clone()).map_err(|e| anyhow!("{}", e))?;
        let attn_mask_val = Value::from_array(mask.clone()).map_err(|e| anyhow!("{}", e))?;
        let type_ids_val = Value::from_array(type_ids.clone()).map_err(|e| anyhow!("{}", e))?;

        let outputs = self.session
            .run(inputs! {
                "input_ids" => &input_ids_val,
                "attention_mask" => &attn_mask_val,
                "token_type_ids" => &type_ids_val,
            })
            .map_err(|e| anyhow!("{}", e))?;

        // First output as ndarray
        let first = outputs.iter().next().map(|(_n,v)| v).ok_or_else(|| anyhow!("no outputs from ONNX session"))?;
        let arr_view = first.try_extract_array().map_err(|e| anyhow!("{}", e))?;
        let arr: ArrayD<f32> = arr_view.to_owned();
        let embed = match arr.ndim() {
            2 => {
                // [batch, dim]
                let b = arr.shape()[0];
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
                let mask3 = mask.map(|&m| m as f32).insert_axis(Axis(2));
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
}

fn l2_normalize(mut v: Vec<f32>) -> Vec<f32> {
    let norm = v.iter().map(|x| (*x as f64) * (*x as f64)).sum::<f64>().sqrt() as f32;
    if norm > 0.0 {
        for x in &mut v { *x /= norm; }
    }
    v
}

fn resolve_onnx(model_id: &str, onnx_filename: Option<&str>) -> Result<std::path::PathBuf> {
    let api = Api::new()?;
    let repo = api.model(model_id.to_string());

    if let Some(name) = onnx_filename {
        let p = repo.get(name)?;
        return Ok(p);
    }

    let candidates = [
        "onnx/model.onnx",
        "model.onnx",
        "e5-small-v2.onnx",
    ];
    for name in candidates {
        if let Ok(p) = repo.get(name) { return Ok(p); }
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

