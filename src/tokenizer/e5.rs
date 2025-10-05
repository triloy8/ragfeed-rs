use anyhow::{anyhow, Result};
use hf_hub::api::sync::Api;
use tokenizers::Tokenizer;

#[derive(Debug, Clone)]
pub struct E5Tokenizer {
    inner: Tokenizer,
}

impl E5Tokenizer {
    // force loads intfloat/e5-small-v2 tokenizer from the HF Hub + applies padding/truncation
    pub fn new() -> Result<Self> {
        let mut tok = Tokenizer::from_pretrained("intfloat/e5-small-v2", None)
            .map_err(|e| anyhow!("{}", e))?;

        // read tokenizer_config.json for defaults (model_max_length, padding_side, pad token)
        let (model_max_len, padding_right, pad_id, pad_type_id, pad_token) = {
            let api = Api::new()?;
            let repo = api.model("intfloat/e5-small-v2".to_string());
            let cfg = repo.get("tokenizer_config.json").ok()
                .and_then(|p| std::fs::read_to_string(p).ok())
                .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
                .unwrap_or(serde_json::json!({}));

            let model_max_len = cfg.get("model_max_length").and_then(|v| v.as_u64()).unwrap_or(512) as usize;
            let padding_side_is_right = cfg.get("padding_side").and_then(|v| v.as_str()).map(|s| s != "left").unwrap_or(true);
            let pad_token_str = cfg.get("pad_token").and_then(|v| v.as_str()).unwrap_or("[PAD]").to_string();
            let pad_token_id_cfg = cfg.get("pad_token_id").and_then(|v| v.as_i64());
            let pad_type_id_cfg = cfg.get("pad_token_type_id").and_then(|v| v.as_i64()).unwrap_or(0);

            // resolve pad_id from tokenizer vocab if not provided
            let pad_id = pad_token_id_cfg
                .and_then(|id| u32::try_from(id).ok())
                .or_else(|| tok.token_to_id(&pad_token_str))
                .unwrap_or(0);

            (model_max_len, padding_side_is_right, pad_id, u32::try_from(pad_type_id_cfg).unwrap_or(0), pad_token_str)
        };

        // apply truncation and padding based on tokenizer_config
        tok.with_truncation(Some(tokenizers::TruncationParams {
            max_length: model_max_len,
            stride: 0,
            strategy: tokenizers::TruncationStrategy::LongestFirst,
            direction: tokenizers::TruncationDirection::Right,
        }))
        .map_err(|e| anyhow!("{}", e))?;

        tok.with_padding(Some(tokenizers::PaddingParams {
            strategy: tokenizers::PaddingStrategy::BatchLongest,
            direction: if padding_right { tokenizers::PaddingDirection::Right } else { tokenizers::PaddingDirection::Left },
            pad_to_multiple_of: None,
            pad_id,
            pad_type_id,
            pad_token,
        }));

        Ok(Self { inner: tok })
    }

    /// encode a query: adds "query: " and special tokens
    pub fn ids_query(&self, text: &str) -> Result<Vec<u32>> {
        let enc = self.inner
            .encode(format!("query: {text}"), true)
            .map_err(|e| anyhow!("{}", e))?;
        Ok(enc.get_ids().to_vec())
    }

    /// encode a passage: adds "passage: " and special tokens
    pub fn ids_passage(&self, text: &str) -> Result<Vec<u32>> {
        let enc = self.inner.encode(format!("passage: {text}"), true)
        .map_err(|e| anyhow!("{}", e))?;
        Ok(enc.get_ids().to_vec())
    }

    /// decode token IDs back to text, keeping special tokens and prefixes
    pub fn decode_ids(&self, ids: &[u32]) -> Result<String> {
        self.inner.decode(ids, false)
            .map_err(|e| anyhow!("{}", e))
    }

    // batch-encode raw texts without E5 prefixes
    // returns (input_ids, attention_mask, token_type_ids), each as Vec 
    pub fn raw_batch_encode_ids(
        &self,
        texts: &[String],
    ) -> Result<(Vec<Vec<i64>>, Vec<Vec<i64>>, Vec<Vec<i64>>)> {
        let tok = self.inner.clone();

        let encodings = tok
            .encode_batch(texts.to_vec(), true)
            .map_err(|e| anyhow!("{}", e))?;

        let mut ids_out: Vec<Vec<i64>> = Vec::with_capacity(encodings.len());
        let mut attn_out: Vec<Vec<i64>> = Vec::with_capacity(encodings.len());
        let mut type_out: Vec<Vec<i64>> = Vec::with_capacity(encodings.len());

        for e in encodings {
            ids_out.push(e.get_ids().iter().map(|&x| x as i64).collect());
            attn_out.push(e.get_attention_mask().iter().map(|&x| x as i64).collect());
            // type ids might be empty depending on tokenizer
            let tids = e.get_type_ids();
            if tids.is_empty() {
                type_out.push(vec![0; ids_out.last().map(|v| v.len()).unwrap_or(0)]);
            } else {
                type_out.push(tids.iter().map(|&x| x as i64).collect());
            }
        }

        Ok((ids_out, attn_out, type_out))
    }

    /// access the inner tokenizer if needed
    pub fn inner(&self) -> &Tokenizer { &self.inner }
}

