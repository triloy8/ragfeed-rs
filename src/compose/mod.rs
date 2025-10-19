use anyhow::{Context, Result};
use clap::Args;
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::PgPool;

use crate::llm::openai::{
    ChatCompletionRequest, ChatMessage, ChatRole, LlmClient, OpenAiClient,
    OpenAiClientConfig, OpenAiError,
};
use crate::query::service::{QueryRequest, QueryOutcome};
use crate::telemetry;
use crate::telemetry::ops::compose::Phase as ComposePhase;
use crate::util::time::parse_since_opt;
use crate::encoder::Device;

#[derive(Args, Debug)]
pub struct ComposeCmd {
    query: String,
    #[arg(long, default_value_t = 6)]
    topk: usize,
    #[arg(long, default_value_t = 2)]
    doc_cap: usize,
    #[arg(long, default_value_t = 100)]
    top_n: i64,
    #[arg(long)]
    probes: Option<i32>,
    #[arg(long)]
    feed: Option<i32>,
    #[arg(long)]
    since: Option<String>,
    #[arg(long)]
    model: Option<String>,
    #[arg(long)]
    system: Option<String>,
    #[arg(long)]
    max_tokens: Option<u32>,
    #[arg(long)]
    temperature: Option<f32>,
    #[arg(long)]
    top_p: Option<f32>,
    #[arg(long, default_value_t = false)]
    dry_run: bool,
    #[arg(long, default_value = "intfloat/e5-small-v2")]
    embed_model: String,
    #[arg(long)]
    embed_onnx_filename: Option<String>,
    #[arg(long, value_enum, default_value_t = Device::Cpu)]
    device: Device,
}

#[derive(Serialize)]
struct ComposePlan<'a> {
    query: &'a str,
    model: &'a str,
    embed_model: &'a str,
    system_message: &'a str,
    hit_count: usize,
    dry_run: bool,
    hits: Vec<ComposeHit>,
    prompt_sections: Vec<PromptSection<'a>>,
}

#[derive(Serialize)]
struct ComposeResult<'a> {
    query: &'a str,
    model: String,
    answer: &'a str,
    hits: Vec<ComposeHit>,
    retrieved_chunks: usize,
    usage: Option<UsageDto>,
}

#[derive(Serialize, Clone)]
struct ComposeHit {
    rank: usize,
    doc_id: i64,
    chunk_id: i64,
    title: Option<String>,
    distance: f32,
    preview: Option<String>,
}

#[derive(Serialize)]
struct PromptSection<'a> {
    rank: usize,
    title: &'a str,
    source: &'a str,
}

#[derive(Serialize)]
struct UsageDto {
    prompt_tokens: Option<u32>,
    completion_tokens: Option<u32>,
    total_tokens: Option<u32>,
}

pub async fn run(pool: &PgPool, args: ComposeCmd) -> Result<()> {
    let log = telemetry::compose();
    let _g = log
        .root_span_kv([
            ("top_n", args.top_n.to_string()),
            ("topk", args.topk.to_string()),
            ("doc_cap", args.doc_cap.to_string()),
            ("probes", format!("{:?}", args.probes)),
            ("feed", format!("{:?}", args.feed)),
            ("since", format!("{:?}", args.since)),
            ("model", format!("{:?}", args.model)),
            ("embed_model", args.embed_model.clone()),
            ("embed_onnx", format!("{:?}", args.embed_onnx_filename)),
            ("dry_run", args.dry_run.to_string()),
            ("temperature", format!("{:?}", args.temperature)),
            ("top_p", format!("{:?}", args.top_p)),
            ("max_tokens", format!("{:?}", args.max_tokens)),
            ("device", format!("{:?}", args.device)),
        ])
        .entered();

    let _prepare_span = log.span(&ComposePhase::Prepare).entered();
    let since_ts: Option<DateTime<Utc>> = parse_since_opt(&args.since)?;
    drop(_prepare_span);

    let _retrieve_span = log.span(&ComposePhase::Retrieve).entered();
    let outcome = fetch_hits(pool, &args, since_ts).await?;
    drop(_retrieve_span);

    if outcome.rows.is_empty() {
        let hint = if args.feed.is_some() || args.since.is_some() {
            let mut details = Vec::new();
            if let Some(feed) = args.feed { details.push(format!("feed={feed}")); }
            if let Some(since) = &args.since { details.push(format!("since={since}")); }
            if details.is_empty() {
                "try relaxing filters or ensure content has been ingested, chunked, and embedded".to_string()
            } else {
                format!(
                    "try relaxing filters ({}) or ensure the selected feed has recent chunked + embedded content",
                    details.join(", ")
                )
            }
        } else {
            "ensure documents have been ingested, chunked, and embedded before composing".to_string()
        };
        log.info(format!("ℹ️  No results — {hint}"));
        return Ok(());
    }

    let system_message = args
        .system
        .clone()
        .unwrap_or_else(|| "You are a helpful assistant.".to_string());
    let client_cfg = OpenAiClientConfig::from_env();
    let model_name = args
        .model
        .clone()
        .unwrap_or_else(|| client_cfg.default_model.clone());

    let hits = extract_hits(&outcome);
    let hit_count = hits.len();
    log.info(format!("📚 Retrieved {hit_count} chunk{}", if hit_count == 1 { "" } else { "s" }));

    if args.dry_run {
        let prompt_sections = build_prompt_sections(&outcome);
        let plan = ComposePlan {
            query: &args.query,
            model: &model_name,
            embed_model: &args.embed_model,
            system_message: &system_message,
            hit_count,
            dry_run: args.dry_run,
            hits: hits.clone(),
            prompt_sections,
        };
        log.info("📝 Dry run — skipping LLM call");
        log.plan(&plan)?;
        return Ok(());
    }

    let prompt = build_prompt(&args.query, &outcome);

    let _prompt_span = log.span(&ComposePhase::Prompt).entered();
    log.info("🧠 Calling OpenAI compose endpoint");
    drop(_prompt_span);

    let client = OpenAiClient::new(client_cfg.clone())
        .context("init OpenAI client")?;

    let request = ChatCompletionRequest {
        model: Some(model_name.clone()),
        messages: vec![
            ChatMessage::new(ChatRole::System, system_message.clone()),
            ChatMessage::new(ChatRole::User, prompt.clone()),
        ],
        max_tokens: args.max_tokens,
        temperature: args.temperature,
        top_p: args.top_p,
    };

    let _call_span = log.span(&ComposePhase::CallLlm).entered();
    let response = match client.chat_completion(request).await {
        Ok(resp) => resp,
        Err(err) => {
            match &err {
                OpenAiError::MissingApiKey => {
                    log.warn("⚠️  Missing OPENAI_API_KEY — set it or use --dry-run / OPENAI_BASE_URL for a compatible proxy.");
                }
                OpenAiError::Api { status, error } => {
                    log.warn(format!(
                        "⚠️  OpenAI API error {} — {}",
                        status,
                        error.message
                    ));
                }
                OpenAiError::Timeout => {
                    log.warn("⚠️  OpenAI request timed out — consider retrying or increasing OPENAI_TIMEOUT_SECS.");
                }
                _ => {
                    log.warn("⚠️  OpenAI request failed — see error details below.");
                }
            }
            drop(_call_span);
            return Err(to_anyhow(err).context("call OpenAI chat completion"));
        }
    };
    drop(_call_span);

    let answer = response.content.trim().to_string();
    log.info(format!("💡 Answer:\n{answer}"));

    let usage = response.usage.map(|u| UsageDto {
        prompt_tokens: u.prompt_tokens,
        completion_tokens: u.completion_tokens,
        total_tokens: u.total_tokens,
    });

    let result = ComposeResult {
        query: &args.query,
        model: model_name,
        answer: &answer,
        hits,
        retrieved_chunks: hit_count,
        usage,
    };

    let _out_span = log.span(&ComposePhase::Output).entered();
    log.result(&result)?;
    drop(_out_span);

    Ok(())
}

async fn fetch_hits(
    pool: &PgPool,
    args: &ComposeCmd,
    since: Option<DateTime<Utc>>,
) -> Result<QueryOutcome> {
    let top_n = args.top_n.max(args.topk as i64).max(1);
    let request = QueryRequest {
        query: &args.query,
        top_n,
        topk: args.topk,
        doc_cap: args.doc_cap,
        probes: args.probes,
        feed: args.feed,
        since,
        include_preview: true,
        include_text: true,
        model_id: &args.embed_model,
        onnx_filename: args.embed_onnx_filename.as_deref(),
        device: args.device,
    };

    crate::query::service::execute(pool, request, None).await
}

fn extract_hits(outcome: &QueryOutcome) -> Vec<ComposeHit> {
    outcome
        .rows
        .iter()
        .map(|row| ComposeHit {
            rank: row.rank,
            doc_id: row.doc_id,
            chunk_id: row.chunk_id,
            title: row.title.clone(),
            distance: row.distance,
            preview: row.preview.clone(),
        })
        .collect()
}

fn build_prompt_sections(outcome: &QueryOutcome) -> Vec<PromptSection<'_>> {
    outcome
        .hits
        .iter()
        .map(|hit| PromptSection {
            rank: hit.rank,
            title: hit.title.as_deref().unwrap_or("Untitled"),
            source: hit
                .text
                .as_deref()
                .or(hit.preview.as_deref())
                .unwrap_or("[no excerpt available]"),
        })
        .collect()
}

fn build_prompt(query: &str, outcome: &QueryOutcome) -> String {
    let mut context_blocks: Vec<String> = Vec::new();
    for hit in &outcome.hits {
        let mut block =
            format!("Source #{rank} (doc {doc})", rank = hit.rank, doc = hit.doc_id);
        if let Some(title) = &hit.title {
            block.push_str(&format!(" — {title}"));
        }
        let excerpt = hit
            .text
            .as_deref()
            .or(hit.preview.as_deref())
            .unwrap_or("[no excerpt]");
        block.push_str(&format!("\n{excerpt}"));
        context_blocks.push(block);
    }

    let context = context_blocks.join("\n\n---\n\n");

    format!(
        "Context:\n{context}\n\nQuestion:\n{query}\n\nPlease answer using the provided context. If the answer is not contained within the context, say so explicitly."
    )
}

fn to_anyhow(err: OpenAiError) -> anyhow::Error {
    anyhow::Error::new(err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::service::QueryHit;
    use crate::query::QueryResultRow;

    fn sample_outcome() -> QueryOutcome {
        QueryOutcome {
            rows: vec![QueryResultRow {
                rank: 1,
                distance: 0.12,
                chunk_id: 7,
                doc_id: 3,
                title: Some("Doc title".into()),
                preview: Some("preview text".into()),
            }],
            hits: vec![QueryHit {
                rank: 1,
                distance: 0.12,
                chunk_id: 7,
                doc_id: 3,
                title: Some("Doc title".into()),
                preview: Some("preview text".into()),
                text: Some("full chunk text".into()),
            }],
            probes: Some(4),
        }
    }

    #[test]
    fn build_prompt_includes_question_and_context() {
        let outcome = sample_outcome();
        let prompt = build_prompt("What is rust?", &outcome);
        assert!(prompt.contains("What is rust?"));
        assert!(prompt.contains("full chunk text"));
        assert!(prompt.contains("Source #1"));
    }

    #[test]
    fn extract_hits_captures_rank_and_preview() {
        let outcome = sample_outcome();
        let hits = extract_hits(&outcome);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].rank, 1);
        assert_eq!(hits[0].chunk_id, 7);
        assert_eq!(hits[0].preview.as_deref(), Some("preview text"));
    }
}
