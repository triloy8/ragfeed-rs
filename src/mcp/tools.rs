#![cfg(feature = "mcp-server")]

use rmcp::model::{CallToolRequestParam, CallToolResult, Tool, ToolAnnotations};
use serde_json::{json, Map, Value};
use sqlx::PgPool;
use url::Url;

use crate::feed::{self, types::FeedAddPlan};
use crate::mcp::adapter::McpSink;
use crate::mcp::policy::McpPolicy;
use crate::mcp::types::{FeedAddParams, FeedListParams, QueryRunParams};
use crate::query::{self, QueryRequest};
use tokio_util::sync::CancellationToken;

const FEED_ADD_TOOL: &str = "feed.add";
const FEED_LS_TOOL: &str = "feed.ls";
const QUERY_RUN_TOOL: &str = "query.run";

pub fn tool_catalog() -> Vec<Tool> {
    vec![feed_add_tool(), feed_ls_tool(), query_run_tool()]
}

fn feed_add_tool() -> Tool {
    Tool::new(
        FEED_ADD_TOOL,
        "Add or update a feed (plan-only by default)",
        rmcp::object!({"type": "object"}),
    )
    .with_input_schema::<FeedAddParams>()
    .annotate(
        ToolAnnotations::new()
            .read_only(false)
            .destructive(false)
            .idempotent(true),
    )
}

fn feed_ls_tool() -> Tool {
    Tool::new(
        FEED_LS_TOOL,
        "List configured feeds",
        rmcp::object!({"type": "object"}),
    )
    .with_input_schema::<FeedListParams>()
    .annotate(ToolAnnotations::new().read_only(true).idempotent(true))
}

fn query_run_tool() -> Tool {
    Tool::new(
        QUERY_RUN_TOOL,
        "Run a similarity query over indexed chunks",
        rmcp::object!({"type": "object"}),
    )
    .with_input_schema::<QueryRunParams>()
    .annotate(ToolAnnotations::new().read_only(true).open_world(false))
}

pub async fn handle_call(
    pool: &PgPool,
    _sink: &McpSink,
    policy: &McpPolicy,
    ct: &CancellationToken,
    request: CallToolRequestParam,
) -> Result<CallToolResult, rmcp::ErrorData> {
    let CallToolRequestParam { name, arguments } = request;
    match name.as_ref() {
        FEED_ADD_TOOL => feed_add(pool, policy, arguments).await,
        FEED_LS_TOOL => feed_list(pool, ct, arguments).await,
        QUERY_RUN_TOOL => query_run(pool, ct, arguments).await,
        _ => Err(rmcp::ErrorData::invalid_params(
            format!("unknown tool: {}", name),
            None,
        )),
    }
}

async fn feed_add(
    pool: &PgPool,
    policy: &McpPolicy,
    arguments: Option<rmcp::model::JsonObject>,
) -> Result<CallToolResult, rmcp::ErrorData> {
    let _ = pool; // plan-only path does not access the database yet

    let args_map = arguments.unwrap_or_else(Map::new);
    let params: FeedAddParams = serde_json::from_value(Value::Object(args_map)).map_err(|err| {
        rmcp::ErrorData::invalid_params(
            format!("invalid feed.add parameters: {}", err),
            None,
        )
    })?;

    let apply_requested = params.apply.unwrap_or(false);
    let apply_allowed = policy.is_apply_allowed(FEED_ADD_TOOL);
    let apply_effective = apply_requested && apply_allowed;

    let active = params.active.unwrap_or(true);
    if Url::parse(&params.url).is_err() {
        return Err(rmcp::ErrorData::invalid_params(
            "invalid url",
            Some(json!({ "url": params.url })),
        ));
    }

    let plan = FeedAddPlan {
        action: "add",
        url: params.url.clone(),
        name: params.name.clone(),
        active,
    };

    crate::telemetry::feed()
        .plan(&plan)
        .map_err(|err| rmcp::ErrorData::internal_error(
            "failed to emit plan",
            Some(json!({ "reason": err.to_string() })),
        ))?;

    let plan_json = serde_json::to_value(&plan).map_err(|err| {
        rmcp::ErrorData::internal_error(
            "failed to encode plan",
            Some(json!({ "reason": err.to_string() })),
        )
    })?;

    let mut response = json!({
        "op": FEED_ADD_TOOL,
        "planned": true,
        "apply_requested": apply_requested,
        "apply_allowed": apply_allowed,
        "apply_effective": apply_effective,
        "plan": plan_json,
    });

    if apply_requested && !apply_effective {
        response
            .as_object_mut()
            .expect("response is object")
            .insert(
                "apply_denied_reason".to_string(),
                Value::String("policy requires plan-only mode".to_string()),
            );
    }

    Ok(CallToolResult::structured(response))
}

async fn feed_list(
    pool: &PgPool,
    ct: &CancellationToken,
    arguments: Option<rmcp::model::JsonObject>,
) -> Result<CallToolResult, rmcp::ErrorData> {
    let args_map = arguments.unwrap_or_else(Map::new);
    let params: FeedListParams = serde_json::from_value(Value::Object(args_map)).map_err(|err| {
        rmcp::ErrorData::invalid_params(
            format!("invalid feed.ls parameters: {}", err),
            None,
        )
    })?;

    let feeds = tokio::select! {
        _ = ct.cancelled() => {
            return Err(rmcp::ErrorData::internal_error("feed.ls cancelled", None));
        }
        res = feed::list_feeds(pool, params.active) => {
            res.map_err(|err| {
                rmcp::ErrorData::internal_error(
                    "failed to list feeds",
                    Some(json!({ "reason": err.to_string() })),
                )
            })?
        }
    };

    let response = json!({
        "op": FEED_LS_TOOL,
        "filters": { "active": params.active },
        "feeds": feeds,
        "count": feeds.len(),
    });

    Ok(CallToolResult::structured(response))
}

async fn query_run(
    pool: &PgPool,
    ct: &CancellationToken,
    arguments: Option<rmcp::model::JsonObject>,
) -> Result<CallToolResult, rmcp::ErrorData> {
    let args_map = arguments.unwrap_or_else(Map::new);
    let params: QueryRunParams = serde_json::from_value(Value::Object(args_map)).map_err(|err| {
        rmcp::ErrorData::invalid_params(
            format!("invalid query.run parameters: {}", err),
            None,
        )
    })?;

    let request = QueryRequest::from_mcp_params(&params);
    let execution = query::execute_query(pool, request, Some(ct))
        .await
        .map_err(|err| {
            if query::is_cancelled_error(&err) {
                rmcp::ErrorData::internal_error("query.run cancelled", None)
            } else {
                rmcp::ErrorData::internal_error(
                    "query execution failed",
                    Some(json!({ "reason": err.to_string() })),
                )
            }
        })?;

    let telemetry_log = crate::telemetry::query();
    if let Err(err) = telemetry_log.plan(&execution.plan) {
        tracing::warn!(target = "rag::mcp", error = %err, "failed to emit query plan telemetry");
    }
    if let Err(err) = telemetry_log.result(&execution.rows) {
        tracing::warn!(target = "rag::mcp", error = %err, "failed to emit query result telemetry");
    }

    let json_value = json!({
        "op": QUERY_RUN_TOOL,
        "plan": execution.plan,
        "rows": execution.rows,
    });

    Ok(CallToolResult::structured(json_value))
}
