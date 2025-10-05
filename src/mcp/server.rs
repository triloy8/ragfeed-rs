#![cfg(feature = "mcp-server")]

use std::sync::Arc;

use anyhow::{Context, Result};
use serde_json::json;
use rmcp::{
    ErrorData as McpError,
    ServiceExt,
    handler::server::ServerHandler,
    model::{
        CallToolRequestParam,
        CallToolResult,
        Implementation,
        ListToolsResult,
        PaginatedRequestParam,
        ProtocolVersion,
        ServerCapabilities,
        ServerInfo,
    },
    service::{QuitReason, RequestContext, RoleServer},
};
use sqlx::PgPool;
use tokio::io::{stdin, stdout};
use tokio::sync::Semaphore;

use crate::mcp::adapter::McpSink;
use crate::mcp::policy::McpPolicy;
use crate::mcp::tools;
use crate::telemetry::{install_sink, OutputSink, SinkGuard};

use super::McpCmd;

#[derive(Clone)]
struct RagMcpServer {
    pool: PgPool,
    info: ServerInfo,
    sink: Arc<McpSink>,
    policy: McpPolicy,
    semaphore: Arc<Semaphore>,
}

impl RagMcpServer {
    fn new(pool: PgPool, sink: Arc<McpSink>, policy: McpPolicy, semaphore: Arc<Semaphore>) -> Self {
        let capabilities = ServerCapabilities::builder().enable_tools().build();
        let info = ServerInfo {
            protocol_version: ProtocolVersion::LATEST,
            capabilities,
            server_info: Implementation {
                name: "rag".to_string(),
                title: Some("rag MCP server".to_string()),
                version: env!("CARGO_PKG_VERSION").to_string(),
                icons: None,
                website_url: None,
            },
            instructions: Some(
                "Plan-only mode is enforced by default. Apply requires explicit policy enablement.".to_string(),
            ),
        };

        Self { pool, info, sink, policy, semaphore }
    }

    fn install_sink(&self) -> SinkGuard {
        let dyn_sink: Arc<dyn OutputSink> = self.sink.clone();
        install_sink(dyn_sink)
    }
}

impl ServerHandler for RagMcpServer {
    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParam>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, McpError>> + Send + '_ {
        let _ = &self.pool;
        async move { Ok(ListToolsResult::with_all_items(tools::tool_catalog())) }
    }

    fn get_info(&self) -> ServerInfo { self.info.clone() }

    fn call_tool(
        &self,
        request: CallToolRequestParam,
        context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<CallToolResult, McpError>> + Send + '_ {
        let pool = self.pool.clone();
        let sink = self.sink.clone();
        let policy = self.policy.clone();
        let ct = context.ct.clone();
        let peer = context.peer.clone();
        let permits = self.semaphore.clone();
        async move {
            let permit = permits
                .acquire_owned()
                .await
                .map_err(|err| rmcp::ErrorData::internal_error(
                    "failed to acquire concurrency permit",
                    Some(json!({ "reason": err.to_string() })),
                ))?;
            sink.drain();
            let result = tools::handle_call(&pool, sink.as_ref(), &policy, &ct, request).await;
            let captured = sink.drain();
            for message in captured {
                let param = message.into_logging_notification();
                if let Err(err) = peer.notify_logging_message(param).await {
                    tracing::warn!(target = "rag::mcp", error = %err, "failed to send logging notification");
                }
            }
            drop(permit);
            result
        }
    }
}

pub async fn run_server(pool: PgPool, _cmd: McpCmd) -> Result<()> {
    let policy = McpPolicy::from_env_and_args(&_cmd);
    let max_concurrency = _cmd
        .max_concurrency
        .or_else(|| std::env::var("MCP_MAX_CONCURRENCY").ok().and_then(|s| s.parse().ok()))
        .unwrap_or(2)
        .max(1);
    let sink = Arc::new(McpSink::new());
    let semaphore = Arc::new(Semaphore::new(max_concurrency));
    let server = RagMcpServer::new(pool, sink, policy, semaphore);

    let _telemetry_guard = server.install_sink();

    tracing::info!(target = "rag::mcp", "Starting rmcp server over stdio");
    let transport = (stdin(), stdout());
    let running = server
        .serve(transport)
        .await
        .context("failed to initialize MCP server")?;

    match running.waiting().await {
        Ok(QuitReason::Closed) => {
            tracing::info!(target = "rag::mcp", "MCP transport closed by peer");
            Ok(())
        }
        Ok(QuitReason::Cancelled) => {
            tracing::info!(target = "rag::mcp", "MCP server cancelled by request");
            Ok(())
        }
        Ok(QuitReason::JoinError(err)) => {
            tracing::error!(target = "rag::mcp", error = %err, "MCP server task aborted");
            Err(err.into())
        }
        Err(err) => {
            tracing::error!(target = "rag::mcp", error = %err, "MCP server join failure");
            Err(err.into())
        }
    }
}
