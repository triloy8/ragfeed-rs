#![cfg(feature = "mcp-server")]

pub mod server;
pub mod adapter;
pub mod types;
pub mod tools;
pub mod policy;

pub use cli::{run, McpCmd};

mod cli {
    use anyhow::Result;
    use clap::Parser;
    use sqlx::PgPool;

    #[derive(Debug, Parser, Default)]
    #[command(name = "mcp", about = "Start the MCP server (experimental)")]
    pub struct McpCmd {
        #[arg(long, default_value_t = false, help = "Allow apply operations (overrides MCP_ALLOW_APPLY)")]
        pub allow_apply: bool,
        #[arg(long, help = "Comma separated list of tools allowed to apply (overrides MCP_APPLY_TOOLS)")]
        pub allow_tools: Option<String>,
        #[arg(long, help = "Maximum concurrent MCP tool calls (overrides MCP_MAX_CONCURRENCY)")]
        pub max_concurrency: Option<usize>,
    }

    pub async fn run(pool: &PgPool, cmd: McpCmd) -> Result<()> {
        super::server::run_server(pool.clone(), cmd).await
    }
}
