#![cfg(feature = "mcp-server")]

use std::collections::HashSet;

#[derive(Debug, Clone, Default)]
pub struct McpPolicy {
    allow_apply: bool,
    allowed_tools: HashSet<String>,
}

impl McpPolicy {
    pub fn from_env_and_args(args: &super::McpCmd) -> Self {
        let mut policy = McpPolicy::default();

        if let Ok(val) = std::env::var("MCP_ALLOW_APPLY") {
            policy.allow_apply = matches!(val.as_str(), "1" | "true" | "TRUE" | "yes" | "YES");
        }

        if let Ok(csv) = std::env::var("MCP_APPLY_TOOLS") {
            policy.allowed_tools = parse_csv(&csv);
        }

        if args.allow_apply { policy.allow_apply = true; }

        if let Some(csv) = &args.allow_tools {
            let cli_tools = parse_csv(csv);
            policy.allowed_tools.extend(cli_tools);
        }

        policy
    }

    pub fn is_apply_allowed(&self, tool: &str) -> bool {
        self.allow_apply || self.allowed_tools.contains(tool)
    }
}

fn parse_csv(input: &str) -> HashSet<String> {
    input
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

