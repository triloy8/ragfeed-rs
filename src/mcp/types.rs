#![cfg(feature = "mcp-server")]

use serde::{Deserialize, Serialize};
use schemars::JsonSchema;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FeedAddParams {
    pub url: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub active: Option<bool>,
    #[serde(default)]
    pub apply: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FeedListParams {
    #[serde(default)]
    pub active: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryRunParams {
    pub text: String,
    #[serde(default)]
    pub top_n: Option<u32>,
    #[serde(default)]
    pub topk: Option<u32>,
    #[serde(default)]
    pub doc_cap: Option<u32>,
    #[serde(default)]
    pub probes: Option<i32>,
    #[serde(default)]
    pub feed: Option<i32>,
    #[serde(default)]
    pub since: Option<String>,
    #[serde(default)]
    pub show_context: Option<bool>,
}
