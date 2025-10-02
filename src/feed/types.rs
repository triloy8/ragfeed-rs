use serde::Serialize;
use crate::stats::types::StatsFeedRow;

#[derive(Serialize)]
pub struct FeedAddPlan {
    pub action: &'static str,
    pub url: String,
    pub name: Option<String>,
    pub active: bool,
}

#[derive(Serialize)]
pub struct FeedAddResult {
    pub inserted: bool,
    pub url: String,
}

#[derive(Serialize)]
pub struct FeedList {
    pub feeds: Vec<StatsFeedRow>,
}

