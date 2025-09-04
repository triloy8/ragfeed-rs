use serde::Serialize;
use chrono::{DateTime, Utc};

// Summary view types
#[derive(Serialize)]
pub struct StatsFeedRow { pub feed_id: i32, pub name: Option<String>, pub url: String, pub is_active: Option<bool>, pub added_at: Option<DateTime<Utc>> }
#[derive(Serialize)]
pub struct StatsDocStatus { pub status: String, pub cnt: i64 }
#[derive(Serialize)]
pub struct StatsChunksSummary { pub total: i64, pub avg_tokens: f64 }
#[derive(Serialize)]
pub struct StatsModelInfo { pub model: String, pub cnt: i64, pub last: Option<DateTime<Utc>> }
#[derive(Serialize)]
pub struct StatsEmbeddings { pub total: i64, pub models: Vec<StatsModelInfo> }
#[derive(Serialize)]
pub struct StatsIndexMeta { pub lists: Option<i32>, pub size_pretty: Option<String>, pub last_analyze: Option<DateTime<Utc>> }
#[derive(Serialize)]
pub struct StatsCoverage { pub chunks: i64, pub embedded: i64, pub pct: f64, pub missing: i64 }
#[derive(Serialize)]
pub struct StatsSummary {
    pub feeds: Vec<StatsFeedRow>,
    pub documents_by_status: Vec<StatsDocStatus>,
    pub last_fetched: Option<DateTime<Utc>>,
    pub chunks: StatsChunksSummary,
    pub embeddings: StatsEmbeddings,
    pub index: StatsIndexMeta,
    pub coverage: StatsCoverage,
}

// Feed view types
#[derive(Serialize)]
pub struct StatsFeedMeta { pub feed_id: i32, pub name: Option<String>, pub url: String, pub is_active: Option<bool>, pub added_at: Option<DateTime<Utc>> }
#[derive(Serialize)]
pub struct StatsFeedCoverage { pub chunks: i64, pub embedded: i64, pub pct: f64, pub last: Option<DateTime<Utc>> }
#[derive(Serialize)]
pub struct StatsPendingTopDoc { pub doc_id: i64, pub source_title: Option<String>, pub pending: i64 }
#[derive(Serialize)]
pub struct StatsLatestDoc { pub doc_id: i64, pub status: Option<String>, pub fetched_at: Option<DateTime<Utc>>, pub source_title: Option<String> }
#[derive(Serialize)]
pub struct StatsFeedStats {
    pub feed: StatsFeedMeta,
    pub documents_by_status: Vec<StatsDocStatus>,
    pub last_fetched: Option<DateTime<Utc>>,
    pub chunks: StatsChunksSummary,
    pub coverage: StatsFeedCoverage,
    pub missing: i64,
    pub models: Vec<StatsModelInfo>,
    pub pending_top_docs: Vec<StatsPendingTopDoc>,
    pub latest_docs: Vec<StatsLatestDoc>,
}

// Chunk/doc snapshots
#[derive(Serialize)]
pub struct StatsChunkSnap { pub chunk_id: i64, pub doc_id: Option<i64>, pub chunk_index: Option<i32>, pub token_count: Option<i32>, pub preview: Option<String> }

// Doc view snapshot types
#[derive(Serialize)]
pub struct StatsDocInfo {
    pub doc_id: i64,
    pub feed_id: Option<i32>,
    pub source_url: String,
    pub source_title: Option<String>,
    pub published_at: Option<DateTime<Utc>>,
    pub fetched_at: Option<DateTime<Utc>>,
    pub status: Option<String>,
    pub error_msg: Option<String>,
    pub preview: Option<String>,
}

#[derive(Serialize)]
pub struct StatsDocChunkInfo { pub chunk_id: i64, pub chunk_index: Option<i32>, pub token_count: Option<i32> }

#[derive(Serialize)]
pub struct StatsDocSnapshot { pub doc: StatsDocInfo, pub chunks: Vec<StatsDocChunkInfo> }
