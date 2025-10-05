use serde::Serialize;

// Plan envelope types
#[derive(Serialize)]
pub struct FeedSample { pub feed_id: i32, pub url: String, pub name: Option<String> }

#[derive(Serialize)]
pub struct IngestPlan { pub feeds: usize, pub mode: String, pub limit: usize, pub sample_feeds: Vec<FeedSample> }

// Apply/result envelope types
#[derive(Serialize)]
pub struct FeedSummary { pub feed_id: i32, pub inserted: usize, pub updated: usize, pub skipped: usize, pub errors: usize }

#[derive(Serialize)]
pub struct IngestTotals { pub inserted: usize, pub updated: usize, pub skipped: usize, pub errors: usize }

#[derive(Serialize)]
pub struct IngestApply { pub totals: IngestTotals, pub per_feed: Vec<FeedSummary> }

