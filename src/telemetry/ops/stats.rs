use tracing::Span;
use tracing::info_span;

use crate::telemetry::ctx::{OpMarker, PhaseSpan};

#[derive(Copy, Clone, Debug)]
pub struct Stats;

#[derive(Copy, Clone, Debug)]
pub enum Phase { Summary, FeedStats, DocSnapshot, ChunkSnapshot }

impl PhaseSpan for Phase {
    fn name(&self) -> &'static str { match self {
        Phase::Summary => "summary",
        Phase::FeedStats => "feed_stats",
        Phase::DocSnapshot => "doc_snapshot",
        Phase::ChunkSnapshot => "chunk_snapshot",
    }}
    fn span(&self) -> Span { match self {
        Phase::Summary => info_span!("summary"),
        Phase::FeedStats => info_span!("feed_stats"),
        Phase::DocSnapshot => info_span!("doc_snapshot"),
        Phase::ChunkSnapshot => info_span!("chunk_snapshot"),
    }}
}

impl OpMarker for Stats {
    const NAME: &'static str = "stats";
    type Phase = Phase;
    fn root_span() -> Span { info_span!("stats") }
}

