use tracing::Span;
use tracing::info_span;

use crate::telemetry::ctx::{OpMarker, PhaseSpan};

#[derive(Copy, Clone, Debug)]
pub struct Embed;

#[derive(Copy, Clone, Debug)]
pub enum Phase { Plan, CountCandidates, LoadModel, FetchBatch, Encode, InsertEmbedding }

impl PhaseSpan for Phase {
    fn name(&self) -> &'static str { match self {
        Phase::Plan => "plan",
        Phase::CountCandidates => "count_candidates",
        Phase::LoadModel => "load_model",
        Phase::FetchBatch => "fetch_batch",
        Phase::Encode => "encode",
        Phase::InsertEmbedding => "insert_embedding",
    }}
    fn span(&self) -> Span { match self {
        Phase::Plan => info_span!("plan"),
        Phase::CountCandidates => info_span!("count_candidates"),
        Phase::LoadModel => info_span!("load_model"),
        Phase::FetchBatch => info_span!("fetch_batch"),
        Phase::Encode => info_span!("encode"),
        Phase::InsertEmbedding => info_span!("insert_embedding"),
    }}
}

impl OpMarker for Embed {
    const NAME: &'static str = "embed";
    type Phase = Phase;
    fn root_span() -> Span { info_span!("embed") }
}

