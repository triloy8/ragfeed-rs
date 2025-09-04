use tracing::Span;
use tracing::info_span;

use crate::telemetry::ctx::{OpMarker, PhaseSpan};

#[derive(Copy, Clone, Debug)]
pub struct Query;

#[derive(Copy, Clone, Debug)]
pub enum Phase { Prepare, EmbedQuery, SetProbes, FetchCandidates, PostFilter, Output }

impl PhaseSpan for Phase {
    fn name(&self) -> &'static str { match self {
        Phase::Prepare => "prepare",
        Phase::EmbedQuery => "embed_query",
        Phase::SetProbes => "set_probes",
        Phase::FetchCandidates => "fetch_candidates",
        Phase::PostFilter => "post_filter",
        Phase::Output => "output",
    }}
    fn span(&self) -> Span { match self {
        Phase::Prepare => info_span!("prepare"),
        Phase::EmbedQuery => info_span!("embed_query"),
        Phase::SetProbes => info_span!("set_probes"),
        Phase::FetchCandidates => info_span!("fetch_candidates"),
        Phase::PostFilter => info_span!("post_filter"),
        Phase::Output => info_span!("output"),
    }}
}

impl OpMarker for Query {
    const NAME: &'static str = "query";
    type Phase = Phase;
    fn root_span() -> Span { info_span!("query") }
}

