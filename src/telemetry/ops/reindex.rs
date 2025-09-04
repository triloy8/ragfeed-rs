use tracing::Span;
use tracing::info_span;

use crate::telemetry::ctx::{OpMarker, PhaseSpan};

#[derive(Copy, Clone, Debug)]
pub struct Reindex;

#[derive(Copy, Clone, Debug)]
pub enum Phase { Plan, CreateIndex, Reindex, Swap, Analyze }

impl PhaseSpan for Phase {
    fn name(&self) -> &'static str { match self {
        Phase::Plan => "plan",
        Phase::CreateIndex => "create_index",
        Phase::Reindex => "reindex",
        Phase::Swap => "swap",
        Phase::Analyze => "analyze",
    }}
    fn span(&self) -> Span { match self {
        Phase::Plan => info_span!("plan"),
        Phase::CreateIndex => info_span!("create_index"),
        Phase::Reindex => info_span!("reindex"),
        Phase::Swap => info_span!("swap"),
        Phase::Analyze => info_span!("analyze"),
    }}
}

impl OpMarker for Reindex {
    const NAME: &'static str = "reindex";
    type Phase = Phase;
    fn root_span() -> Span { info_span!("reindex") }
}

