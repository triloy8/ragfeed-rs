use tracing::Span;
use tracing::info_span;

use crate::telemetry::ctx::{OpMarker, PhaseSpan};

#[derive(Copy, Clone, Debug)]
pub struct Gc;

#[derive(Copy, Clone, Debug)]
pub enum Phase { Plan, Count, Delete, FixStatus, DropTemp, Analyze, Vacuum }

impl PhaseSpan for Phase {
    fn name(&self) -> &'static str { match self {
        Phase::Plan => "plan",
        Phase::Count => "count",
        Phase::Delete => "delete",
        Phase::FixStatus => "fix_status",
        Phase::DropTemp => "drop_temp",
        Phase::Analyze => "analyze",
        Phase::Vacuum => "vacuum",
    }}
    fn span(&self) -> Span { match self {
        Phase::Plan => info_span!("plan"),
        Phase::Count => info_span!("count"),
        Phase::Delete => info_span!("delete"),
        Phase::FixStatus => info_span!("fix_status"),
        Phase::DropTemp => info_span!("drop_temp"),
        Phase::Analyze => info_span!("analyze"),
        Phase::Vacuum => info_span!("vacuum"),
    }}
}

impl OpMarker for Gc {
    const NAME: &'static str = "gc";
    type Phase = Phase;
    fn root_span() -> Span { info_span!("gc") }
}

