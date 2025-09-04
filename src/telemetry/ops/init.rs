use tracing::Span;
use tracing::info_span;

use crate::telemetry::ctx::{OpMarker, PhaseSpan};

#[derive(Copy, Clone, Debug)]
pub struct Init;

#[derive(Copy, Clone, Debug)]
pub enum Phase { Plan, Migrate }

impl PhaseSpan for Phase {
    fn name(&self) -> &'static str { match self { Phase::Plan => "plan", Phase::Migrate => "migrate" } }
    fn span(&self) -> Span { match self { Phase::Plan => info_span!("plan"), Phase::Migrate => info_span!("migrate") } }
}

impl OpMarker for Init {
    const NAME: &'static str = "init";
    type Phase = Phase;
    fn root_span() -> Span { info_span!("init") }
}

