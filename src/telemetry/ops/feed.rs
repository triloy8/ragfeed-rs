use tracing::Span;
use tracing::info_span;

use crate::telemetry::ctx::{OpMarker, PhaseSpan};

#[derive(Copy, Clone, Debug)]
pub struct Feed;

#[derive(Copy, Clone, Debug)]
pub enum Phase { Plan, Add, List }

impl PhaseSpan for Phase {
    fn name(&self) -> &'static str { match self { Phase::Plan => "plan", Phase::Add => "add", Phase::List => "list" } }
    fn span(&self) -> Span { match self { Phase::Plan => info_span!("plan"), Phase::Add => info_span!("add"), Phase::List => info_span!("list") } }
}

impl OpMarker for Feed {
    const NAME: &'static str = "feed";
    type Phase = Phase;
    fn root_span() -> Span { info_span!("feed") }
}

