use tracing::{info_span, Span};

use crate::telemetry::ctx::{OpMarker, PhaseSpan};

#[derive(Copy, Clone, Debug)]
pub struct Compose;

#[derive(Copy, Clone, Debug)]
pub enum Phase {
    Prepare,
    Retrieve,
    Prompt,
    CallLlm,
    Output,
}

impl PhaseSpan for Phase {
    fn name(&self) -> &'static str {
        match self {
            Phase::Prepare => "prepare",
            Phase::Retrieve => "retrieve",
            Phase::Prompt => "prompt",
            Phase::CallLlm => "call_llm",
            Phase::Output => "output",
        }
    }

    fn span(&self) -> Span {
        match self {
            Phase::Prepare => info_span!("prepare"),
            Phase::Retrieve => info_span!("retrieve"),
            Phase::Prompt => info_span!("prompt"),
            Phase::CallLlm => info_span!("call_llm"),
            Phase::Output => info_span!("output"),
        }
    }
}

impl OpMarker for Compose {
    const NAME: &'static str = "compose";
    type Phase = Phase;

    fn root_span() -> Span {
        info_span!("compose")
    }
}
