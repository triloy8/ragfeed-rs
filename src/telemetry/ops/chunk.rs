use tracing::Span;
use tracing::info_span;

use crate::telemetry::ctx::{OpMarker, PhaseSpan};

#[derive(Copy, Clone, Debug)]
pub struct Chunk;

#[derive(Copy, Clone, Debug)]
pub enum Phase { Plan, SelectDocs, Tokenize, InsertChunk, UpdateStatus }

impl PhaseSpan for Phase {
    fn name(&self) -> &'static str { match self {
        Phase::Plan => "plan",
        Phase::SelectDocs => "select_docs",
        Phase::Tokenize => "tokenize",
        Phase::InsertChunk => "insert_chunk",
        Phase::UpdateStatus => "update_status",
    }}
    fn span(&self) -> Span { match self {
        Phase::Plan => info_span!("plan"),
        Phase::SelectDocs => info_span!("select_docs"),
        Phase::Tokenize => info_span!("tokenize"),
        Phase::InsertChunk => info_span!("insert_chunk"),
        Phase::UpdateStatus => info_span!("update_status"),
    }}
}

impl OpMarker for Chunk {
    const NAME: &'static str = "chunk";
    type Phase = Phase;
    fn root_span() -> Span { info_span!("chunk") }
}

