use tracing::Span;
use tracing::info_span;

use crate::telemetry::ctx::{OpMarker, PhaseSpan};

#[derive(Copy, Clone, Debug)]
pub struct Ingest;

#[derive(Copy, Clone, Debug)]
pub enum Phase { Feed, FetchRss, ParseRss, FetchItem, Extract, WriteDoc }

impl PhaseSpan for Phase {
    fn name(&self) -> &'static str { match self {
        Phase::Feed => "feed",
        Phase::FetchRss => "fetch_rss",
        Phase::ParseRss => "parse_rss",
        Phase::FetchItem => "fetch_item",
        Phase::Extract => "extract",
        Phase::WriteDoc => "write_doc",
    }}
    fn span(&self) -> Span { match self {
        Phase::Feed => info_span!("feed"),
        Phase::FetchRss => info_span!("fetch_rss"),
        Phase::ParseRss => info_span!("parse_rss"),
        Phase::FetchItem => info_span!("fetch_item"),
        Phase::Extract => info_span!("extract"),
        Phase::WriteDoc => info_span!("write_doc"),
    }}
}

impl OpMarker for Ingest {
    const NAME: &'static str = "ingest";
    type Phase = Phase;
    fn root_span() -> Span { info_span!("ingest") }
}

