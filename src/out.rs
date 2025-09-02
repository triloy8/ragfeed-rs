use serde::Serialize;
use serde_json::json;
use std::io::{self, Write};
use std::sync::OnceLock;
use std::marker::PhantomData;

use tracing::{info, debug, warn, error, info_span, Span};

static JSON_MODE: OnceLock<bool> = OnceLock::new();

pub fn set_json_mode(v: bool) {
    let _ = JSON_MODE.set(v);
}

pub fn json_mode() -> bool {
    *JSON_MODE.get().unwrap_or(&false)
}

pub fn logs_are_json() -> bool {
    matches!(std::env::var("RAG_LOG_FORMAT").as_deref(), Ok("json"))
}

#[derive(Serialize)]
pub struct Meta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
}

// Type-state LogCtx: specialized per op. Start with Ingest only.
pub mod ingest {
    #[derive(Copy, Clone, Debug)]
    pub struct Ingest;

    #[derive(Copy, Clone, Debug)]
    pub enum Phase {
        Feed,
        FetchRss,
        ParseRss,
        FetchItem,
        Extract,
        WriteDoc,
    }
    // name() and span() implemented via crate-level traits below
}

// Type-state LogCtx: init command
pub mod init {
    #[derive(Copy, Clone, Debug)]
    pub struct Init;

    #[derive(Copy, Clone, Debug)]
    pub enum Phase {
        Plan,
        Migrate,
    }
    // name() and span() via traits below
}

// Type-state LogCtx: feed command
pub mod feed {
    #[derive(Copy, Clone, Debug)]
    pub struct Feed;

    #[derive(Copy, Clone, Debug)]
    pub enum Phase {
        Plan,
        Add,
        List,
    }
    // name() and span() via traits below
}

// Type-state LogCtx: chunk command
pub mod chunk {
    #[derive(Copy, Clone, Debug)]
    pub struct Chunk;

    #[derive(Copy, Clone, Debug)]
    pub enum Phase {
        Plan,
        SelectDocs,
        Tokenize,
        InsertChunk,
        UpdateStatus,
    }
}

// Type-state LogCtx: embed command
pub mod embed {
    #[derive(Copy, Clone, Debug)]
    pub struct Embed;

    #[derive(Copy, Clone, Debug)]
    pub enum Phase {
        Plan,
        CountCandidates,
        LoadModel,
        FetchBatch,
        Encode,
        InsertEmbedding,
    }
}

// Traits to avoid duplication while keeping literal span names
pub trait PhaseSpan {
    fn name(&self) -> &'static str;
    fn span(&self) -> Span;
}

pub trait OpMarker {
    const NAME: &'static str;
    type Phase: PhaseSpan;
    fn root_span() -> Span;
}

pub struct LogCtx<O: OpMarker> {
    json: bool,
    _marker: PhantomData<O>,
}

pub fn ingest() -> LogCtx<ingest::Ingest> { LogCtx { json: logs_are_json(), _marker: PhantomData } }
pub fn init() -> LogCtx<init::Init> { LogCtx { json: logs_are_json(), _marker: PhantomData } }
pub fn feed() -> LogCtx<feed::Feed> { LogCtx { json: logs_are_json(), _marker: PhantomData } }
pub fn chunk() -> LogCtx<chunk::Chunk> { LogCtx { json: logs_are_json(), _marker: PhantomData } }
pub fn embed() -> LogCtx<embed::Embed> { LogCtx { json: logs_are_json(), _marker: PhantomData } }

impl<O: OpMarker> LogCtx<O> {
    fn op_name(&self) -> &'static str { O::NAME }

    pub fn root_span(&self) -> Span { O::root_span() }

    pub fn root_span_kv<'a, T>(&self, fields: T) -> Span
    where
        T: IntoIterator<Item = (&'a str, String)>,
    {
        let span = self.root_span();
        if self.json {
            let details = kv_to_string(fields);
            info!(op = %self.op_name(), details = %details, "start");
        }
        span
    }

    pub fn span(&self, ph: &O::Phase) -> Span { ph.span() }

    pub fn span_kv<'a, T>(&self, ph: &O::Phase, fields: T) -> Span
    where
        T: IntoIterator<Item = (&'a str, String)>,
    {
        let span = self.span(ph);
        if self.json {
            let details = kv_to_string(fields);
            if !details.is_empty() { info!(op = %self.op_name(), phase = ph.name(), details = %details, "span_start"); }
        }
        span
    }

    pub fn info(&self, msg: impl AsRef<str>) { if self.json { info!(op = %self.op_name(), "{}", msg.as_ref()); } else { info!("{}", msg.as_ref()); } }
    pub fn debug(&self, msg: impl AsRef<str>) { if self.json { debug!(op = %self.op_name(), "{}", msg.as_ref()); } else { debug!("{}", msg.as_ref()); } }
    pub fn warn(&self, msg: impl AsRef<str>) { if self.json { warn!(op = %self.op_name(), "{}", msg.as_ref()); } else { warn!("{}", msg.as_ref()); } }
    pub fn error(&self, msg: impl AsRef<str>) { if self.json { error!(op = %self.op_name(), "{}", msg.as_ref()); } else { error!("{}", msg.as_ref()); } }

    pub fn info_kv<'a, D>(&self, msg: &str, kv: D)
    where
        D: IntoIterator<Item = (&'a str, String)>,
    {
        if self.json { let details = kv_to_string(kv); info!(op = %self.op_name(), details = %details, "{}", msg); }
        else { info!("{}", msg); }
    }

    pub fn debug_kv<'a, D>(&self, msg: &str, kv: D)
    where
        D: IntoIterator<Item = (&'a str, String)>,
    {
        if self.json { let details = kv_to_string(kv); debug!(op = %self.op_name(), details = %details, "{}", msg); }
        else { debug!("{}", msg); }
    }

    pub fn warn_kv<'a, D>(&self, msg: &str, kv: D)
    where
        D: IntoIterator<Item = (&'a str, String)>,
    {
        if self.json { let details = kv_to_string(kv); warn!(op = %self.op_name(), details = %details, "{}", msg); }
        else { warn!("{}", msg); }
    }

    pub fn error_kv<'a, D>(&self, msg: &str, kv: D)
    where
        D: IntoIterator<Item = (&'a str, String)>,
    {
        if self.json { let details = kv_to_string(kv); error!(op = %self.op_name(), details = %details, "{}", msg); }
        else { error!("{}", msg); }
    }

    pub fn plan<T: Serialize>(&self, plan: &T) -> anyhow::Result<()> { print_plan(self.op_name(), plan, None) }
    pub fn result<T: Serialize>(&self, result: &T) -> anyhow::Result<()> { print_result(self.op_name(), result, None) }
}

// Ingest implementations for traits
impl PhaseSpan for ingest::Phase {
    fn name(&self) -> &'static str {
        match self {
            ingest::Phase::Feed => "feed",
            ingest::Phase::FetchRss => "fetch_rss",
            ingest::Phase::ParseRss => "parse_rss",
            ingest::Phase::FetchItem => "fetch_item",
            ingest::Phase::Extract => "extract",
            ingest::Phase::WriteDoc => "write_doc",
        }
    }
    fn span(&self) -> Span {
        match self {
            ingest::Phase::Feed => info_span!("feed"),
            ingest::Phase::FetchRss => info_span!("fetch_rss"),
            ingest::Phase::ParseRss => info_span!("parse_rss"),
            ingest::Phase::FetchItem => info_span!("fetch_item"),
            ingest::Phase::Extract => info_span!("extract"),
            ingest::Phase::WriteDoc => info_span!("write_doc"),
        }
    }
}

impl OpMarker for ingest::Ingest {
    const NAME: &'static str = "ingest";
    type Phase = ingest::Phase;
    fn root_span() -> Span { info_span!("ingest") }
}

// Init implementations for traits
impl PhaseSpan for init::Phase {
    fn name(&self) -> &'static str {
        match self {
            init::Phase::Plan => "plan",
            init::Phase::Migrate => "migrate",
        }
    }
    fn span(&self) -> Span {
        match self {
            init::Phase::Plan => info_span!("plan"),
            init::Phase::Migrate => info_span!("migrate"),
        }
    }
}

impl OpMarker for init::Init {
    const NAME: &'static str = "init";
    type Phase = init::Phase;
    fn root_span() -> Span { info_span!("init") }
}

// Feed implementations for traits
impl PhaseSpan for feed::Phase {
    fn name(&self) -> &'static str {
        match self {
            feed::Phase::Plan => "plan",
            feed::Phase::Add => "add",
            feed::Phase::List => "list",
        }
    }
    fn span(&self) -> Span {
        match self {
            feed::Phase::Plan => info_span!("plan"),
            feed::Phase::Add => info_span!("add"),
            feed::Phase::List => info_span!("list"),
        }
    }
}

impl OpMarker for feed::Feed {
    const NAME: &'static str = "feed";
    type Phase = feed::Phase;
    fn root_span() -> Span { info_span!("feed") }
}

// Chunk implementations for traits
impl PhaseSpan for chunk::Phase {
    fn name(&self) -> &'static str {
        match self {
            chunk::Phase::Plan => "plan",
            chunk::Phase::SelectDocs => "select_docs",
            chunk::Phase::Tokenize => "tokenize",
            chunk::Phase::InsertChunk => "insert_chunk",
            chunk::Phase::UpdateStatus => "update_status",
        }
    }
    fn span(&self) -> Span {
        match self {
            chunk::Phase::Plan => info_span!("plan"),
            chunk::Phase::SelectDocs => info_span!("select_docs"),
            chunk::Phase::Tokenize => info_span!("tokenize"),
            chunk::Phase::InsertChunk => info_span!("insert_chunk"),
            chunk::Phase::UpdateStatus => info_span!("update_status"),
        }
    }
}

impl OpMarker for chunk::Chunk {
    const NAME: &'static str = "chunk";
    type Phase = chunk::Phase;
    fn root_span() -> Span { info_span!("chunk") }
}

// Embed implementations for traits
impl PhaseSpan for embed::Phase {
    fn name(&self) -> &'static str {
        match self {
            embed::Phase::Plan => "plan",
            embed::Phase::CountCandidates => "count_candidates",
            embed::Phase::LoadModel => "load_model",
            embed::Phase::FetchBatch => "fetch_batch",
            embed::Phase::Encode => "encode",
            embed::Phase::InsertEmbedding => "insert_embedding",
        }
    }
    fn span(&self) -> Span {
        match self {
            embed::Phase::Plan => info_span!("plan"),
            embed::Phase::CountCandidates => info_span!("count_candidates"),
            embed::Phase::LoadModel => info_span!("load_model"),
            embed::Phase::FetchBatch => info_span!("fetch_batch"),
            embed::Phase::Encode => info_span!("encode"),
            embed::Phase::InsertEmbedding => info_span!("insert_embedding"),
        }
    }
}

impl OpMarker for embed::Embed {
    const NAME: &'static str = "embed";
    type Phase = embed::Phase;
    fn root_span() -> Span { info_span!("embed") }
}

// Ingest-specific helpers remain available on the typed context
impl LogCtx<ingest::Ingest> {
    pub fn feed_summary(&self, feed_id: i32, inserted: usize, updated: usize, skipped: usize, errors: usize) {
        if self.json { info!(op = %self.op_name(), feed_id, inserted, updated, skipped, errors, "feed_summary"); }
        else { info!("✅ Feed {} — inserted={} updated={} skipped={} errors={}", feed_id, inserted, updated, skipped, errors); }
    }

    pub fn totals(&self, inserted: usize, updated: usize, skipped: usize, errors: usize) {
        if self.json { info!(op = %self.op_name(), inserted, updated, skipped, errors, "ingest_totals"); }
        else { info!("📊 Ingest totals — inserted={} updated={} skipped={} errors={}", inserted, updated, skipped, errors); }
    }
}

fn kv_to_string<'a, T>(kv: T) -> String
where
    T: IntoIterator<Item = (&'a str, String)>,
{
    let mut parts: Vec<String> = Vec::new();
    for (k, v) in kv {
        parts.push(format!("{}={}", k, v));
    }
    parts.join(" ")
}

fn print_plan<T: Serialize>(op: &str, plan: &T, meta: Option<Meta>) -> anyhow::Result<()> {
    let env = json!({
        "op": op,
        "apply": false,
        "plan": plan,
        "meta": meta
    });
    // Write to stdout as a single line
    let mut out = io::stdout();
    serde_json::to_writer(&mut out, &env)?;
    writeln!(&mut out)?;
    Ok(())
}

fn print_result<T: Serialize>(op: &str, result: &T, meta: Option<Meta>) -> anyhow::Result<()> {
    let env = json!({
        "op": op,
        "apply": true,
        "result": result,
        "meta": meta
    });
    let mut out = io::stdout();
    serde_json::to_writer(&mut out, &env)?;
    writeln!(&mut out)?;
    Ok(())
}
