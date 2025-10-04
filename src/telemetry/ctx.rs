use anyhow::Result;
use serde::Serialize;
use std::marker::PhantomData;
use tracing::{info, debug, warn, error, info_span, Span};

use super::{config, emit};

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
    pub(crate) json: bool,
    pub(crate) _marker: PhantomData<O>,
}

impl<O: OpMarker> LogCtx<O> {
    fn op_name(&self) -> &'static str { O::NAME }

    pub fn root_span(&self) -> Span { O::root_span() }

    pub fn root_span_kv<'a, T>(&self, fields: T) -> Span
    where
        T: IntoIterator<Item = (&'a str, String)>,
    {
        let span = self.root_span();
        let details = kv_to_string(fields);
        if details.is_empty() {
            info!(op = %self.op_name(), "start");
        } else {
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
        let details = kv_to_string(fields);
        if details.is_empty() {
            info!(op = %self.op_name(), phase = ph.name(), "span_start");
        } else {
            info!(op = %self.op_name(), phase = ph.name(), details = %details, "span_start");
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

    pub fn plan<T: Serialize>(&self, plan: &T) -> Result<()> { emit::print_plan(self.op_name(), plan, None) }
    pub fn result<T: Serialize>(&self, result: &T) -> Result<()> { emit::print_result(self.op_name(), result, None) }
}

// Ingest-specific helpers remain available on the typed context
impl LogCtx<crate::telemetry::ops::ingest::Ingest> {
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
    for (k, v) in kv { parts.push(format!("{}={}", k, v)); }
    parts.join(" ")
}
