// Compatibility shim: re-export the new telemetry module under the old `out` API

pub use crate::telemetry::config::{set_json_mode, json_mode, logs_are_json};
pub use crate::telemetry::emit::Meta;
pub use crate::telemetry::ctx::{LogCtx, PhaseSpan, OpMarker};

pub mod ingest { pub use crate::telemetry::ops::ingest::{Ingest, Phase}; }
pub mod init { pub use crate::telemetry::ops::init::{Init, Phase}; }
pub mod feed { pub use crate::telemetry::ops::feed::{Feed, Phase}; }
pub mod chunk { pub use crate::telemetry::ops::chunk::{Chunk, Phase}; }
pub mod embed { pub use crate::telemetry::ops::embed::{Embed, Phase}; }
pub mod reindex { pub use crate::telemetry::ops::reindex::{Reindex, Phase}; }
pub mod gc { pub use crate::telemetry::ops::gc::{Gc, Phase}; }
pub mod stats { pub use crate::telemetry::ops::stats::{Stats, Phase}; }
pub mod query { pub use crate::telemetry::ops::query::{Query, Phase}; }

pub fn ingest() -> LogCtx<crate::telemetry::ops::ingest::Ingest> { crate::telemetry::ingest() }
pub fn init() -> LogCtx<crate::telemetry::ops::init::Init> { crate::telemetry::init() }
pub fn feed() -> LogCtx<crate::telemetry::ops::feed::Feed> { crate::telemetry::feed() }
pub fn chunk() -> LogCtx<crate::telemetry::ops::chunk::Chunk> { crate::telemetry::chunk() }
pub fn embed() -> LogCtx<crate::telemetry::ops::embed::Embed> { crate::telemetry::embed() }
pub fn reindex() -> LogCtx<crate::telemetry::ops::reindex::Reindex> { crate::telemetry::reindex() }
pub fn gc() -> LogCtx<crate::telemetry::ops::gc::Gc> { crate::telemetry::gc() }
pub fn stats() -> LogCtx<crate::telemetry::ops::stats::Stats> { crate::telemetry::stats() }
pub fn query() -> LogCtx<crate::telemetry::ops::query::Query> { crate::telemetry::query() }

