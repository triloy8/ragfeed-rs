pub mod config;
pub mod ctx;
pub mod emit;
pub mod macros;
pub mod ops;

use ctx::LogCtx;

// Factory helpers mirroring the old out::{ingest, init, ...} API
pub fn ingest() -> LogCtx<ops::ingest::Ingest> { LogCtx { json: config::logs_are_json(), _marker: std::marker::PhantomData } }
pub fn feed() -> LogCtx<ops::feed::Feed> { LogCtx { json: config::logs_are_json(), _marker: std::marker::PhantomData } }
pub fn chunk() -> LogCtx<ops::chunk::Chunk> { LogCtx { json: config::logs_are_json(), _marker: std::marker::PhantomData } }
pub fn embed() -> LogCtx<ops::embed::Embed> { LogCtx { json: config::logs_are_json(), _marker: std::marker::PhantomData } }
pub fn reindex() -> LogCtx<ops::reindex::Reindex> { LogCtx { json: config::logs_are_json(), _marker: std::marker::PhantomData } }
pub fn gc() -> LogCtx<ops::gc::Gc> { LogCtx { json: config::logs_are_json(), _marker: std::marker::PhantomData } }
pub fn stats() -> LogCtx<ops::stats::Stats> { LogCtx { json: config::logs_are_json(), _marker: std::marker::PhantomData } }
pub fn query() -> LogCtx<ops::query::Query> { LogCtx { json: config::logs_are_json(), _marker: std::marker::PhantomData } }
