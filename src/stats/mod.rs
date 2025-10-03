use anyhow::Result;
use clap::Args;
use sqlx::PgPool;

pub mod summary;
pub mod feed;
pub mod doc;
pub mod chunk;
pub mod types;
pub mod db;

#[derive(Args, Debug)]
pub struct StatsCmd {
    #[arg(long)] pub feed: Option<i32>,
    #[arg(long)] pub doc: Option<i64>,
    #[arg(long)] pub chunk: Option<i64>,

    /// Number of docs to list in --feed view (default: 10)
    #[arg(long, default_value_t = 10)]
    pub doc_limit: i64,

    /// Number of chunks to list in --doc view (default: 10)
    #[arg(long, default_value_t = 10)]
    pub chunk_limit: i64,
}

pub async fn run(pool: &PgPool, args: StatsCmd) -> Result<()> {
    if let Some(id) = args.doc { return doc::snapshot_doc(pool, id, args.chunk_limit).await; }
    if let Some(id) = args.chunk { return chunk::snapshot_chunk(pool, id).await; }
    if let Some(feed_id) = args.feed { return feed::feed_stats(pool, feed_id, args.doc_limit).await; }
    summary::summary(pool).await
}
